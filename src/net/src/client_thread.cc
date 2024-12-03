// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/client_thread.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <glog/logging.h>

#include "net/include/net_conn.h"
#include "net/src/server_socket.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

namespace net {

using pstd::Status;

ClientThread::ClientThread(ConnFactory* conn_factory, int cron_interval, int keepalive_timeout, ClientHandle* handle,
                           void* private_data)
    : keepalive_timeout_(keepalive_timeout),
      cron_interval_(cron_interval),
      handle_(handle),
      private_data_(private_data),
      conn_factory_(conn_factory) {
  net_multiplexer_.reset(CreateNetMultiplexer());
  net_multiplexer_->Initialize();
}

ClientThread::~ClientThread() = default;

// 启动线程
int ClientThread::StartThread() {
  // 如果handle_为空，创建一个新的ClientHandle对象
  if (!handle_) {
    handle_ = new ClientHandle();
    own_handle_ = true;  // 表示该类负责销毁handle_
  }

  // 设置own_handle_为false，表明handle_的生命周期不由当前对象管理
  own_handle_ = false;

  // 创建线程私有数据
  int res = handle_->CreateWorkerSpecificData(&private_data_);
  if (res) {
    return res;  // 创建失败，返回错误码
  }

  // 设置线程名称为 "ClientThread"
  set_thread_name("ClientThread");

  // 调用基类的StartThread方法启动线程
  return Thread::StartThread();
}

// 停止线程
int ClientThread::StopThread() {
  // 如果有私有数据，删除它
  if (private_data_) {
    int res = handle_->DeleteWorkerSpecificData(private_data_);
    if (res) {
      return res;  // 删除失败，返回错误码
    }
    private_data_ = nullptr;  // 清空私有数据指针
  }

  // 如果当前类负责销毁handle_，则删除它
  if (own_handle_) {
    delete handle_;
  }

  // 调用基类的StopThread方法停止线程
  return Thread::StopThread();
}

// 向指定IP:port的连接写入消息
Status ClientThread::Write(const std::string& ip, const int port, const std::string& msg) {
  // 构造ip:port格式的字符串
  std::string ip_port = ip + ":" + std::to_string(port);

  // 检查该IP:port是否被用户禁止访问
  if (!handle_->AccessHandle(ip_port)) {
    return Status::Corruption(ip_port + " is baned by user!");  // 如果被禁止，返回错误
  }

  // 锁定互斥量，确保对to_send_的操作是线程安全的
  {
    std::lock_guard l(mu_);
    size_t size = 0;

    // 计算待发送消息的总大小
    for (auto& str : to_send_[ip_port]) {
      size += str.size();
    }

    // 如果待发送的消息总大小超过了连接缓冲区的最大值，返回错误
    if (size > kConnWriteBuf) {
      return Status::Corruption("Connection buffer over maximum size");
    }

    // 将消息添加到待发送队列中
    to_send_[ip_port].push_back(msg);
  }

  // 通知写操作，等待将消息发送到目标地址
  NotifyWrite(ip_port);

  return Status::OK();  // 写入成功，返回OK状态
}

// 关闭指定IP:port的连接
Status ClientThread::Close(const std::string& ip, const int port) {
  // 锁定to_del_mu_，确保对to_del_的操作是线程安全的
  {
    std::lock_guard l(to_del_mu_);
    to_del_.push_back(ip + ":" + std::to_string(port));  // 将要关闭的连接添加到待删除列表中
  }

  return Status::OK();  // 关闭成功，返回OK状态
}

Status ClientThread::ProcessConnectStatus(NetFiredEvent* pfe, int* should_close) {
  if (pfe->mask & kErrorEvent) {
    *should_close = 1;
    return Status::Corruption("POLLERR or POLLHUP");
  }
  int val = 0;
  socklen_t lon = sizeof(int);

  if (getsockopt(pfe->fd, SOL_SOCKET, SO_ERROR, &val, &lon) == -1) {
    *should_close = 1;
    return Status::Corruption("Get Socket opt failed");
  }
  if (val) {
    *should_close = 1;
    return Status::Corruption("Get socket error " + std::to_string(val));
  }
  return Status::OK();
}

void ClientThread::SetWaitConnectOnEpoll(int sockfd) {
  net_multiplexer_->NetAddEvent(sockfd, kReadable | kWritable);
  connecting_fds_.insert(sockfd);
}

void ClientThread::NewConnection(const std::string& peer_ip, int peer_port, int sockfd) {
  std::string ip_port = peer_ip + ":" + std::to_string(peer_port);
  std::shared_ptr<NetConn> tc = conn_factory_->NewNetConn(sockfd, ip_port, this, nullptr, net_multiplexer_.get());
  tc->SetNonblock();
  // This flag specifies that the file descriptor should be closed when an exec function is invoked.
  fcntl(sockfd, F_SETFD, fcntl(sockfd, F_GETFD) | FD_CLOEXEC);

  fd_conns_.insert(std::make_pair(sockfd, tc));
  ipport_conns_.insert(std::make_pair(ip_port, tc));
}

Status ClientThread::ScheduleConnect(const std::string& dst_ip, int dst_port) {
  Status s;
  int sockfd = -1;
  int rv;
  char cport[6];
  struct addrinfo hints;
  struct addrinfo* servinfo;
  struct addrinfo* p;
  snprintf(cport, sizeof(cport), "%d", dst_port);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  // We do not handle IPv6
  if (rv = getaddrinfo(dst_ip.c_str(), cport, &hints, &servinfo); rv) {
    return Status::IOError("connect getaddrinfo error for ", dst_ip);
  }
  for (p = servinfo; p != nullptr; p = p->ai_next) {
    if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      continue;
    }
    int flags = fcntl(sockfd, F_GETFL, 0);
    fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

    if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      if (errno == EHOSTUNREACH) {
        CloseFd(sockfd, dst_ip + ":" + std::to_string(dst_port));
        continue;
      } else if (errno == EINPROGRESS || errno == EAGAIN || errno == EWOULDBLOCK) {
        NewConnection(dst_ip, dst_port, sockfd);
        SetWaitConnectOnEpoll(sockfd);
        freeaddrinfo(servinfo);
        return Status::OK();
      } else {
        CloseFd(sockfd, dst_ip + ":" + std::to_string(dst_port));
        freeaddrinfo(servinfo);
        return Status::IOError("EHOSTUNREACH", "The target host cannot be reached");
      }
    }

    NewConnection(dst_ip, dst_port, sockfd);
    net_multiplexer_->NetAddEvent(sockfd, kReadable | kWritable);
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(sockfd, reinterpret_cast<struct sockaddr*>(&laddr), &llen);
    std::string lip(inet_ntoa(laddr.sin_addr));
    int lport = ntohs(laddr.sin_port);
    if (dst_ip == lip && dst_port == lport) {
      return Status::IOError("EHOSTUNREACH", "same ip port");
    }

    freeaddrinfo(servinfo);

    return s;
  }
  if (!p) {
    s = Status::IOError(strerror(errno), "Can't create socket ");
    return s;
  }
  freeaddrinfo(servinfo);
  freeaddrinfo(p);
  int val = 1;
  setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
  return s;
}

void ClientThread::CloseFd(const std::shared_ptr<NetConn>& conn) {
  close(conn->fd());
  CleanUpConnRemaining(conn->ip_port());
  handle_->FdClosedHandle(conn->fd(), conn->ip_port());
}

void ClientThread::CloseFd(int fd, const std::string& ip_port) {
  close(fd);
  CleanUpConnRemaining(ip_port);
  handle_->FdClosedHandle(fd, ip_port);
}

void ClientThread::CleanUpConnRemaining(const std::string& ip_port) {
  std::lock_guard l(mu_);
  to_send_.erase(ip_port);
}

void ClientThread::DoCronTask() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  auto iter = fd_conns_.begin();
  while (iter != fd_conns_.end()) {
    std::shared_ptr<NetConn> conn = iter->second;

    // Check keepalive timeout connection
    if (keepalive_timeout_ > 0 && (now.tv_sec - conn->last_interaction().tv_sec > keepalive_timeout_)) {
      LOG(INFO) << "Do cron task del fd " << conn->fd();
      net_multiplexer_->NetDelEvent(conn->fd(), 0);
      // did not clean up content in to_send queue
      // will try to send remaining by reconnecting
      close(conn->fd());
      handle_->FdTimeoutHandle(conn->fd(), conn->ip_port());
      if (ipport_conns_.count(conn->ip_port())) {
        ipport_conns_.erase(conn->ip_port());
      }
      if (connecting_fds_.count(conn->fd())) {
        connecting_fds_.erase(conn->fd());
      }
      iter = fd_conns_.erase(iter);
      continue;
    }

    // Maybe resize connection buffer
    conn->TryResizeBuffer();

    ++iter;
  }

  std::vector<std::string> to_del;
  {
    std::lock_guard l(to_del_mu_);
    to_del = std::move(to_del_);
    to_del_.clear();
  }

  for (auto& conn_name : to_del) {
    auto iter = ipport_conns_.find(conn_name);
    if (iter == ipport_conns_.end()) {
      continue;
    }
    std::shared_ptr<NetConn> conn = iter->second;
    net_multiplexer_->NetDelEvent(conn->fd(), 0);
    CloseFd(conn);
    fd_conns_.erase(conn->fd());
    ipport_conns_.erase(conn->ip_port());
    connecting_fds_.erase(conn->fd());
  }
}

void ClientThread::InternalDebugPrint() {
  LOG(INFO) << "___________________________________";
  {
    std::lock_guard l(mu_);
    LOG(INFO) << "To send map: ";
    for (const auto& to_send : to_send_) {
      UNUSED(to_send);
      const std::vector<std::string>& tmp = to_send.second;
      for (const auto& tmp_to_send : tmp) {
        UNUSED(tmp_to_send);
        LOG(INFO) << to_send.first << " " << tmp_to_send;
      }
    }
  }
  LOG(INFO) << "Ipport conn map: ";
  for (const auto& ipport_conn : ipport_conns_) {
    UNUSED(ipport_conn);
    LOG(INFO) << "ipport " << ipport_conn.first;
  }
  LOG(INFO) << "Connected fd map: ";
  for (const auto& fd_conn : fd_conns_) {
    UNUSED(fd_conn);
    LOG(INFO) << "fd " << fd_conn.first;
  }
  LOG(INFO) << "Connecting fd map: ";
  for (const auto& connecting_fd : connecting_fds_) {
    UNUSED(connecting_fd);
    LOG(INFO) << "fd: " << connecting_fd;
  }
  LOG(INFO) << "___________________________________";
}

void ClientThread::NotifyWrite(const std::string& ip_port) {
  // put fd = 0, cause this lib user does not need to know which fd to write to
  // we will check fd by checking ipport_conns_
  NetItem ti(0, ip_port, kNotiWrite);
  net_multiplexer_->Register(ti, true);
}

// ProcessNotifyEvents: 处理通知事件的函数。
// 功能：该函数根据事件的类型（如可读、关闭等）处理相关的网络操作。
// 作用：从通知队列中获取事件，根据事件类型执行相应的操作，如连接、发送消息、关闭连接等。

void ClientThread::ProcessNotifyEvents(const NetFiredEvent* pfe) {
  // 如果事件掩码中包含 kReadable（表示可读事件）
  if (pfe->mask & kReadable) {
    char bb[2048];  // 创建一个缓冲区，用于读取数据
    // 从通知接收文件描述符中读取数据
    int64_t nread = read(net_multiplexer_->NotifyReceiveFd(), bb, 2048);
    // 如果没有数据可读，直接返回
    if (nread == 0) {
      return;
    } else {
      // 遍历读取的数据
      for (int32_t idx = 0; idx < nread; ++idx) {
        // 从队列中获取一个通知项
        NetItem ti = net_multiplexer_->NotifyQueuePop();
        std::string ip_port = ti.ip_port();  // 获取事件对应的IP:端口
        int fd = ti.fd();                    // 获取文件描述符

        // 如果通知类型是 kNotiWrite（可写事件）
        if (ti.notify_type() == kNotiWrite) {
          // 如果没有该IP:端口的连接，尝试连接
          if (ipport_conns_.find(ip_port) == ipport_conns_.end()) {
            std::string ip;
            int port = 0;
            // 解析IP:端口字符串
            if (!pstd::ParseIpPortString(ip_port, ip, port)) {
              continue;  // 如果解析失败，跳过
            }
            // 调度连接
            Status s = ScheduleConnect(ip, port);
            if (!s.ok()) {
              std::string ip_port = ip + ":" + std::to_string(port);
              handle_->DestConnectFailedHandle(ip_port, s.ToString());  // 处理连接失败
              LOG(INFO) << "Ip " << ip << ", port " << port << " Connect err " << s.ToString();
              continue;
            }
          } else {
            // 如果连接已存在，更新网络事件（可读和可写）
            net_multiplexer_->NetModEvent(ipport_conns_[ip_port]->fd(), 0, kReadable | kWritable);
          }

          // 获取待发送的消息列表
          std::vector<std::string> msgs;
          {
            std::lock_guard l(mu_);  // 加锁，保护 to_send_ 容器
            auto iter = to_send_.find(ip_port);
            if (iter == to_send_.end()) {
              continue;  // 如果没有待发送的消息，跳过
            }
            msgs.swap(iter->second);  // 交换消息队列
          }

          // 遍历待发送的消息并尝试发送
          std::vector<std::string> send_failed_msgs;
          for (auto& msg : msgs) {
            // 如果消息发送成功，放入发送失败队列
            if (ipport_conns_[ip_port]->WriteResp(msg)) {
              send_failed_msgs.push_back(msg);
            }
          }

          // 如果有消息发送失败，将其放回待发送队列，并通知写操作
          std::lock_guard l(mu_);
          if (!send_failed_msgs.empty()) {
            send_failed_msgs.insert(send_failed_msgs.end(), to_send_[ip_port].begin(), to_send_[ip_port].end());
            send_failed_msgs.swap(to_send_[ip_port]);
            NotifyWrite(ip_port);  // 通知可以继续写入
          }
        }
        // 如果通知类型是 kNotiClose（连接关闭事件）
        else if (ti.notify_type() == kNotiClose) {
          LOG(INFO) << "received kNotiClose";  // 记录收到关闭通知
          // 从事件多路复用器中删除该文件描述符的事件监听
          net_multiplexer_->NetDelEvent(fd, 0);
          // 关闭连接的文件描述符并清理相关资源
          CloseFd(fd, ip_port);
          fd_conns_.erase(fd);           // 删除文件描述符关联的连接
          ipport_conns_.erase(ip_port);  // 删除IP:端口关联的连接
          connecting_fds_.erase(fd);     // 删除正在连接的文件描述符
        }
      }
    }
  }
}

// ClientThread 类的线程主函数，负责处理网络连接和定时任务
void* ClientThread::ThreadMain() {
  int nfds = 0;                  // 用于存储事件数量
  NetFiredEvent* pfe = nullptr;  // 指向触发的网络事件

  struct timeval when;           // 用于设置定时器
  gettimeofday(&when, nullptr);  // 获取当前时间
  struct timeval now = when;     // 备份当前时间

  // 设置定时任务的时间间隔
  when.tv_sec += (cron_interval_ / 1000);            // 秒数
  when.tv_usec += ((cron_interval_ % 1000) * 1000);  // 毫秒转微秒
  int timeout = cron_interval_;                      // 定时任务的超时时间，单位毫秒
  if (timeout <= 0) {
    timeout = NET_CRON_INTERVAL;  // 如果定时任务时间间隔小于等于0，则使用默认的定时任务间隔
  }

  std::string ip_port;

  // 进入主循环，直到调用 should_stop() 停止线程
  while (!should_stop()) {
    // 如果设置了定时任务时间间隔，执行定时任务
    if (cron_interval_ > 0) {
      gettimeofday(&now, nullptr);  // 获取当前时间
      // 判断是否到了下一个定时任务的执行时间
      if (when.tv_sec > now.tv_sec || (when.tv_sec == now.tv_sec && when.tv_usec > now.tv_usec)) {
        // 如果还没到时间，更新超时时间
        timeout = static_cast<int32_t>((when.tv_sec - now.tv_sec) * 1000 + (when.tv_usec - now.tv_usec) / 1000);
      } else {
        // 执行用户定义的定时任务（通过 handle->CronHandle()）
        handle_->CronHandle();

        // 执行定期的其他任务
        DoCronTask();

        // 更新定时任务的下一次执行时间
        when.tv_sec = now.tv_sec + (cron_interval_ / 1000);
        when.tv_usec = now.tv_usec + ((cron_interval_ % 1000) * 1000);
        timeout = cron_interval_;  // 重置超时时间
      }
    }

    // 使用网路事件多路复用器来监听网络事件
    nfds = net_multiplexer_->NetPoll(timeout);  // 阻塞等待事件触发，超时时间为timeout
    for (int i = 0; i < nfds; i++) {
      pfe = (net_multiplexer_->FiredEvents()) + i;  // 获取触发的网络事件
      if (!pfe) {
        continue;  // 如果事件为空，跳过
      }

      // 检查事件是否是通知事件
      if (pfe->fd == net_multiplexer_->NotifyReceiveFd()) {
        ProcessNotifyEvents(pfe);  // 处理通知事件
        continue;
      }

      int should_close = 0;                 // 标记是否需要关闭连接
      auto iter = fd_conns_.find(pfe->fd);  // 查找文件描述符对应的连接
      if (iter == fd_conns_.end()) {
        LOG(INFO) << "fd " << pfe->fd << " not found in fd_conns";  // 如果未找到，打印日志并删除事件
        net_multiplexer_->NetDelEvent(pfe->fd, 0);
        continue;
      }

      // 获取该文件描述符对应的连接
      std::shared_ptr<NetConn> conn = iter->second;

      // 处理正在连接的文件描述符
      if (connecting_fds_.count(pfe->fd)) {
        Status s = ProcessConnectStatus(pfe, &should_close);  // 处理连接状态
        if (!s.ok()) {
          handle_->DestConnectFailedHandle(conn->ip_port(), s.ToString());  // 如果连接失败，处理失败回调
        }
        connecting_fds_.erase(pfe->fd);  // 移除正在连接的文件描述符
      }

      // 如果不需要关闭连接且可写，则处理写操作
      if ((should_close == 0) && (pfe->mask & kWritable) && conn->is_reply()) {
        WriteStatus write_status = conn->SendReply();  // 发送回复
        conn->set_last_interaction(now);               // 更新最后交互时间
        if (write_status == kWriteAll) {
          // 如果已全部发送，切换为只读模式
          net_multiplexer_->NetModEvent(pfe->fd, 0, kReadable);
          conn->set_is_reply(false);  // 重置为非回复状态
        } else if (write_status == kWriteHalf) {
          continue;  // 如果发送部分完成，继续等待
        } else {
          LOG(INFO) << "send reply error " << write_status;  // 如果发送失败，记录日志并标记为需要关闭
          should_close = 1;
        }
      }

      // 如果不需要关闭连接且可读，则处理读操作
      if ((should_close == 0) && (pfe->mask & kReadable)) {
        ReadStatus read_status = conn->GetRequest();  // 获取请求数据
        conn->set_last_interaction(now);              // 更新最后交互时间
        if (read_status == kReadAll) {
          // 如果已读取全部数据，可以切换到写操作
          // net_multiplexer_->NetModEvent(pfe->fd, 0, EPOLLOUT);
        } else if (read_status == kReadHalf) {
          continue;  // 如果读取部分数据，继续等待
        } else {
          LOG(INFO) << "Get request error " << read_status;  // 如果读取失败，记录日志并标记为需要关闭
          should_close = 1;
        }
      }

      // 如果发生错误或需要关闭连接，则关闭连接
      if ((pfe->mask & kErrorEvent) || should_close) {
        {
          LOG(INFO) << "close connection " << pfe->fd << " reason " << pfe->mask << " " << should_close;
          net_multiplexer_->NetDelEvent(pfe->fd, 0);  // 删除事件
          CloseFd(conn);                              // 关闭文件描述符
          fd_conns_.erase(pfe->fd);                   // 从连接列表中删除
          if (ipport_conns_.count(conn->ip_port())) {
            ipport_conns_.erase(conn->ip_port());  // 删除 ip:port 对应的连接
          }
          if (connecting_fds_.count(conn->fd())) {
            connecting_fds_.erase(conn->fd());  // 删除正在连接的文件描述符
          }
        }
      }
    }
  }
  return nullptr;  // 线程退出
}

}  // namespace net
