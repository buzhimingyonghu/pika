// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include "net/src/holy_thread.h"

#include <glog/logging.h>

#include "net/include/net_conn.h"
#include "net/src/net_item.h"
#include "net/src/net_multiplexer.h"
#include "pstd/include/xdebug.h"

namespace net {

HolyThread::HolyThread(int port, ConnFactory* conn_factory, int cron_interval, const ServerHandle* handle, bool async)
    : ServerThread::ServerThread(port, cron_interval, handle),
      conn_factory_(conn_factory),

      keepalive_timeout_(kDefaultKeepAliveTime),
      async_(async) {}

HolyThread::HolyThread(const std::string& bind_ip, int port, ConnFactory* conn_factory, int cron_interval,
                       const ServerHandle* handle, bool async)
    : ServerThread::ServerThread(bind_ip, port, cron_interval, handle), conn_factory_(conn_factory), async_(async) {}

HolyThread::HolyThread(const std::set<std::string>& bind_ips, int port, ConnFactory* conn_factory, int cron_interval,
                       const ServerHandle* handle, bool async)
    : ServerThread::ServerThread(bind_ips, port, cron_interval, handle), conn_factory_(conn_factory), async_(async) {}

HolyThread::~HolyThread() { Cleanup(); }

// 获取当前连接数
int HolyThread::conn_num() const {
  std::shared_lock l(rwlock_);                 // 对连接容器加共享锁，保证线程安全读取
  return static_cast<int32_t>(conns_.size());  // 返回当前连接的数量
}

// 获取所有连接的详细信息
std::vector<ServerThread::ConnInfo> HolyThread::conns_info() const {
  std::vector<ServerThread::ConnInfo> result;  // 用于存储连接信息的结果
  std::shared_lock l(rwlock_);                 // 对连接容器加共享锁，保证线程安全读取
  for (auto& conn : conns_) {
    // 将每个连接的信息封装成 ConnInfo 并加入结果列表
    result.push_back({conn.first, conn.second->ip_port(), conn.second->last_interaction()});
  }
  return result;  // 返回所有连接的详细信息
}

// 移除指定文件描述符的连接并返回该连接对象
std::shared_ptr<NetConn> HolyThread::MoveConnOut(int fd) {
  std::lock_guard l(rwlock_);               // 对连接容器加锁，确保线程安全操作
  std::shared_ptr<NetConn> conn = nullptr;  // 默认连接为空
  auto iter = conns_.find(fd);              // 查找指定文件描述符的连接
  if (iter != conns_.end()) {
    conn = iter->second;                   // 找到连接后，保存到 conn 变量
    net_multiplexer_->NetDelEvent(fd, 0);  // 从多路复用器中删除该连接的事件
    conns_.erase(iter);                    // 从连接容器中移除该连接
  }
  return conn;  // 返回被移除的连接对象
}

// 获取指定文件描述符的连接
std::shared_ptr<NetConn> HolyThread::get_conn(int fd) {
  std::shared_lock l(rwlock_);  // 对连接容器加共享锁，保证线程安全读取
  auto iter = conns_.find(fd);  // 查找指定文件描述符的连接
  if (iter != conns_.end()) {
    return iter->second;  // 如果找到连接，返回连接对象
  } else {
    return nullptr;  // 如果找不到连接，返回 nullptr
  }
}

// 启动线程
int HolyThread::StartThread() {
  int ret = handle_->CreateWorkerSpecificData(&private_data_);  // 为线程创建专用数据
  if (ret) {
    return ret;  // 如果创建失败，返回错误码
  }
  return ServerThread::StartThread();  // 调用基类的 StartThread 启动线程
}

// 停止线程
int HolyThread::StopThread() {
  if (private_data_) {
    int ret = handle_->DeleteWorkerSpecificData(private_data_);  // 删除线程的专用数据
    if (ret) {
      return ret;  // 如果删除失败，返回错误码
    }
    private_data_ = nullptr;  // 清空专用数据
  }
  return ServerThread::StopThread();  // 调用基类的 StopThread 停止线程
}

// 处理新连接
void HolyThread::HandleNewConn(const int connfd, const std::string& ip_port) {
  // 使用连接工厂创建新的 NetConn 对象
  std::shared_ptr<NetConn> tc = conn_factory_->NewNetConn(connfd, ip_port, this, private_data_, net_multiplexer_.get());
  tc->SetNonblock();  // 设置连接为非阻塞模式
  {
    std::lock_guard l(rwlock_);  // 对连接容器加锁，确保线程安全操作
    conns_[connfd] = tc;         // 将新连接加入连接容器
  }

  // 在多路复用器中注册该连接的可读事件
  net_multiplexer_->NetAddEvent(connfd, kReadable);
}

/*
 * 函数名：HolyThread::HandleConnEvent
 * 功能：处理指定文件描述符上的网络事件，包括读取请求、发送响应以及连接关闭的逻辑。
 * 参数：
 *   - pfe: 指向触发事件的 `NetFiredEvent` 结构体。
 */
void HolyThread::HandleConnEvent(NetFiredEvent* pfe) {
  if (!pfe) {
    return;  // 如果事件为空，直接返回
  }

  std::shared_ptr<NetConn> in_conn = nullptr;  // 保存当前连接
  int should_close = 0;                        // 标志是否需要关闭连接

  // 加读锁，确保在多线程环境下安全访问连接映射
  {
    std::shared_lock l(rwlock_);
    auto iter = conns_.find(pfe->fd);  // 查找事件对应的连接
    if (iter == conns_.end()) {
      // 如果连接不存在，删除对应的网络事件并返回
      net_multiplexer_->NetDelEvent(pfe->fd, 0);
      return;
    } else {
      in_conn = iter->second;  // 获取连接对象
    }
  }

  // 异步模式处理逻辑
  if (async_) {
    if (pfe->mask & kReadable) {                       // 可读事件
      ReadStatus read_status = in_conn->GetRequest();  // 获取请求
      struct timeval now;
      gettimeofday(&now, nullptr);
      in_conn->set_last_interaction(now);  // 更新最后交互时间
      if (read_status == kReadAll) {
        // 读取完成，继续监听可读事件
      } else if (read_status == kReadHalf) {
        return;  // 数据未完整读取，等待下次事件
      } else {
        // 处理读取错误或连接关闭等异常状态
        should_close = 1;
      }
    }
    if ((pfe->mask & kWritable) && in_conn->is_reply()) {  // 可写事件且需要响应
      WriteStatus write_status = in_conn->SendReply();     // 发送响应
      if (write_status == kWriteAll) {
        in_conn->set_is_reply(false);                          // 清除回复标志
        net_multiplexer_->NetModEvent(pfe->fd, 0, kReadable);  // 修改事件为可读
      } else if (write_status == kWriteHalf) {
        return;  // 响应未发送完，等待下次事件
      } else if (write_status == kWriteError) {
        should_close = 1;  // 发送失败，关闭连接
      }
    }
  }
  // 同步模式处理逻辑
  else {
    if (pfe->mask & kReadable) {                  // 可读事件
      ReadStatus getRes = in_conn->GetRequest();  // 获取请求
      struct timeval now;
      gettimeofday(&now, nullptr);
      in_conn->set_last_interaction(now);  // 更新最后交互时间
      if (getRes != kReadAll && getRes != kReadHalf) {
        // 处理读取错误或连接关闭等异常状态
        should_close = 1;
      } else if (in_conn->is_reply()) {
        net_multiplexer_->NetModEvent(pfe->fd, 0, kWritable);  // 修改事件为可写
      } else {
        return;  // 等待更多数据
      }
    }
    if (pfe->mask & kWritable) {                        // 可写事件
      WriteStatus write_status = in_conn->SendReply();  // 发送响应
      if (write_status == kWriteAll) {
        in_conn->set_is_reply(false);                          // 清除回复标志
        net_multiplexer_->NetModEvent(pfe->fd, 0, kReadable);  // 修改事件为可读
      } else if (write_status == kWriteHalf) {
        return;  // 响应未发送完，等待下次事件
      } else if (write_status == kWriteError) {
        should_close = 1;  // 发送失败，关闭连接
      }
    }
  }

  // 错误事件或需要关闭连接
  if ((pfe->mask & kErrorEvent) || should_close) {
    net_multiplexer_->NetDelEvent(pfe->fd, 0);  // 从多路复用器中删除事件
    CloseFd(in_conn);                           // 关闭文件描述符
    in_conn = nullptr;

    {
      std::lock_guard l(rwlock_);  // 加写锁，安全删除连接
      conns_.erase(pfe->fd);       // 从连接映射中移除
    }
  }
}
// 执行定时任务，处理连接关闭和超时等操作
void HolyThread::DoCronTask() {
  struct timeval now;
  gettimeofday(&now, nullptr);                       // 获取当前时间
  std::vector<std::shared_ptr<NetConn>> to_close;    // 需要关闭的连接
  std::vector<std::shared_ptr<NetConn>> to_timeout;  // 需要超时处理的连接

  {
    std::lock_guard l(rwlock_);  // 对连接容器加锁，确保线程安全

    // 检查是否需要关闭所有连接
    std::lock_guard kl(killer_mutex_);                     // 加锁以保护删除操作
    if (deleting_conn_ipport_.count(kKillAllConnsTask)) {  // 判断是否需要关闭所有连接
      for (auto& conn : conns_) {
        to_close.push_back(conn.second);  // 添加所有连接到待关闭队列
      }
      conns_.clear();                 // 清空所有连接
      deleting_conn_ipport_.clear();  // 清空删除标记
      for (const auto& conn : to_close) {
        CloseFd(conn);  // 关闭每一个连接
      }
      return;  // 退出任务，不再处理其他连接
    }

    // 遍历所有连接，检查是否需要关闭或超时
    auto iter = conns_.begin();
    while (iter != conns_.end()) {
      std::shared_ptr<NetConn> conn = iter->second;  // 获取连接对象
      // 如果该连接标记为需要删除
      if (deleting_conn_ipport_.count(conn->ip_port())) {
        to_close.push_back(conn);                      // 将该连接加入待关闭队列
        deleting_conn_ipport_.erase(conn->ip_port());  // 删除标记
        iter = conns_.erase(iter);                     // 从连接容器中删除该连接
        continue;
      }

      // 如果连接处于长时间未交互状态，认为它超时
      if (keepalive_timeout_ > 0 && (now.tv_sec - conn->last_interaction().tv_sec > keepalive_timeout_)) {
        to_timeout.push_back(conn);  // 将超时连接加入超时队列
        iter = conns_.erase(iter);   // 从连接容器中删除该连接
        continue;
      }

      // 可能需要调整连接的缓冲区大小
      conn->TryResizeBuffer();

      ++iter;  // 继续遍历下一个连接
    }
  }

  // 关闭所有需要关闭的连接
  for (const auto& conn : to_close) {
    CloseFd(conn);  // 关闭连接
  }
  // 处理所有超时连接
  for (const auto& conn : to_timeout) {
    CloseFd(conn);                                          // 关闭超时连接
    handle_->FdTimeoutHandle(conn->fd(), conn->ip_port());  // 执行超时处理
  }
}

// 关闭连接并执行相关处理
void HolyThread::CloseFd(const std::shared_ptr<NetConn>& conn) {
  close(conn->fd());                                     // 关闭连接的文件描述符
  handle_->FdClosedHandle(conn->fd(), conn->ip_port());  // 执行连接关闭后的处理
}
// 清理所有连接
void HolyThread::Cleanup() {
  std::map<int, std::shared_ptr<NetConn>> to_close;  // 存储待关闭的连接
  {
    std::lock_guard l(rwlock_);    // 对连接容器加锁，确保线程安全
    to_close = std::move(conns_);  // 将当前连接列表转移到待关闭列表
    conns_.clear();                // 清空连接容器
  }
  // 遍历待关闭连接，并关闭每个连接
  for (auto& iter : to_close) {
    CloseFd(iter.second);  // 调用 CloseFd 关闭连接
  }
}

// 关闭所有连接
void HolyThread::KillAllConns() {
  KillConn(kKillAllConnsTask);  // 调用 KillConn 来关闭所有连接
}

// 根据 ip_port 关闭指定连接
// 如果 ip_port 是 kKillAllConnsTask，则会关闭所有连接
bool HolyThread::KillConn(const std::string& ip_port) {
  bool find = false;
  // 如果指定的 ip_port 不是 "KillAllConnsTask"，则检查该连接是否存在
  if (ip_port != kKillAllConnsTask) {
    std::shared_lock lock(rwlock_);  // 对连接容器加共享锁，确保线程安全读取
    for (auto& [_, conn] : conns_) {
      if (conn->ip_port() == ip_port) {
        find = true;  // 找到对应的连接
        break;
      }
    }
  }
  // 如果找到指定连接或是关闭所有连接的任务，则进行关闭操作
  if (find || ip_port == kKillAllConnsTask) {
    std::lock_guard l(killer_mutex_);       // 对删除连接的标记加锁
    deleting_conn_ipport_.insert(ip_port);  // 将连接标记为待删除
    return true;
  }
  return false;  // 未找到指定连接，返回 false
}

// 处理通知事件
void HolyThread::ProcessNotifyEvents(const net::NetFiredEvent* pfe) {
  if (pfe->mask & kReadable) {                                            // 检查是否为可读事件
    char bb[2048];                                                        // 缓冲区
    int64_t nread = read(net_multiplexer_->NotifyReceiveFd(), bb, 2048);  // 读取通知事件
    if (nread == 0) {
      return;  // 如果没有数据，直接返回
    } else {
      // 遍历所有的通知事件
      for (int32_t idx = 0; idx < nread; ++idx) {
        net::NetItem ti = net_multiplexer_->NotifyQueuePop();                // 弹出通知事件
        std::string ip_port = ti.ip_port();                                  // 获取 IP 和端口信息
        int fd = ti.fd();                                                    // 获取文件描述符
        if (ti.notify_type() == net::kNotiWrite) {                           // 如果是写事件
          net_multiplexer_->NetModEvent(ti.fd(), 0, kReadable | kWritable);  // 修改事件为可读写
        } else if (ti.notify_type() == net::kNotiClose) {                    // 如果是关闭事件
          LOG(INFO) << "receive noti close";                                 // 打印关闭日志
          std::shared_ptr<net::NetConn> conn = get_conn(fd);                 // 获取连接对象
          if (!conn) {
            continue;  // 如果连接为空，则跳过
          }
          CloseFd(conn);   // 关闭连接
          conn = nullptr;  // 置空连接
          {
            std::lock_guard l(rwlock_);  // 对连接容器加锁
            conns_.erase(fd);            // 从连接容器中移除该连接
          }
        }
      }
    }
  }
}

extern ServerThread* NewHolyThread(int port, ConnFactory* conn_factory, int cron_interval, const ServerHandle* handle) {
  return new HolyThread(port, conn_factory, cron_interval, handle);
}
extern ServerThread* NewHolyThread(const std::string& bind_ip, int port, ConnFactory* conn_factory, int cron_interval,
                                   const ServerHandle* handle) {
  return new HolyThread(bind_ip, port, conn_factory, cron_interval, handle);
}
extern ServerThread* NewHolyThread(const std::set<std::string>& bind_ips, int port, ConnFactory* conn_factory,
                                   int cron_interval, const ServerHandle* handle) {
  return new HolyThread(bind_ips, port, conn_factory, cron_interval, handle);
}
extern ServerThread* NewHolyThread(const std::set<std::string>& bind_ips, int port, ConnFactory* conn_factory,
                                   bool async, int cron_interval, const ServerHandle* handle) {
  return new HolyThread(bind_ips, port, conn_factory, cron_interval, handle, async);
}
};  // namespace net
