// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/server_thread.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>

#include <glog/logging.h>

#include "dispatch_thread.h"
#include "net/src/server_socket.h"
#include "pstd/include/testutil.h"
#include "pstd/include/xdebug.h"

namespace net {

using pstd::Status;

class DefaultServerHandle : public ServerHandle {
 public:
  void CronHandle() const override {}
  void FdTimeoutHandle(int fd, const std::string& ip_port) const override {
    UNUSED(fd);
    UNUSED(ip_port);
  }
  void FdClosedHandle(int fd, const std::string& ip_port) const override {
    UNUSED(fd);
    UNUSED(ip_port);
  }
  bool AccessHandle(std::string& ip) const override {
    UNUSED(ip);
    return true;
  }
  bool AccessHandle(int fd, std::string& ip) const override {
    UNUSED(fd);
    UNUSED(ip);
    return true;
  }
  int CreateWorkerSpecificData(void** data) const override {
    UNUSED(data);
    return 0;
  }
  int DeleteWorkerSpecificData(void* data) const override {
    UNUSED(data);
    return 0;
  }
};

static const ServerHandle* SanitizeHandle(const ServerHandle* raw_handle) {
  if (!raw_handle) {
    return new DefaultServerHandle();
  }
  return raw_handle;
}

ServerThread::ServerThread(int port, int cron_interval, const ServerHandle* handle)
    : cron_interval_(cron_interval),
      handle_(SanitizeHandle(handle)),
      own_handle_(handle_ != handle),
#ifdef __ENABLE_SSL
      security_(false),
#endif
      port_(port) {
  net_multiplexer_.reset(CreateNetMultiplexer());
  net_multiplexer_->Initialize();
  ips_.insert("0.0.0.0");
}

ServerThread::ServerThread(const std::string& bind_ip, int port, int cron_interval, const ServerHandle* handle)
    : cron_interval_(cron_interval),
      handle_(SanitizeHandle(handle)),
      own_handle_(handle_ != handle),
#ifdef __ENABLE_SSL
      security_(false),
#endif
      port_(port) {
  net_multiplexer_.reset(CreateNetMultiplexer());
  net_multiplexer_->Initialize();
  ips_.insert(bind_ip);
}

ServerThread::ServerThread(const std::set<std::string>& bind_ips, int port, int cron_interval,
                           const ServerHandle* handle)
    : cron_interval_(cron_interval),
      handle_(SanitizeHandle(handle)),
      own_handle_(handle_ != handle),
#ifdef __ENABLE_SSL
      security_(false),
#endif
      port_(port) {
  net_multiplexer_.reset(CreateNetMultiplexer());
  net_multiplexer_->Initialize();
  ips_ = bind_ips;
}

ServerThread::~ServerThread() {
#ifdef __ENABLE_SSL
  if (security_) {
    SSL_CTX_free(ssl_ctx_);
    EVP_cleanup();
  }
#endif
  if (own_handle_) {
    delete handle_;
  }
}

int ServerThread::SetTcpNoDelay(int connfd) {
  int val = 1;
  return setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
}

int ServerThread::StartThread() {
  int ret = 0;
  ret = InitHandle();
  if (ret != kSuccess) {
    return ret;
  }
  return Thread::StartThread();
}

int ServerThread::InitHandle() {
  int ret = 0;
  std::shared_ptr<ServerSocket> socket_p;
  if (ips_.find("0.0.0.0") != ips_.end()) {
    ips_.clear();
    ips_.insert("0.0.0.0");
  }

  for (const auto& ip : ips_) {
    socket_p = std::make_shared<ServerSocket>(port_);
    server_sockets_.emplace_back(socket_p);
    ret = socket_p->Listen(ip);
    if (ret != kSuccess) {
      return ret;
    }

    // init pool
    net_multiplexer_->NetAddEvent(socket_p->sockfd(), kReadable | kWritable);
    server_fds_.insert(socket_p->sockfd());
  }
  return kSuccess;
}

void ServerThread::DoCronTask() {}

void ServerThread::ProcessNotifyEvents(const NetFiredEvent* pfe) { UNUSED(pfe); }
// 服务器线程的主入口函数
void* ServerThread::ThreadMain() {
  int nfds;            // 活跃文件描述符数量
  NetFiredEvent* pfe;  // 活跃事件
  Status s;
  struct sockaddr_in cliaddr;                  // 客户端地址
  socklen_t clilen = sizeof(struct sockaddr);  // 客户端地址长度
  int fd;                                      // 文件描述符
  int connfd;                                  // 连接文件描述符

  struct timeval when;
  gettimeofday(&when, nullptr);  // 获取当前时间
  struct timeval now = when;     // 当前时间

  // 设置下一次定时任务执行的时间
  when.tv_sec += (cron_interval_ / 1000);
  when.tv_usec += ((cron_interval_ % 1000) * 1000);
  int timeout = cron_interval_;
  if (timeout <= 0) {
    timeout = NET_CRON_INTERVAL;  // 默认超时时间
  }

  std::string ip_port;                 // 客户端 IP:端口
  char port_buf[32];                   // 用于存储端口号
  char ip_addr[INET_ADDRSTRLEN] = "";  // 客户端 IP 地址

  while (!should_stop()) {  // 只要线程不停止，循环运行
    if (cron_interval_ > 0) {
      gettimeofday(&now, nullptr);  // 获取当前时间
      // 如果当前时间小于下一次定时任务时间，计算超时等待时间
      if (when.tv_sec > now.tv_sec || (when.tv_sec == now.tv_sec && when.tv_usec > now.tv_usec)) {
        timeout = static_cast<int32_t>((when.tv_sec - now.tv_sec) * 1000 + (when.tv_usec - now.tv_usec) / 1000);
      } else {
        // 执行定时任务，并处理用户任务
        DoCronTask();
        handle_->CronHandle();

        // 更新下一次定时任务时间
        when.tv_sec = now.tv_sec + (cron_interval_ / 1000);
        when.tv_usec = now.tv_usec + ((cron_interval_ % 1000) * 1000);
        timeout = cron_interval_;
      }
    }

    // 调用网络多路复用函数，等待事件的发生
    nfds = net_multiplexer_->NetPoll(timeout);
    // 处理所有已触发的事件
    for (int i = 0; i < nfds; i++) {
      pfe = (net_multiplexer_->FiredEvents()) + i;  // 获取事件
      fd = pfe->fd;                                 // 获取触发事件的文件描述符

      // 如果是通知事件，处理通知
      if (pfe->fd == net_multiplexer_->NotifyReceiveFd()) {
        ProcessNotifyEvents(pfe);
        continue;
      }

      /*
       * 处理服务器监听事件
       */
      if (server_fds_.find(fd) != server_fds_.end()) {  // 如果是监听文件描述符
        if ((pfe->mask & kReadable) != 0) {             // 如果是可读事件
          // 接受新的连接
          connfd = accept(fd, reinterpret_cast<struct sockaddr*>(&cliaddr), &clilen);
          if (connfd == -1) {
            LOG(WARNING) << "accept error, errno numberis " << errno << ", error reason " << strerror(errno);
            continue;
          }
          fcntl(connfd, F_SETFD, fcntl(connfd, F_GETFD) | FD_CLOEXEC);  // 设置文件描述符为子进程关闭

          // 设置 TCP_NODELAY 以避免 TCP 延迟
          if (SetTcpNoDelay(connfd) == -1) {
            LOG(WARNING) << "setsockopt error, errno numberis " << errno << ", error reason " << strerror(errno);
            close(connfd);
            continue;
          }

          // 获取客户端 IP 和端口
          ip_port = inet_ntop(AF_INET, &cliaddr.sin_addr, ip_addr, sizeof(ip_addr));

          // 进行访问控制
          if (!handle_->AccessHandle(ip_port) || !handle_->AccessHandle(connfd, ip_port)) {
            close(connfd);  // 如果访问控制不通过，关闭连接
            continue;
          }

          ip_port.append(":");
          snprintf(port_buf, sizeof(port_buf), "%d", ntohs(cliaddr.sin_port));  // 获取端口号并拼接到 IP 地址后
          ip_port.append(port_buf);

          /*
           * 处理新连接，由派生类实现
           */
          HandleNewConn(connfd, ip_port);

        } else if ((pfe->mask & kErrorEvent) != 0) {
          // 如果监听文件描述符发生错误，关闭该文件描述符
          close(pfe->fd);
          continue;
        }
      } else {
        /*
         * 处理已连接客户端的事件，由派生类实现
         */
        HandleConnEvent(pfe);
      }
    }
  }

  // 清理资源
  server_sockets_.clear();
  server_fds_.clear();

  return nullptr;  // 线程退出
}

#ifdef __ENABLE_SSL
static std::vector<std::unique_ptr<pstd::Mutex>> ssl_mutex_;

static void SSLLockingCallback(int mode, int type, const char* file, int line) {
  if (mode & CRYPTO_LOCK) {
    ssl_mutex_[type]->Lock();
  } else {
    ssl_mutex_[type]->Unlock();
  }
}

static unsigned long SSLIdCallback() { return (unsigned long)pthread_self(); }

int ServerThread::EnableSecurity(const std::string& cert_file, const std::string& key_file) {
  if (cert_file.empty() || key_file.empty()) {
    LOG(WARNING) << "cert_file and key_file can not be empty!";
  }
  // Init Security Env
  // 1. Create multithread mutex used by openssl
  ssl_mutex_.resize(CRYPTO_num_locks());
  for (auto& sm : ssl_mutex_) {
    sm.reset(new pstd::Mutex());
  }
  CRYPTO_set_locking_callback(SSLLockingCallback);
  CRYPTO_set_id_callback(SSLIdCallback);

  // 2. Use default configuration
  OPENSSL_config(nullptr);

  // 3. Init library, load all algorithms
  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();

  // 4. Create ssl context
  ssl_ctx_ = SSL_CTX_new(SSLv23_server_method());
  if (!ssl_ctx_) {
    LOG(WARNING) << "Unable to create SSL context";
    return -1;
  }

  // 5. Set cert file and key file, then check key file
  if (SSL_CTX_use_certificate_file(ssl_ctx_, cert_file.c_str(), SSL_FILETYPE_PEM) != 1) {
    LOG(WARNING) << "SSL_CTX_use_certificate_file(" << cert_file << ") failed";
    return -1;
  }

  if (SSL_CTX_use_PrivateKey_file(ssl_ctx_, key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
    LOG(WARNING) << "SSL_CTX_use_PrivateKey_file(" << key_file << ")";
    return -1;
  }

  if (SSL_CTX_check_private_key(ssl_ctx_) != 1) {
    LOG(WARNING) << "SSL_CTX_check_private_key(" << key_file << ")";
    return -1;
  }

  // https://wiki.openssl.org/index.php/Manual:SSL_CTX_set_read_ahead(3)
  // read data as more as possible
  SSL_CTX_set_read_ahead(ssl_ctx_, true);

  // Force using TLS 1.2
  SSL_CTX_set_options(ssl_ctx_, SSL_OP_NO_SSLv2);
  SSL_CTX_set_options(ssl_ctx_, SSL_OP_NO_SSLv3);
  SSL_CTX_set_options(ssl_ctx_, SSL_OP_NO_TLSv1);

  // Enable ECDH
  // https://en.wikipedia.org/wiki/Elliptic_curve_Diffie%E2%80%93Hellman
  // https://wiki.openssl.org/index.php/Diffie_Hellman
  // https://wiki.openssl.org/index.php/Diffie-Hellman_parameters
  EC_KEY* ecdh = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
  if (!ecdh) {
    LOG(WARNING) << "EC_KEY_new_by_curve_name(" << NID_X9_62_prime256v1 << ")";
    return -1;
  }

  SSL_CTX_set_options(ssl_ctx_, SSL_OP_SINGLE_ECDH_USE);
  SSL_CTX_set_tmp_ecdh(ssl_ctx_, ecdh);
  EC_KEY_free(ecdh);

  security_ = true;
  return 0;
}
#endif

}  // namespace net
