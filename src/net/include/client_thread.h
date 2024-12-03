// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_CLIENT_THREAD_H_
#define NET_INCLUDE_CLIENT_THREAD_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "net/include/net_thread.h"
#include "net/src/net_multiplexer.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"

// remove 'unused parameter' warning
#define UNUSED(expr) \
  do {               \
    (void)(expr);    \
  } while (0)

#define kConnWriteBuf (1024 * 1024 * 100)  // cache 100 MB data per connection

namespace net {

struct NetFiredEvent;
class ConnFactory;
class NetConn;

/*
 *  ClientHandle 类将在客户端线程的主循环中，在适当的时机被调用。
 *  它提供了多个虚函数，允许用户根据不同的事件自定义处理逻辑。
 */
class ClientHandle {
 public:
  // 默认构造函数，构造一个 ClientHandle 对象
  ClientHandle() = default;

  // 默认析构函数，销毁 ClientHandle 对象
  virtual ~ClientHandle() = default;

  /*
   *  CronHandle() 在每个时间间隔 (cron_interval) 过后被调用。
   *  用户可以在这里实现定时任务。
   */
  virtual void CronHandle() const {}

  /*
   *  FdTimeoutHandle(...) 在连接超时后被调用。
   *  这里可以处理连接超时的逻辑。
   *  参数:
   *    - fd: 超时的文件描述符
   *    - ip_port: 与该文件描述符相关联的IP:port地址
   */
  virtual void FdTimeoutHandle(int fd, const std::string& ip_port) const {
    UNUSED(fd);       // fd 参数未使用，标记为未使用
    UNUSED(ip_port);  // ip_port 参数未使用，标记为未使用
  }

  /*
   *  FdClosedHandle(...) 在连接关闭之前被调用。
   *  用户可以在这里实现连接关闭前的清理操作。
   *  参数:
   *    - fd: 即将关闭的文件描述符
   *    - ip_port: 与该文件描述符相关联的IP:port地址
   */
  virtual void FdClosedHandle(int fd, const std::string& ip_port) const {
    UNUSED(fd);       // fd 参数未使用，标记为未使用
    UNUSED(ip_port);  // ip_port 参数未使用，标记为未使用
  }

  /*
   *  AccessHandle(...) 在 Write 调用后但在处理之前被调用。
   *  这里可以做权限检查或连接访问控制等工作。
   *  参数:
   *    - ip: 目标IP地址
   *  返回:
   *    - 如果返回 true，则继续执行写操作；如果返回 false，则阻止写操作。
   */
  virtual bool AccessHandle(std::string& ip) const {
    UNUSED(ip);   // ip 参数未使用，标记为未使用
    return true;  // 默认返回 true，表示允许继续执行写操作
  }

  /*
   *  CreateWorkerSpecificData(...) 在 StartThread() 过程中被调用。
   *  该函数用于为线程创建特定的数据，'data' 指针应被分配。
   *  参数:
   *    - data: 用于存储线程特定数据的指针
   *  返回:
   *    - 如果成功，返回 0；否则返回错误码。
   */
  virtual int CreateWorkerSpecificData(void** data) const {
    UNUSED(data);  // data 参数未使用，标记为未使用
    return 0;      // 默认返回 0，表示创建成功
  }

  /*
   *  DeleteWorkerSpecificData(...) 在 StopThread() 过程中被调用。
   *  该函数用于删除在 CreateWorkerSpecificData 中创建的线程特定数据。
   *  参数:
   *    - data: 要删除的线程特定数据指针
   *  返回:
   *    - 如果成功，返回 0；否则返回错误码。
   */
  virtual int DeleteWorkerSpecificData(void* data) const {
    UNUSED(data);  // data 参数未使用，标记为未使用
    return 0;      // 默认返回 0，表示删除成功
  }

  /*
   * DestConnectFailedHandle(...) 当连接目标服务器失败时被调用。
   * 该方法允许用户在连接失败时执行自定义的处理逻辑。
   * 参数:
   *   - ip_port: 目标服务器的 IP:port 地址
   *   - reason: 连接失败的原因
   */
  virtual void DestConnectFailedHandle(const std::string& ip_port, const std::string& reason) const {
    UNUSED(ip_port);  // ip_port 参数未使用，标记为未使用
    UNUSED(reason);   // reason 参数未使用，标记为未使用
  }
};

class ClientThread : public Thread {
 public:
  ClientThread(ConnFactory* conn_factory, int cron_interval, int keepalive_timeout, ClientHandle* handle,
               void* private_data);
  ~ClientThread() override;
  /*
   * StartThread will return the error code as pthread_create return
   *  Return 0 if success
   */
  int StartThread() override;
  int StopThread() override;
  void set_thread_name(const std::string& name) override { Thread::set_thread_name(name); }
  pstd::Status Write(const std::string& ip, int port, const std::string& msg);
  pstd::Status Close(const std::string& ip, int port);

 private:
  void* ThreadMain() override;

  void InternalDebugPrint();
  // Set connect fd into epoll
  // connect condition: no EPOLLERR EPOLLHUP events,  no error in socket opt
  pstd::Status ProcessConnectStatus(NetFiredEvent* pfe, int* should_close);
  void SetWaitConnectOnEpoll(int sockfd);

  void NewConnection(const std::string& peer_ip, int peer_port, int sockfd);
  // Try to connect fd noblock, if return EINPROGRESS or EAGAIN or EWOULDBLOCK
  // put this fd in epoll (SetWaitConnectOnEpoll), process in ProcessConnectStatus
  pstd::Status ScheduleConnect(const std::string& dst_ip, int dst_port);
  void CloseFd(const std::shared_ptr<NetConn>& conn);
  void CloseFd(int fd, const std::string& ip_port);
  void CleanUpConnRemaining(const std::string& ip_port);
  void DoCronTask();
  void NotifyWrite(const std::string& ip_port);
  void ProcessNotifyEvents(const NetFiredEvent* pfe);

  int keepalive_timeout_;
  int cron_interval_;
  ClientHandle* handle_;
  bool own_handle_{false};
  void* private_data_;

  /*
   * The event handler
   */
  std::unique_ptr<NetMultiplexer> net_multiplexer_;

  ConnFactory* conn_factory_;

  pstd::Mutex mu_;
  std::map<std::string, std::vector<std::string>> to_send_;  // ip+":"+port, to_send_msg

  std::map<int, std::shared_ptr<NetConn>> fd_conns_;
  std::map<std::string, std::shared_ptr<NetConn>> ipport_conns_;
  std::set<int> connecting_fds_;

  pstd::Mutex to_del_mu_;
  std::vector<std::string> to_del_;
};

}  // namespace net
#endif  // NET_INCLUDE_CLIENT_THREAD_H_
