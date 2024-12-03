// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_HOLY_THREAD_H_
#define NET_SRC_HOLY_THREAD_H_

#include <atomic>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "net/include/net_conn.h"
#include "net/include/server_thread.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/xdebug.h"

namespace net {
class NetConn;

class HolyThread : public ServerThread {
 public:
  HolyThread(int port, ConnFactory* conn_factory, int cron_interval = 0, const ServerHandle* handle = nullptr,
             bool async = true);

  HolyThread(const std::string& bind_ip, int port, ConnFactory* conn_factory, int cron_interval = 0,
             const ServerHandle* handle = nullptr, bool async = true);
  HolyThread(const std::set<std::string>& bind_ips, int port, ConnFactory* conn_factory, int cron_interval = 0,
             const ServerHandle* handle = nullptr, bool async = true);

  ~HolyThread() override;

  int StartThread() override;

  int StopThread() override;

  void set_thread_name(const std::string& name) override { Thread::set_thread_name(name); }

  // 设置连接的保活超时时间。
  void set_keepalive_timeout(int timeout) override { keepalive_timeout_ = timeout; }

  //@return 当前连接数量。

  int conn_num() const override;

  // 获取所有连接的信息。
  std::vector<ServerThread::ConnInfo> conns_info() const override;

  // 将指定文件描述符的连接移出管理。
  std::shared_ptr<NetConn> MoveConnOut(int fd) override;

  // 将外部连接移入管理
  void MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) override {}

  // 关闭并移除所有连接。
  void KillAllConns() override;

  // 根据 IP:Port 格式标识关闭指定连接。
  bool KillConn(const std::string& ip_port) override;

  // 获取指定文件描述符对应的连接。
  virtual std::shared_ptr<NetConn> get_conn(int fd);

  // 处理通知事件。

  void ProcessNotifyEvents(const net::NetFiredEvent* pfe) override;

  void Cleanup();

 private:
  mutable pstd::RWMutex rwlock_;                   // 用于保护外部统计信息的读写锁。
  std::map<int, std::shared_ptr<NetConn>> conns_;  // 当前管理的连接映射表。

  ConnFactory* conn_factory_ = nullptr;  // 连接工厂实例。
  void* private_data_ = nullptr;         // 私有数据指针。

  std::atomic<int> keepalive_timeout_;  // 连接保活超时时间（秒）。
  bool async_;                          // 是否以异步模式运行。

  // 执行定时任务的实现。

  void DoCronTask() override;

  pstd::Mutex killer_mutex_;                    // 保护删除连接操作的互斥锁。
  std::set<std::string> deleting_conn_ipport_;  // 正在删除的连接 IP:Port 集合。

  // 处理新的连接。

  void HandleNewConn(int connfd, const std::string& ip_port) override;

  // 处理连接事件。
  void HandleConnEvent(NetFiredEvent* pfe) override;

  // 关闭指定的连接。

  void CloseFd(const std::shared_ptr<NetConn>& conn);
};  // class HolyThread

}  // namespace net
#endif  // NET_SRC_HOLY_THREAD_H_
