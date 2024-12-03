// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_H_  // 防止重复包含该头文件
#define PIKA_REPL_SERVER_H_

// 引入所需的头文件
#include <shared_mutex>  // 用于共享锁和独占锁
#include <utility>       // 用于移动语义
#include <vector>        // 用于 vector 容器
#include "net/include/thread_pool.h"

#include "include/pika_command.h"             // 包含 Pika 的命令定义
#include "include/pika_repl_bgworker.h"       // 背景工作线程
#include "include/pika_repl_server_thread.h"  // 服务器线程处理

// 定义一个结构体，表示复制服务器任务的参数
struct ReplServerTaskArg {
  std::shared_ptr<InnerMessage::InnerRequest> req;  // 请求对象
  std::shared_ptr<net::PbConn> conn;                // 连接对象
  // 构造函数初始化
  ReplServerTaskArg(std::shared_ptr<InnerMessage::InnerRequest> _req, std::shared_ptr<net::PbConn> _conn)
      : req(std::move(_req)), conn(std::move(_conn)) {}
};

// PikaReplServer 类，用于管理与从节点的复制同步任务
class PikaReplServer {
 public:
  // 构造函数，初始化时设置 IP 地址集、端口和定时器周期
  PikaReplServer(const std::set<std::string>& ips, int port, int cron_interval);

  // 析构函数
  ~PikaReplServer();

  // 启动复制服务器
  int Start();

  // 停止复制服务器
  int Stop();

  // 向从节点发送 binlog 数据块
  pstd::Status SendSlaveBinlogChips(const std::string& ip, int port, const std::vector<WriteTask>& tasks);

  // 向特定从节点发送消息
  pstd::Status Write(const std::string& ip, int port, const std::string& msg);

  // 将日志偏移量构建成响应格式
  void BuildBinlogOffset(const LogOffset& offset, InnerMessage::BinlogOffset* boffset);

  // 将 binlog 数据构建成响应格式
  void BuildBinlogSyncResp(const std::vector<WriteTask>& tasks, InnerMessage::InnerResponse* resp);

  // 调度任务到线程池执行
  void Schedule(net::TaskFunc func, void* arg);

  // 更新客户端连接映射表
  void UpdateClientConnMap(const std::string& ip_port, int fd);

  // 移除客户端连接
  void RemoveClientConn(int fd);

  // 关闭所有客户端连接
  void KillAllConns();

 private:
  std::unique_ptr<net::ThreadPool> server_tp_ = nullptr;                     // 服务器线程池
  std::unique_ptr<PikaReplServerThread> pika_repl_server_thread_ = nullptr;  // 复制服务器线程
  std::shared_mutex client_conn_rwlock_;                                     // 用于保护客户端连接的读写锁
  std::map<std::string, int> client_conn_map_;  // 客户端连接映射表（IP:port -> fd）
};

#endif
