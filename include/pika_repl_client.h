// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_H_  // 防止重复包含该头文件
#define PIKA_REPL_CLIENT_H_

// 引入所需的头文件
#include <memory>   // 用于智能指针
#include <string>   // 用于字符串操作
#include <utility>  // 用于 std::move

#include "include/pika_define.h"        // Pika 的定义文件
#include "net/include/client_thread.h"  // 引入客户端线程管理
#include "net/include/net_conn.h"       // 网络连接管理
#include "net/include/thread_pool.h"    // 线程池管理
#include "pstd/include/pstd_status.h"   // 状态定义

#include "include/pika_binlog_reader.h"       // Binlog 读取器
#include "include/pika_repl_bgworker.h"       // 复制背景工作线程
#include "include/pika_repl_client_thread.h"  // 复制客户端线程

#include "net/include/thread_pool.h"  // 重复包含线程池库
#include "pika_inner_message.pb.h"    // Pika 内部消息协议

// 定义用于存储复制客户端任务的结构体
struct ReplClientTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;  // 内部响应对象
  std::shared_ptr<net::PbConn> conn;                 // 连接对象
  // 构造函数初始化
  ReplClientTaskArg(const std::shared_ptr<InnerMessage::InnerResponse>& _res, const std::shared_ptr<net::PbConn>& _conn)
      : res(_res), conn(_conn) {}
};

// 复制客户端写 Binlog 任务参数结构体
struct ReplClientWriteBinlogTaskArg {
  std::shared_ptr<InnerMessage::InnerResponse> res;  // 内部响应对象
  std::shared_ptr<net::PbConn> conn;                 // 连接对象
  void* res_private_data;                            // 响应私有数据
  PikaReplBgWorker* worker;                          // 工作线程
  // 构造函数初始化
  ReplClientWriteBinlogTaskArg(const std::shared_ptr<InnerMessage::InnerResponse>& _res,
                               const std::shared_ptr<net::PbConn>& _conn, void* _res_private_data,
                               PikaReplBgWorker* _worker)
      : res(_res), conn(_conn), res_private_data(_res_private_data), worker(_worker) {}
};

// 复制客户端写 DB 任务参数结构体
struct ReplClientWriteDBTaskArg {
  const std::shared_ptr<Cmd> cmd_ptr;  // 命令指针
  // 构造函数初始化
  explicit ReplClientWriteDBTaskArg(std::shared_ptr<Cmd> _cmd_ptr) : cmd_ptr(std::move(_cmd_ptr)) {}
  ~ReplClientWriteDBTaskArg() = default;
};

// 复制客户端类
class PikaReplClient {
 public:
  // 构造函数，初始化复制客户端，包括定时器周期和保持连接超时
  PikaReplClient(int cron_interval, int keepalive_timeout);

  // 析构函数
  ~PikaReplClient();

  // 启动客户端
  int Start();

  // 停止客户端
  int Stop();

  // 向特定服务器发送消息
  pstd::Status Write(const std::string& ip, int port, const std::string& msg);

  // 关闭与特定服务器的连接
  pstd::Status Close(const std::string& ip, int port);

  // 调度任务到线程池
  void Schedule(net::TaskFunc func, void* arg);

  // 根据数据库名称调度任务到线程池
  void ScheduleByDBName(net::TaskFunc func, void* arg, const std::string& db_name);

  // 调度写 Binlog 任务
  void ScheduleWriteBinlogTask(const std::string& db_name, const std::shared_ptr<InnerMessage::InnerResponse>& res,
                               const std::shared_ptr<net::PbConn>& conn, void* res_private_data);

  // 调度写 DB 任务
  void ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const std::string& db_name);

  // 发送元数据同步请求
  pstd::Status SendMetaSync();

  // 发送数据库同步请求
  pstd::Status SendDBSync(const std::string& ip, uint32_t port, const std::string& db_name, const BinlogOffset& boffset,
                          const std::string& local_ip);

  // 尝试同步请求
  pstd::Status SendTrySync(const std::string& ip, uint32_t port, const std::string& db_name,
                           const BinlogOffset& boffset, const std::string& local_ip);

  // 发送 Binlog 同步请求
  pstd::Status SendBinlogSync(const std::string& ip, uint32_t port, const std::string& db_name,
                              const LogOffset& ack_start, const LogOffset& ack_end, const std::string& local_ip,
                              bool is_first_send);

  // 发送移除从节点的请求
  pstd::Status SendRemoveSlaveNode(const std::string& ip, uint32_t port, const std::string& db_name,
                                   const std::string& local_ip);

  // 增加异步 DB 写任务计数
  void IncrAsyncWriteDBTaskCount(const std::string& db_name, int32_t incr_step) {
    int32_t db_index = db_name.back() - '0';  // 从数据库名称获取数据库索引
    assert(db_index >= 0 && db_index <= 7);   // 确保数据库索引在有效范围内
    async_write_db_task_counts_[db_index].fetch_add(incr_step, std::memory_order::memory_order_seq_cst);  // 增加计数
  }

  // 减少异步 DB 写任务计数
  void DecrAsyncWriteDBTaskCount(const std::string& db_name, int32_t incr_step) {
    int32_t db_index = db_name.back() - '0';  // 从数据库名称获取数据库索引
    assert(db_index >= 0 && db_index <= 7);   // 确保数据库索引在有效范围内
    async_write_db_task_counts_[db_index].fetch_sub(incr_step, std::memory_order::memory_order_seq_cst);  // 减少计数
  }

  // 获取未完成的异步 DB 写任务计数
  int32_t GetUnfinishedAsyncWriteDBTaskCount(const std::string& db_name) {
    int32_t db_index = db_name.back() - '0';  // 从数据库名称获取数据库索引
    assert(db_index >= 0 && db_index <= 7);   // 确保数据库索引在有效范围内
    return async_write_db_task_counts_[db_index].load(std::memory_order_seq_cst);  // 获取当前计数
  }

 private:
  // 根据数据库名称获取对应的 Binlog 工作线程索引
  size_t GetBinlogWorkerIndexByDBName(const std::string& db_name);

  // 根据键获取哈希索引
  size_t GetHashIndexByKey(const std::string& key);

  // 更新下一个可用的工作线程索引
  void UpdateNextAvail() { next_avail_ = (next_avail_ + 1) % static_cast<int32_t>(write_binlog_workers_.size()); }

  std::unique_ptr<PikaReplClientThread> client_thread_;  // 复制客户端线程
  int next_avail_ = 0;                                   // 下一个可用工作线程的索引
  std::hash<std::string> str_hash;                       // 字符串哈希函数

  // 异步写 DB 任务计数器，用于处理 Binlog
  std::atomic<int32_t> async_write_db_task_counts_[MAX_DB_NUM];

  // 写 Binlog 工作线程池
  std::vector<std::unique_ptr<PikaReplBgWorker>> write_binlog_workers_;

  // 写 DB 工作线程池
  std::vector<std::unique_ptr<PikaReplBgWorker>> write_db_workers_;
};

#endif
