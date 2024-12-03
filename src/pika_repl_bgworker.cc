// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include "include/pika_cmd_table_manager.h"
#include "include/pika_conf.h"
#include "include/pika_repl_bgworker.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "pstd/include/pstd_defer.h"
#include "src/pstd/include/scope_record_lock.h"

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

PikaReplBgWorker::PikaReplBgWorker(int queue_size) : bg_thread_(queue_size) {
  bg_thread_.set_thread_name("ReplBgWorker");
  net::RedisParserSettings settings;
  settings.DealMessage = &(PikaReplBgWorker::HandleWriteBinlog);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
  db_name_ = g_pika_conf->default_db();
}

int PikaReplBgWorker::StartThread() { return bg_thread_.StartThread(); }

int PikaReplBgWorker::StopThread() { return bg_thread_.StopThread(); }

void PikaReplBgWorker::Schedule(net::TaskFunc func, void* arg) { bg_thread_.Schedule(func, arg); }

void PikaReplBgWorker::Schedule(net::TaskFunc func, void* arg, std::function<void()>& call_back) {
  bg_thread_.Schedule(func, arg, call_back);
}

void PikaReplBgWorker::ParseBinlogOffset(const InnerMessage::BinlogOffset& pb_offset, LogOffset* offset) {
  offset->b_offset.filenum = pb_offset.filenum();
  offset->b_offset.offset = pb_offset.offset();
  offset->l_offset.term = pb_offset.term();
  offset->l_offset.index = pb_offset.index();
}
void PikaReplBgWorker::HandleBGWorkerWriteBinlog(void* arg) {
  // 将任务参数转换为具体类型
  auto task_arg = static_cast<ReplClientWriteBinlogTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerResponse> res = task_arg->res;   // 获取响应对象
  std::shared_ptr<net::PbConn> conn = task_arg->conn;                       // 获取连接对象
  auto index = static_cast<std::vector<int>*>(task_arg->res_private_data);  // 获取索引列表
  PikaReplBgWorker* worker = task_arg->worker;                              // 获取背景工作者对象
  worker->ip_port_ = conn->ip_port();                                       // 保存连接的 IP:端口

  // 使用 DEFER 确保函数退出时清理资源
  DEFER {
    delete index;     // 删除索引列表
    delete task_arg;  // 删除任务参数
  };

  std::string db_name;          // 数据库名称
  LogOffset pb_begin;           // 存储 binlog 的开始偏移量
  LogOffset pb_end;             // 存储 binlog 的结束偏移量
  bool only_keepalive = false;  // 是否只包含 keepalive 信息

  // 查找第一个非 keepalive 的 binlog_sync
  for (size_t i = 0; i < index->size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync((*index)[i]);
    if (i == 0) {
      db_name = binlog_res.slot().db_name();  // 第一个 binlog 的数据库名称
    }
    if (!binlog_res.binlog().empty()) {
      ParseBinlogOffset(binlog_res.binlog_offset(), &pb_begin);  // 解析开始偏移量
      break;
    }
  }

  // 查找最后一个非 keepalive 的 binlog_sync
  for (int i = static_cast<int>(index->size() - 1); i >= 0; i--) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync((*index)[i]);
    if (!binlog_res.binlog().empty()) {
      ParseBinlogOffset(binlog_res.binlog_offset(), &pb_end);  // 解析结束偏移量
      break;
    }
  }

  // 如果没有找到非 keepalive 的 binlog，则设置 only_keepalive 为 true
  if (pb_begin == LogOffset()) {
    only_keepalive = true;
  }

  // 确定确认的开始偏移量
  LogOffset ack_start;
  if (only_keepalive) {
    ack_start = LogOffset();  // 只有 keepalive 时，不需要确认偏移量
  } else {
    ack_start = pb_begin;  // 否则使用第一个 binlog 的偏移量
  }

  // 更新背景工作者的数据库名称
  worker->db_name_ = db_name;

  // 获取同步主数据库实例
  std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!db) {
    LOG(WARNING) << "DB " << db_name << " Not Found";  // 如果未找到数据库，记录警告并返回
    return;
  }

  // 获取同步从数据库实例
  std::shared_ptr<SyncSlaveDB> slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    LOG(WARNING) << "Slave DB " << db_name << " Not Found";  // 如果未找到从数据库，记录警告并返回
    return;
  }

  // 遍历所有的 binlog 索引
  for (int i : *index) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync(i);

    // 如果 Pika 不是从节点或从数据库不在 BinlogSync 状态，则丢弃剩余的 binlog 写任务
    if (((g_pika_server->role() & PIKA_ROLE_SLAVE) == 0) ||
        ((slave_db->State() != ReplState::kConnected) && (slave_db->State() != ReplState::kWaitDBSync))) {
      return;
    }

    // 检查 session_id 是否匹配
    if (slave_db->MasterSessionId() != binlog_res.session_id()) {
      LOG(WARNING) << "Check SessionId Mismatch: " << slave_db->MasterIp() << ":" << slave_db->MasterPort() << ", "
                   << slave_db->SyncDBInfo().ToString() << " expected_session: " << binlog_res.session_id()
                   << ", actual_session:" << slave_db->MasterSessionId();
      LOG(WARNING) << "Check Session failed " << binlog_res.slot().db_name();
      slave_db->SetReplState(ReplState::kTryConnect);  // 如果 session_id 不匹配，设置从库状态为重连状态
      return;
    }

    // 如果 binlog 为空，视为 keepalive 包，跳过当前 binlog
    if (binlog_res.binlog().empty()) {
      continue;
    }

    // 解码 binlog 项
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog_res.binlog(), &worker->binlog_item_)) {
      LOG(WARNING) << "Binlog item decode failed";  // 如果解码失败，记录警告并设置从库为重连状态
      slave_db->SetReplState(ReplState::kTryConnect);
      return;
    }

    // 解析 Redis 数据
    const char* redis_parser_start = binlog_res.binlog().data() + BINLOG_ENCODE_LEN;
    int redis_parser_len = static_cast<int>(binlog_res.binlog().size()) - BINLOG_ENCODE_LEN;
    int processed_len = 0;
    net::RedisParserStatus ret =
        worker->redis_parser_.ProcessInputBuffer(redis_parser_start, redis_parser_len, &processed_len);
    if (ret != net::kRedisParserDone) {
      LOG(WARNING) << "Redis parser failed";  // 如果 Redis 解析失败，记录警告并设置从库为重连状态
      slave_db->SetReplState(ReplState::kTryConnect);
      return;
    }
  }

  // 确定确认的结束偏移量
  LogOffset ack_end;
  if (only_keepalive) {
    ack_end = LogOffset();  // 只有 keepalive 时，结束偏移量为空
  } else {
    LogOffset productor_status;
    // 获取生产者状态
    std::shared_ptr<Binlog> logger = db->Logger();
    logger->GetProducerStatus(&productor_status.b_offset.filenum, &productor_status.b_offset.offset,
                              &productor_status.l_offset.term, &productor_status.l_offset.index);
    ack_end = productor_status;                    // 设置确认的结束偏移量
    ack_end.l_offset.term = pb_end.l_offset.term;  // 使用最后一个 binlog 的 term
  }

  // 发送 binlog 确认请求
  g_pika_rm->SendBinlogSyncAckRequest(db_name, ack_start, ack_end);
}
// 处理写入 binlog 的命令
int PikaReplBgWorker::HandleWriteBinlog(net::RedisParser* parser, const net::RedisCmdArgsType& argv) {
  std::string opt = argv[0];                                   // 获取命令的名称
  auto worker = static_cast<PikaReplBgWorker*>(parser->data);  // 获取当前的工作者实例

  // 监控相关处理
  std::string monitor_message;
  if (g_pika_server->HasMonitorClients()) {            // 如果有监控客户端
    std::string db_name = worker->db_name_.substr(2);  // 获取数据库名称（去除前缀）
    // 构建监控信息
    monitor_message = std::to_string(static_cast<double>(pstd::NowMicros()) / 1000000) + " [" + db_name + " " +
                      worker->ip_port_ + "]";
    for (const auto& item : argv) {  // 拼接命令参数
      monitor_message += " " + pstd::ToRead(item);
    }
    g_pika_server->AddMonitorMessage(monitor_message);  // 将信息加入监控消息队列
  }

  // 获取命令表中的命令
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(pstd::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Command " << opt << " not in the command db";  // 如果命令不存在，记录警告并返回错误
    return -1;
  }

  // 初始化命令
  c_ptr->Initial(argv, worker->db_name_);
  if (!c_ptr->res().ok()) {  // 如果命令初始化失败，记录警告并返回错误
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    return -1;
  }

  // 更新查询数量和执行计数
  g_pika_server->UpdateQueryNumAndExecCountDB(worker->db_name_, opt, c_ptr->is_write());

  // 获取同步主数据库
  std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(worker->db_name_));
  if (!db) {
    LOG(WARNING) << worker->db_name_ << " Not found.";  // 如果找不到数据库，记录警告
  }

  // 处理领导日志（Consensus 过程）
  db->ConsensusProcessLeaderLog(c_ptr, worker->binlog_item_);
  return 0;
}

// 处理回调工作线程写入数据库的任务
void PikaReplBgWorker::HandleBGWorkerWriteDB(void* arg) {
  // 将任务参数转为特定类型并创建智能指针
  std::unique_ptr<ReplClientWriteDBTaskArg> task_arg(static_cast<ReplClientWriteDBTaskArg*>(arg));
  const std::shared_ptr<Cmd> c_ptr = task_arg->cmd_ptr;  // 获取命令指针
  WriteDBInSyncWay(c_ptr);                               // 以同步方式写入数据库
}

// 以同步方式执行数据库写操作
void PikaReplBgWorker::WriteDBInSyncWay(const std::shared_ptr<Cmd>& c_ptr) {
  const PikaCmdArgsType& argv = c_ptr->argv();  // 获取命令的参数列表

  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {  // 如果配置了慢日志
    start_us = pstd::NowMicros();                 // 获取当前时间
  }

  // 对命令的当前键进行多重锁操作（避免并发问题）
  pstd::lock::MultiRecordLock record_lock(c_ptr->GetDB()->LockMgr());
  record_lock.Lock(c_ptr->current_key());

  // 如果命令没有挂起，则获取数据库的共享锁
  if (!c_ptr->IsSuspend()) {
    c_ptr->GetDB()->DBLockShared();
  }

  // 如果需要缓存处理，并且配置了缓存且缓存状态正常
  if (c_ptr->IsNeedCacheDo() && PIKA_CACHE_NONE != g_pika_conf->cache_mode() &&
      c_ptr->GetDB()->cache()->CacheStatus() == PIKA_CACHE_STATUS_OK) {
    if (c_ptr->is_write()) {
      c_ptr->DoThroughDB();  // 执行数据库写操作
      if (c_ptr->IsNeedUpdateCache()) {
        c_ptr->DoUpdateCache();  // 更新缓存
      }
    } else {
      LOG(WARNING) << "It is impossible to reach here";  // 如果不是写操作，记录警告
    }
  } else {
    c_ptr->Do();  // 执行非缓存操作
  }

  // 如果命令没有挂起，则释放数据库的共享锁
  if (!c_ptr->IsSuspend()) {
    c_ptr->GetDB()->DBUnlockShared();
  }

  // 处理写操作和慢查询日志
  if (c_ptr->res().ok() && c_ptr->is_write() && c_ptr->name() != kCmdNameFlushdb && c_ptr->name() != kCmdNameFlushall &&
      c_ptr->name() != kCmdNameExec) {
    auto table_keys = c_ptr->current_key();
    // 将当前命令的键添加到表的前缀中
    for (auto& key : table_keys) {
      key = c_ptr->db_name().append(key);
    }

    // 获取事务相关连接
    auto dispatcher = dynamic_cast<net::DispatchThread*>(g_pika_server->pika_dispatch_thread()->server_thread());
    auto involved_conns = dispatcher->GetInvolvedTxn(table_keys);
    for (auto& conn : involved_conns) {
      auto c = std::dynamic_pointer_cast<PikaClientConn>(conn);
      c->SetTxnWatchFailState(true);  // 标记连接的事务失败状态
    }
  }

  // 释放锁
  record_lock.Unlock(c_ptr->current_key());

  // 记录慢查询日志
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    auto start_time = static_cast<int32_t>(start_us / 1000000);          // 获取命令开始时间（秒）
    auto duration = static_cast<int64_t>(pstd::NowMicros() - start_us);  // 计算命令执行时长（微秒）
    if (duration > g_pika_conf->slowlog_slower_than()) {                 // 如果执行时间超过配置的阈值
      g_pika_server->SlowlogPushEntry(argv, start_time, duration);       // 记录到慢查询日志
      if (g_pika_conf->slowlog_write_errorlog()) {
        LOG(ERROR) << "command: " << argv[0] << ", start_time(s): " << start_time << ", duration(us): " << duration;
      }
    }
  }
}
