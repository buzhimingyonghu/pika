// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fmt/format.h>
#include <glog/logging.h>
#include <utility>
#include <vector>

#include "include/pika_admin.h"
#include "include/pika_client_conn.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "net/src/dispatch_thread.h"
#include "net/src/worker_thread.h"
#include "src/pstd/include/scope_record_lock.h"

extern std::unique_ptr<PikaConf> g_pika_conf;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

PikaClientConn::PikaClientConn(int fd, const std::string& ip_port, net::Thread* thread, net::NetMultiplexer* mpx,
                               const net::HandleType& handle_type, int max_conn_rbuf_size)
    : RedisConn(fd, ip_port, thread, mpx, handle_type, max_conn_rbuf_size),
      server_thread_(reinterpret_cast<net::ServerThread*>(thread)),
      current_db_(g_pika_conf->default_db()) {
  InitUser();
  time_stat_.reset(new TimeStat());
}

std::shared_ptr<Cmd> PikaClientConn::DoCmd(const PikaCmdArgsType& argv, const std::string& opt,
                                           const std::shared_ptr<std::string>& resp_ptr, bool cache_miss_in_rtc) {
  // 获取对应命令的命令对象
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(opt);

  // 如果命令不存在，返回一个 DummyCmd 对象并设置错误信息
  if (!c_ptr) {
    std::shared_ptr<Cmd> tmp_ptr = std::make_shared<DummyCmd>(DummyCmd());
    tmp_ptr->res().SetRes(CmdRes::kErrOther, "unknown command \"" + opt + "\"");  // 设置未知命令错误
    if (IsInTxn()) {
      SetTxnInitFailState(true);  // 如果在事务中，标记事务初始化失败
    }
    return tmp_ptr;  // 返回 DummyCmd 对象
  }

  // 设置缓存缺失标记
  c_ptr->SetCacheMissedInRtc(cache_miss_in_rtc);
  // 设置连接对象
  c_ptr->SetConn(shared_from_this());
  // 设置响应指针
  c_ptr->SetResp(resp_ptr);

  // 检查是否需要认证
  if (AuthRequired()) {                        // 用户未认证，需进行认证
    if (!(c_ptr->flag() & kCmdFlagsNoAuth)) {  // 如果命令没有标记为不需要认证
      c_ptr->res().SetRes(CmdRes::kErrOther, "NOAUTH Authentication required.");  // 设置认证错误
      return c_ptr;                                                               // 返回命令对象
    }
  }

  // 初始化命令对象，传入命令参数和当前数据库
  c_ptr->Initial(argv, current_db_);
  if (!c_ptr->res().ok()) {  // 如果初始化失败
    if (IsInTxn()) {
      SetTxnInitFailState(true);  // 如果在事务中，标记事务初始化失败
    }
    return c_ptr;  // 返回初始化失败的命令对象
  }

  int8_t subCmdIndex = -1;
  std::string errKey;
  // 检查用户是否有权限执行该命令
  auto checkRes = user_->CheckUserPermission(c_ptr, argv, subCmdIndex, &errKey);
  std::string cmdName = c_ptr->name();
  if (subCmdIndex >= 0 && checkRes == AclDeniedCmd::CMD) {
    cmdName += "|" + argv[1];  // 如果有子命令，附加子命令名称
  }

  std::string object;
  switch (checkRes) {  // 根据权限检查结果处理不同的权限拒绝情况
    case AclDeniedCmd::CMD:
      c_ptr->res().SetRes(CmdRes::kNone, fmt::format("-NOPERM this user has no permissions to run the '{}' command\r\n",
                                                     pstd::StringToLower(cmdName)));
      object = cmdName;
      break;
    case AclDeniedCmd::KEY:
      c_ptr->res().SetRes(CmdRes::kNone,
                          "-NOPERM this user has no permissions to access one of the keys used as arguments\r\n");
      object = errKey;
      break;
    case AclDeniedCmd::CHANNEL:
      c_ptr->res().SetRes(CmdRes::kNone,
                          "-NOPERM this user has no permissions to access one of the channel used as arguments\r\n");
      object = errKey;
      break;
    case AclDeniedCmd::NO_SUB_CMD:
      c_ptr->res().SetRes(CmdRes::kErrOther, fmt::format("unknown subcommand '{}' subcommand", argv[1]));
      break;
    case AclDeniedCmd::NO_AUTH:
      c_ptr->res().AppendContent("-NOAUTH Authentication required.");
      break;
    default:
      break;
  }

  // 记录权限日志
  if (checkRes == AclDeniedCmd::CMD || checkRes == AclDeniedCmd::KEY || checkRes == AclDeniedCmd::CHANNEL) {
    std::string cInfo;
    ClientInfoToString(&cInfo, cmdName);
    int32_t context = IsInTxn() ? static_cast<int32_t>(AclLogCtx::MULTI) : static_cast<int32_t>(AclLogCtx::TOPLEVEL);

    if (checkRes == AclDeniedCmd::CMD && IsInTxn() && cmdName == kCmdNameExec) {
      object = kCmdNameMulti;
    }
    // 记录日志
    g_pika_server->Acl()->AddLogEntry(static_cast<int32_t>(checkRes), context, user_->Name(), object, cInfo);

    return c_ptr;  // 返回权限拒绝的命令对象
  }

  // 如果在事务中，检查写操作是否允许
  if (IsInTxn() && opt != kCmdNameExec && opt != kCmdNameWatch && opt != kCmdNameDiscard && opt != kCmdNameMulti) {
    if (c_ptr->is_write() && g_pika_server->readonly(current_db_)) {
      SetTxnInitFailState(true);
      c_ptr->res().SetRes(CmdRes::kErrOther, "READONLY You can't write against a read only replica.");
      return c_ptr;
    }
    // 将命令加入事务队列
    PushCmdToQue(c_ptr);
    c_ptr->res().SetRes(CmdRes::kTxnQueued);  // 返回事务排队状态
    return c_ptr;
  }

  // 检查是否有监控客户端
  bool is_monitoring = g_pika_server->HasMonitorClients();
  if (is_monitoring) {
    ProcessMonitor(argv);  // 处理监控命令
  }

  // 更新命令执行统计
  g_pika_server->UpdateQueryNumAndExecCountDB(current_db_, opt, c_ptr->is_write());

  // 如果是发布订阅连接，检查命令是否允许
  if (this->IsPubSub()) {
    if (opt != kCmdNameSubscribe && opt != kCmdNameUnSubscribe && opt != kCmdNamePing && opt != kCmdNamePSubscribe &&
        opt != kCmdNamePUnSubscribe) {
      c_ptr->res().SetRes(CmdRes::kErrOther,
                          "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
      return c_ptr;
    }
  }

  // 如果处于领导保护模式，拒绝所有请求
  if (g_pika_server->leader_protected_mode()) {
    c_ptr->res().SetRes(CmdRes::kErrOther, "Cannot process command before new leader sync finished");
    return c_ptr;
  }

  // 检查数据库是否存在
  if (!g_pika_server->IsDBExist(current_db_)) {
    c_ptr->res().SetRes(CmdRes::kErrOther, "DB not found");
    return c_ptr;
  }

  // 处理写操作的相关检查
  if (c_ptr->is_write()) {
    // 检查写操作是否成功
    if (g_pika_server->IsDBBinlogIoError(current_db_)) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Writing binlog failed, maybe no space left on device");
      return c_ptr;
    }
    std::vector<std::string> cur_key = c_ptr->current_key();
    if (cur_key.empty() && opt != kCmdNameExec) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
      return c_ptr;
    }
    // 如果是只读数据库，禁止写操作
    if (g_pika_server->readonly(current_db_) && opt != kCmdNameExec) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "READONLY You can't write against a read only replica.");
      return c_ptr;
    }
  } else if (c_ptr->is_read() && c_ptr->flag_ == 0) {
    const auto& server_guard = std::lock_guard(g_pika_server->GetDBLock());
    int role = 0;
    auto status = g_pika_rm->CheckDBRole(current_db_, &role);
    if (!status.ok()) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
      return c_ptr;
    } else if ((role & PIKA_ROLE_SLAVE) == PIKA_ROLE_SLAVE) {
      const auto& slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(current_db_));
      if (!slave_db) {
        c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
        return c_ptr;
      } else if (slave_db->State() != ReplState::kConnected) {
        c_ptr->res().SetRes(CmdRes::kErrOther, "Full sync not completed");
        return c_ptr;
      }
    }
  }

  // 处理写命令失败的事务标记
  if (c_ptr->res().ok() && c_ptr->is_write() && name() != kCmdNameExec) {
    if (c_ptr->name() == kCmdNameFlushdb) {
      auto flushdb = std::dynamic_pointer_cast<FlushdbCmd>(c_ptr);
      SetTxnFailedIfKeyExists(flushdb->GetFlushDBname());
    } else if (c_ptr->name() == kCmdNameFlushall) {
      SetTxnFailedIfKeyExists();
    } else {
      auto table_keys = c_ptr->current_key();
      for (auto& key : table_keys) {
        key = c_ptr->db_name().append("_").append(key);
      }
      SetTxnFailedFromKeys(table_keys);
    }
  }

  // 执行命令
  c_ptr->Execute();

  // 更新命令处理时间和统计信息
  time_stat_->process_done_ts_ = pstd::NowMicros();
  LOG(INFO) << "DOCMD process_done_ts_: " << time_stat_->process_done_ts_ / 1000;
  LOG(INFO) << "DOCMD process_time: " << time_stat_->process_time() / 1000;
  auto cmdstat_map = g_pika_cmd_table_manager->GetCommandStatMap();
  (*cmdstat_map)[opt].cmd_count.fetch_add(1);
  (*cmdstat_map)[opt].cmd_time_consuming.fetch_add(time_stat_->total_time());
  LOG(INFO) << "DOCMD: " << time_stat_->total_time() / 1000;

  // 如果配置了慢查询日志，处理慢查询
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    ProcessSlowlog(argv, c_ptr->GetDoDuration());
  }

  return c_ptr;  // 返回执行后的命令对象
}

void PikaClientConn::ProcessSlowlog(const PikaCmdArgsType& argv, uint64_t do_duration) {
  // 检查当前命令的执行时间是否超过慢查询阈值
  if (time_stat_->total_time() > g_pika_conf->slowlog_slower_than()) {
    // 将慢查询命令信息记录到慢查询日志
    g_pika_server->SlowlogPushEntry(argv, time_stat_->start_ts() / 1000000, time_stat_->total_time());

    // 如果配置要求将慢查询写入错误日志
    if (g_pika_conf->slowlog_write_errorlog()) {
      bool trim = false;      // 标记是否需要截断日志
      std::string slow_log;   // 用于存储命令的字符串形式
      uint32_t cmd_size = 0;  // 用于存储命令的总字节数（包括空格和参数）

      // 遍历命令参数并构建日志
      for (const auto& i : argv) {
        cmd_size += 1 + i.size();  // 计算命令参数的长度（包括空格）

        if (!trim) {
          slow_log.append(" ");              // 添加空格
          slow_log.append(pstd::ToRead(i));  // 将参数转为可读格式并追加到慢查询日志中

          // 如果命令参数长度过长，截断并标记
          if (slow_log.size() >= 1000) {
            trim = true;               // 标记为需要截断
            slow_log.resize(1000);     // 截断日志
            slow_log.append("...\"");  // 添加省略号表示截断
          }
        }
      }
      // 将慢查询信息记录到错误日志中
      LOG(ERROR) << "ip_port: " << ip_port() << ", db: " << current_db_ << ", command:" << slow_log
                 << ", command_size: " << cmd_size - 1 << ", arguments: " << argv.size()
                 << ", total_time(ms): " << time_stat_->total_time() / 1000                // 总耗时（毫秒）
                 << ", before_queue_time(ms): " << time_stat_->before_queue_time() / 1000  // 排队前耗时（毫秒）
                 << ", queue_time(ms): " << time_stat_->queue_time() / 1000                // 排队耗时（毫秒）
                 << ", process_time(ms): " << time_stat_->process_time() / 1000            // 处理时间（毫秒）
                 << ", cmd_time(ms): " << do_duration / 1000;  // 当前命令执行时间（毫秒）
    }
  }
}

void PikaClientConn::ProcessMonitor(const PikaCmdArgsType& argv) {
  std::string monitor_message;
  std::string db_name = current_db_.substr(2);
  monitor_message = std::to_string(1.0 * static_cast<double>(pstd::NowMicros()) / 1000000) + " [" + db_name + " " +
                    this->ip_port() + "]";
  for (const auto& iter : argv) {
    monitor_message += " " + pstd::ToRead(iter);
  }
  g_pika_server->AddMonitorMessage(monitor_message);
}

bool PikaClientConn::IsInterceptedByRTC(std::string& opt) {
  // currently we only Intercept: Get, HGet
  if (opt == kCmdNameGet && g_pika_conf->GetCacheString()) {
    return true;
  }
  if (opt == kCmdNameHGet && g_pika_conf->GetCacheHash()) {
    return true;
  }
  return false;
}
void PikaClientConn::ProcessRedisCmds(const std::vector<net::RedisCmdArgsType>& argvs, bool async,
                                      std::string* response) {
  // 重置时间统计信息
  time_stat_->Reset();

  if (async) {
    // 如果是异步模式，创建后台任务参数
    auto arg = new BgTaskArg();
    arg->cache_miss_in_rtc_ = false;  // 初始化 RTC 缓存未命中的标志
    arg->redis_cmds = argvs;          // 保存 Redis 命令
    time_stat_->enqueue_ts_ = time_stat_->before_queue_ts_ = pstd::NowMicros();  // 记录入队时间

    // 保存当前连接的智能指针到任务参数
    arg->conn_ptr = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());

    /**
     * 关于批量命令的处理：
     * - 如果使用 pipeline 方法将批量命令发送到 Pika，则无法正确区分快慢命令。
     * - 如果是 Codis 的 pipeline 方法，可以正确区分快慢命令，但不能保证按顺序执行。
     */

    // 获取命令的第一个参数（即操作类型，例如 GET、SET）
    std::string opt = argvs[0][0];
    pstd::StringToLower(opt);  // 转换为小写以便统一处理

    // 判断命令是否为慢命令或管理员命令
    bool is_slow_cmd = g_pika_conf->is_slow_cmd(opt);
    bool is_admin_cmd = g_pika_conf->is_admin_cmd(opt);

    // 如果启用了 RTC 缓存读取功能，并且是单命令且符合拦截规则
    if (g_pika_conf->rtc_cache_read_enabled() && argvs.size() == 1 && IsInterceptedByRTC(opt) &&
        PIKA_CACHE_NONE != g_pika_conf->cache_mode() && !IsInTxn()) {
      // 尝试从缓存中读取命令结果
      if (ReadCmdInCache(argvs[0], opt)) {
        // 如果命中缓存，释放任务参数并返回
        delete arg;
        return;
      }
      // 如果未命中缓存，记录排队前的时间戳
      arg->cache_miss_in_rtc_ = true;
      time_stat_->before_queue_ts_ = pstd::NowMicros();
    }

    // 将任务提交到后台线程池进行调度
    g_pika_server->ScheduleClientPool(&DoBackgroundTask, arg, is_slow_cmd, is_admin_cmd);
    return;
  }

  // 如果是同步模式，直接批量执行 Redis 命令
  BatchExecRedisCmd(argvs, false);
}

void PikaClientConn::DoBackgroundTask(void* arg) {
  std::unique_ptr<BgTaskArg> bg_arg(static_cast<BgTaskArg*>(arg));
  std::shared_ptr<PikaClientConn> conn_ptr = bg_arg->conn_ptr;
  conn_ptr->time_stat_->dequeue_ts_ = pstd::NowMicros();
  if (bg_arg->redis_cmds.empty()) {
    conn_ptr->NotifyEpoll(false);
    return;
  }
  for (const auto& argv : bg_arg->redis_cmds) {
    if (argv.empty()) {
      conn_ptr->NotifyEpoll(false);
      return;
    }
  }

  conn_ptr->BatchExecRedisCmd(bg_arg->redis_cmds, bg_arg->cache_miss_in_rtc_);
}

void PikaClientConn::BatchExecRedisCmd(const std::vector<net::RedisCmdArgsType>& argvs, bool cache_miss_in_rtc) {
  // 设置响应的数量，用于标记需要处理的 Redis 命令总数
  resp_num.store(static_cast<int32_t>(argvs.size()));

  // 遍历所有的 Redis 命令参数
  for (const auto& argv : argvs) {
    // 为每个命令创建一个用于存储响应结果的共享指针
    std::shared_ptr<std::string> resp_ptr = std::make_shared<std::string>();

    // 将响应指针加入响应数组，便于后续统一处理
    resp_array.push_back(resp_ptr);

    // 执行 Redis 命令
    ExecRedisCmd(argv, resp_ptr, cache_miss_in_rtc);
  }

  // 设置处理完成的时间戳，表示命令执行结束的时刻
  time_stat_->process_done_ts_ = pstd::NowMicros();

  // 尝试将响应写回给客户端
  TryWriteResp();
}

bool PikaClientConn::ReadCmdInCache(const net::RedisCmdArgsType& argv, const std::string& opt) {
  resp_num.store(1);
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!c_ptr) {
    return false;
  }
  // Check authed
  if (AuthRequired()) {  // the user is not authed, need to do auth
    if (!(c_ptr->flag() & kCmdFlagsNoAuth)) {
      return false;
    }
  }
  // Initial
  c_ptr->Initial(argv, current_db_);
  // dont store cmd with too large key(only Get/HGet cmd can reach here)
  // the cmd with large key should be non-exist in cache, except for pre-stored
  if (c_ptr->IsTooLargeKey(g_pika_conf->max_key_size_in_cache())) {
    resp_num--;
    return false;
  }
  // acl check
  int8_t subCmdIndex = -1;
  std::string errKey;
  auto checkRes = user_->CheckUserPermission(c_ptr, argv, subCmdIndex, &errKey);
  std::string object;
  if (checkRes == AclDeniedCmd::CMD || checkRes == AclDeniedCmd::KEY || checkRes == AclDeniedCmd::CHANNEL ||
      checkRes == AclDeniedCmd::NO_SUB_CMD || checkRes == AclDeniedCmd::NO_AUTH) {
    // acl check failed
    return false;
  }
  // only read command(Get, HGet) will reach here, no need of record lock
  bool read_status = c_ptr->DoReadCommandInCache();
  auto cmdstat_map = g_pika_cmd_table_manager->GetCommandStatMap();
  resp_num--;
  if (read_status) {
    time_stat_->process_done_ts_ = pstd::NowMicros();
    (*cmdstat_map)[argv[0]].cmd_count.fetch_add(1);
    (*cmdstat_map)[argv[0]].cmd_time_consuming.fetch_add(time_stat_->total_time());
    resp_array.emplace_back(std::make_shared<std::string>(std::move(c_ptr->res().message())));
    TryWriteResp();
  }
  return read_status;
}

void PikaClientConn::TryWriteResp() {
  int expected = 0;
  if (resp_num.compare_exchange_strong(expected, -1)) {
    for (auto& resp : resp_array) {
      WriteResp(*resp);
    }
    if (write_completed_cb_) {
      write_completed_cb_();
      write_completed_cb_ = nullptr;
    }
    resp_array.clear();
    NotifyEpoll(true);
  }
}

void PikaClientConn::PushCmdToQue(std::shared_ptr<Cmd> cmd) { txn_cmd_que_.push(cmd); }

bool PikaClientConn::IsInTxn() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::Start];
}

bool PikaClientConn::IsTxnInitFailed() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::InitCmdFailed];
}

bool PikaClientConn::IsTxnWatchFailed() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::WatchFailed];
}

bool PikaClientConn::IsTxnExecing() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::Execing] && txn_state_[TxnStateBitMask::Start];
}

void PikaClientConn::SetTxnWatchFailState(bool is_failed) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::WatchFailed] = is_failed;
}

void PikaClientConn::SetTxnInitFailState(bool is_failed) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::InitCmdFailed] = is_failed;
}

void PikaClientConn::SetTxnStartState(bool is_start) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::Start] = is_start;
}

void PikaClientConn::ClearTxnCmdQue() { txn_cmd_que_ = std::queue<std::shared_ptr<Cmd>>{}; }

void PikaClientConn::AddKeysToWatch(const std::vector<std::string>& db_keys) {
  for (const auto& it : db_keys) {
    watched_db_keys_.emplace(it);
  }

  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    dispatcher->AddWatchKeys(watched_db_keys_, shared_from_this());
  }
}

void PikaClientConn::RemoveWatchedKeys() {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    watched_db_keys_.clear();
    dispatcher->RemoveWatchKeys(shared_from_this());
  }
}

void PikaClientConn::SetTxnFailedFromKeys(const std::vector<std::string>& db_keys) {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    auto involved_conns = std::vector<std::shared_ptr<NetConn>>{};
    involved_conns = dispatcher->GetInvolvedTxn(db_keys);
    for (auto& conn : involved_conns) {
      if (auto c = std::dynamic_pointer_cast<PikaClientConn>(conn); c != nullptr) {
        c->SetTxnWatchFailState(true);
      }
    }
  }
}

// if key in target_db exists, then the key been watched multi will be failed
void PikaClientConn::SetTxnFailedIfKeyExists(std::string target_db_name) {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher == nullptr) {
    return;
  }
  auto involved_conns = dispatcher->GetAllTxns();
  for (auto& conn : involved_conns) {
    std::shared_ptr<PikaClientConn> c;
    if (c = std::dynamic_pointer_cast<PikaClientConn>(conn); c == nullptr) {
      continue;
    }

    for (const auto& db_key : c->watched_db_keys_) {
      size_t pos = db_key.find('_');
      if (pos == std::string::npos) {
        continue;
      }

      auto db_name = db_key.substr(0, pos);
      auto key = db_key.substr(pos + 1);

      if (target_db_name == "" || target_db_name == "all" || target_db_name == db_name) {
        auto db = g_pika_server->GetDB(db_name);
        // if watched key exists, set watch state to failed
        if (db->storage()->Exists({key}) > 0) {
          c->SetTxnWatchFailState(true);
          break;
        }
      }
    }
  }
}

void PikaClientConn::ExitTxn() {
  if (IsInTxn()) {
    RemoveWatchedKeys();
    ClearTxnCmdQue();
    std::lock_guard<std::mutex> lg(txn_state_mu_);
    txn_state_.reset();
  }
}

void PikaClientConn::ExecRedisCmd(const PikaCmdArgsType& argv, std::shared_ptr<std::string>& resp_ptr,
                                  bool cache_miss_in_rtc) {
  // get opt
  std::string opt = argv[0];
  pstd::StringToLower(opt);
  if (opt == kClusterPrefix) {
    if (argv.size() >= 2) {
      opt += argv[1];
      pstd::StringToLower(opt);
    }
  }

  std::shared_ptr<Cmd> cmd_ptr = DoCmd(argv, opt, resp_ptr, cache_miss_in_rtc);
  *resp_ptr = std::move(cmd_ptr->res().message());
  resp_num--;
}

std::queue<std::shared_ptr<Cmd>> PikaClientConn::GetTxnCmdQue() { return txn_cmd_que_; }

void PikaClientConn::DoAuth(const std::shared_ptr<User>& user) {
  user_ = user;
  authenticated_ = true;
}

void PikaClientConn::UnAuth(const std::shared_ptr<User>& user) {
  user_ = user;
  // If the user does not have a password, and the user is valid, then the user does not need authentication
  authenticated_ = user_->HasFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS)) &&
                   !user_->HasFlags(static_cast<uint32_t>(AclUserFlag::DISABLED));
}

bool PikaClientConn::IsAuthed() const { return authenticated_; }
void PikaClientConn::InitUser() {
  if (!g_pika_conf->GetUserBlackList().empty()) {
    user_ = g_pika_server->Acl()->GetUserLock(Acl::DefaultLimitUser);
  } else {
    user_ = g_pika_server->Acl()->GetUserLock(Acl::DefaultUser);
  }
  authenticated_ = user_->HasFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS)) &&
                   !user_->HasFlags(static_cast<uint32_t>(AclUserFlag::DISABLED));
}
bool PikaClientConn::AuthRequired() const {
  // If the user does not have a password, and the user is valid, then the user does not need authentication
  // Otherwise, you need to determine whether go has been authenticated
  if (IsAuthed()) {
    return false;
  }
  if (user_->HasFlags(static_cast<uint32_t>(AclUserFlag::DISABLED))) {
    return true;
  }
  if (user_->HasFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS))) {
    return false;
  }
  return true;
}
std::string PikaClientConn::UserName() const { return user_->Name(); }

void PikaClientConn::ClientInfoToString(std::string* info, const std::string& cmdName) {
  uint64_t age = pstd::NowMicros() - last_interaction().tv_usec;

  std::string flags;
  g_pika_server->ClientIsMonitor(std::dynamic_pointer_cast<PikaClientConn>(shared_from_this())) ? flags.append("O")
                                                                                                : flags.append("S");
  if (IsPubSub()) {
    flags.append("P");
  }

  info->append(fmt::format(
      "id={} addr={} name={} age={} idle={} flags={} db={} sub={} psub={} multi={} "
      "cmd={} user={} resp=2",
      fd(), ip_port(), name(), age, age / 1000000, flags, GetCurrentTable(),
      IsPubSub() ? g_pika_server->ClientPubSubChannelSize(shared_from_this()) : 0,
      IsPubSub() ? g_pika_server->ClientPubSubChannelPatternSize(shared_from_this()) : 0, -1, cmdName, user_->Name()));
}

// compare addr in ClientInfo
bool AddrCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return rhs.ip_port < lhs.ip_port; }

bool IdleCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return lhs.last_interaction < rhs.last_interaction; }
