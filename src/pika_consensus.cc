// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <utility>

#include "include/pika_consensus.h"

#include "include/pika_client_conn.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaConf> g_pika_conf;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;
/* Context */

// 构造函数，初始化上下文对象并设置文件路径
Context::Context(std::string path) : path_(std::move(path)) {}

Status Context::StableSave() {
  char* p = save_->GetData();

  // 将 applied_index_ 中的字段逐一写入文件
  memcpy(p, &(applied_index_.b_offset.filenum), sizeof(uint32_t));  // 保存文件号
  p += 4;
  memcpy(p, &(applied_index_.b_offset.offset), sizeof(uint64_t));  // 保存偏移量
  p += 8;
  memcpy(p, &(applied_index_.l_offset.term), sizeof(uint32_t));  // 保存日志任期
  p += 4;
  memcpy(p, &(applied_index_.l_offset.index), sizeof(uint64_t));  // 保存日志索引

  return Status::OK();
}

Status Context::Init() {
  if (!pstd::FileExists(path_)) {
    // 如果文件不存在，则创建文件并进行初始化保存
    Status s = pstd::NewRWFile(path_, save_);
    if (!s.ok()) {
      LOG(FATAL) << "Context new file failed " << s.ToString();
    }
    StableSave();
  } else {
    // 如果文件存在，则加载该文件
    std::unique_ptr<pstd::RWFile> tmp_file;
    Status s = pstd::NewRWFile(path_, tmp_file);
    save_.reset(tmp_file.release());
    if (!s.ok()) {
      LOG(FATAL) << "Context new file failed " << s.ToString();
    }
  }

  // 从文件中读取数据并加载到应用索引
  if (save_->GetData()) {
    memcpy(reinterpret_cast<char*>(&(applied_index_.b_offset.filenum)), save_->GetData(), sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(applied_index_.b_offset.offset)), save_->GetData() + 4, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&(applied_index_.l_offset.term)), save_->GetData() + 12, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(applied_index_.l_offset.index)), save_->GetData() + 16, sizeof(uint64_t));
    return Status::OK();
  } else {
    return Status::Corruption("Context init error");
  }
}

// 更新已应用的日志索引
void Context::UpdateAppliedIndex(const LogOffset& offset) {
  std::lock_guard l(rwlock_);  // 使用互斥锁保护共享资源

  LogOffset cur_offset;
  // 更新应用窗口，计算新的已应用偏移量
  applied_win_.Update(SyncWinItem(offset), SyncWinItem(offset), &cur_offset);

  // 如果新的偏移量大于当前的已应用索引，则更新已应用索引并保存
  if (cur_offset > applied_index_) {
    applied_index_ = cur_offset;
    StableSave();
  }
}

// 重置上下文的应用索引
void Context::Reset(const LogOffset& offset) {
  std::lock_guard l(rwlock_);

  applied_index_ = offset;  // 重置已应用的索引
  applied_win_.Reset();     // 重置应用窗口
  StableSave();             // 保存重置后的状态
}

/* SyncProgress */

std::string MakeSlaveKey(const std::string& ip, int port) { return ip + ":" + std::to_string(port); }

std::shared_ptr<SlaveNode> SyncProgress::GetSlaveNode(const std::string& ip, int port) {
  std::string slave_key = MakeSlaveKey(ip, port);  // 生成从节点标识
  std::shared_lock l(rwlock_);                     // 共享锁，允许多个线程读取
  if (slaves_.find(slave_key) == slaves_.end()) {  // 如果从节点不存在
    return nullptr;
  }
  return slaves_[slave_key];
}

// 获取所有的从节点
std::unordered_map<std::string, std::shared_ptr<SlaveNode>> SyncProgress::GetAllSlaveNodes() {
  std::shared_lock l(rwlock_);
  return slaves_;
}

// 添加一个新的从节点，如果从节点已经存在，则更新其 session_id
Status SyncProgress::AddSlaveNode(const std::string& ip, int port, const std::string& db_name, int session_id) {
  std::string slave_key = MakeSlaveKey(ip, port);
  std::shared_ptr<SlaveNode> exist_ptr = GetSlaveNode(ip, port);  // 检查从节点是否已经存在
  if (exist_ptr) {                                                // 如果从节点已存在
    LOG(WARNING) << "SlaveNode " << exist_ptr->ToString() << " already exist, set new session " << session_id;
    exist_ptr->SetSessionId(session_id);  // 更新 session_id
    return Status::OK();
  }

  // 如果从节点不存在，创建一个新的从节点
  std::shared_ptr<SlaveNode> slave_ptr = std::make_shared<SlaveNode>(ip, port, db_name, session_id);
  slave_ptr->SetLastSendTime(pstd::NowMicros());  // 设置最后发送时间
  slave_ptr->SetLastRecvTime(pstd::NowMicros());  // 设置最后接收时间

  {
    std::lock_guard l(rwlock_);
    slaves_[slave_key] = slave_ptr;         // 将新的从节点添加到 slaves_ 中
    match_index_[slave_key] = LogOffset();  // 在 match_index_ 中为该从节点初始化日志偏移
  }

  return Status::OK();
}

Status SyncProgress::RemoveSlaveNode(const std::string& ip, int port) {
  std::string slave_key = MakeSlaveKey(ip, port);  // 生成从节点标识
  {
    std::lock_guard l(rwlock_);
    slaves_.erase(slave_key);       // 从 slaves_ 中移除该从节点
    match_index_.erase(slave_key);  // 从 match_index_ 中移除该从节点
  }
  return Status::OK();
}

// 更新指定从节点的日志偏移信息
Status SyncProgress::Update(const std::string& ip, int port, const LogOffset& start, const LogOffset& end,
                            LogOffset* committed_index) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {  // 如果从节点不存在
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  LogOffset acked_offset;
  {
    std::lock_guard l(slave_ptr->slave_mu);
    Status s = slave_ptr->Update(start, end, &acked_offset);  // 更新从节点的日志偏移
    if (!s.ok()) {
      return s;
    }
    // 更新该从节点在 match_index_ 中的日志偏移
    match_index_[ip + std::to_string(port)] = acked_offset;
  }
  return Status::OK();
}

int SyncProgress::SlaveSize() {
  std::shared_lock l(rwlock_);
  return static_cast<int32_t>(slaves_.size());
}
/* MemLog */

MemLog::MemLog() = default;

int MemLog::Size() { return static_cast<int>(logs_.size()); }

Status MemLog::TruncateTo(const LogOffset& offset) {
  std::lock_guard l_logs(logs_mu_);
  int index = InternalFindLogByBinlogOffset(offset);  // 查找指定 offset 对应的日志位置
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  last_offset_ = logs_[index].offset;                   // 更新最后的偏移
  logs_.erase(logs_.begin() + index + 1, logs_.end());  // 删除从 index+1 到末尾的日志
  return Status::OK();                                  // 返回成功状态
}

// 重置日志，清空所有日志并设置最后的偏移
void MemLog::Reset(const LogOffset& offset) {
  std::lock_guard l_logs(logs_mu_);         // 使用锁保证对日志的独占访问
  logs_.erase(logs_.begin(), logs_.end());  // 清空所有日志
  last_offset_ = offset;                    // 设置最后的偏移
}

// 查找指定偏移的日志项
bool MemLog::FindLogItem(const LogOffset& offset, LogOffset* found_offset) {
  std::lock_guard l_logs(logs_mu_);                 // 使用锁保证对日志的独占访问
  int index = InternalFindLogByLogicIndex(offset);  // 查找指定偏移对应的日志
  if (index < 0) {
    return false;  // 没有找到对应日志，返回 false
  }
  *found_offset = logs_[index].offset;  // 将找到的日志偏移赋值给输出参数
  return true;                          // 找到日志，返回 true
}

// 根据逻辑索引查找日志
int MemLog::InternalFindLogByLogicIndex(const LogOffset& offset) {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset.l_offset.index > offset.l_offset.index) {  // 如果日志的逻辑索引大于目标偏移，返回 -1
      return -1;
    }
    if (logs_[i].offset.l_offset.index == offset.l_offset.index) {  // 如果找到了对应的逻辑索引，返回该日志的位置
      return static_cast<int32_t>(i);
    }
  }
  return -1;  // 没有找到对应的日志，返回 -1
}

// 根据 Binlog 偏移查找日志
int MemLog::InternalFindLogByBinlogOffset(const LogOffset& offset) {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset > offset) {  // 如果日志的偏移大于目标偏移，返回 -1
      return -1;
    }
    if (logs_[i].offset == offset) {  // 如果找到了对应的 Binlog 偏移，返回该日志的位置
      return static_cast<int32_t>(i);
    }
  }
  return -1;
}

/* ConsensusCoordinator */

// 构造函数，初始化数据库名称，并创建相关的上下文、稳定日志和内存日志对象
ConsensusCoordinator::ConsensusCoordinator(const std::string& db_name) : db_name_(db_name) {
  std::string db_log_path = g_pika_conf->log_path() + "log_" + db_name + "/";  // 构建数据库日志路径
  std::string log_path = db_log_path;                                          // 日志路径
  context_ = std::make_shared<Context>(log_path + kContext);  // 创建上下文对象，负责存储和恢复日志
  stable_logger_ = std::make_shared<StableLog>(db_name, log_path);  // 创建稳定日志对象
  mem_logger_ = std::make_shared<MemLog>();                         // 创建内存日志对象
}

ConsensusCoordinator::~ConsensusCoordinator() = default;

void ConsensusCoordinator::Init() {
  // 加载已提交的索引和应用的索引
  context_->Init();                             // 初始化上下文，恢复保存的索引
  committed_index_ = context_->applied_index_;  // 从上下文获取已提交的索引

  // 加载当前任期（term）
  term_ = stable_logger_->Logger()->term();  // 获取稳定日志中的当前任期

  LOG(INFO) << DBInfo(db_name_).ToString() << "Restore applied index " << context_->applied_index_.ToString()
            << " current term " << term_;

  // 如果已提交的索引为空，则直接返回
  if (committed_index_ == LogOffset()) {
    return;
  }

  // 加载内存日志的最后偏移
  mem_logger_->SetLastOffset(committed_index_);

  // 初始化 Redis 解析器设置
  net::RedisParserSettings settings;
  settings.DealMessage = &(ConsensusCoordinator::InitCmd);  // 设置处理命令的回调函数

  // 创建 Redis 解析器实例并初始化
  net::RedisParser redis_parser;
  redis_parser.RedisParserInit(REDIS_PARSER_REQUEST, settings);

  // 创建 Binlog 读取器并设置开始读取的位置
  PikaBinlogReader binlog_reader;
  int res =
      binlog_reader.Seek(stable_logger_->Logger(), committed_index_.b_offset.filenum, committed_index_.b_offset.offset);
  if (res != 0) {
    // 如果初始化失败，输出错误日志并终止程序
    LOG(FATAL) << DBInfo(db_name_).ToString() << "Binlog reader init failed";
  }

  // 循环读取 Binlog 文件中的日志项
  while (true) {
    LogOffset offset;    // 存储日志的偏移量
    std::string binlog;  // 存储当前 Binlog 条目

    // 从 Binlog 读取器中获取一条日志
    Status s = binlog_reader.Get(&binlog, &(offset.b_offset.filenum), &(offset.b_offset.offset));

    // 如果已经读取到文件末尾，跳出循环
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      // 如果读取出现错误，输出日志并终止程序
      LOG(FATAL) << DBInfo(db_name_).ToString() << "Read Binlog error";
    }

    BinlogItem item;
    // 解码 Binlog 条目，获取其相关信息
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      LOG(FATAL) << DBInfo(db_name_).ToString() << "Binlog item decode failed";
    }

    // 设置日志项的逻辑偏移
    offset.l_offset.term = item.term_id();    // 设置日志项的 term
    offset.l_offset.index = item.logic_id();  // 设置日志项的逻辑索引

    // 配置 Redis 解析器
    redis_parser.data = static_cast<void*>(&db_name_);  // 设置数据库名称为 Redis 解析器的参数
    const char* redis_parser_start = binlog.data() + BINLOG_ENCODE_LEN;          // 获取 Binlog 内容的起始位置
    int redis_parser_len = static_cast<int>(binlog.size()) - BINLOG_ENCODE_LEN;  // 计算 Binlog 内容的长度
    int processed_len = 0;

    // 解析 Binlog 内容
    net::RedisParserStatus ret = redis_parser.ProcessInputBuffer(redis_parser_start, redis_parser_len, &processed_len);

    // 如果解析失败，输出错误日志并终止程序
    if (ret != net::kRedisParserDone) {
      LOG(FATAL) << DBInfo(db_name_).ToString() << "Redis parser parse failed";
      return;
    }

    // 获取解析结果中的命令
    auto arg = static_cast<CmdPtrArg*>(redis_parser.data);
    std::shared_ptr<Cmd> cmd_ptr = arg->cmd_ptr;  // 获取解析后的命令
    delete arg;                                   // 释放命令参数
    redis_parser.data = nullptr;                  // 清空 Redis 解析器的数据

    // 将解析后的命令追加到内存日志中
    mem_logger_->AppendLog(MemLog::LogItem(offset, cmd_ptr, nullptr, nullptr));
  }
}
// 重置共识协调器的状态
Status ConsensusCoordinator::Reset(const LogOffset& offset) {
  // 重置上下文中的状态
  context_->Reset(offset);

  // 使用锁更新已提交的索引
  {
    std::lock_guard l(index_mu_);
    committed_index_ = offset;  // 将已提交的索引设置为传入的 offset
  }

  // 更新当前任期
  UpdateTerm(offset.l_offset.term);

  // 在稳定日志中设置生产者状态
  Status s = stable_logger_->Logger()->SetProducerStatus(offset.b_offset.filenum, offset.b_offset.offset,
                                                         offset.l_offset.term, offset.l_offset.index);
  if (!s.ok()) {
    // 如果设置失败，记录警告日志并返回错误
    LOG(WARNING) << DBInfo(db_name_).ToString() << "Consensus reset status failed " << s.ToString();
    return s;
  }

  // 设置稳定日志的初始偏移
  stable_logger_->SetFirstOffset(offset);

  // 锁住稳定日志并重置内存日志
  stable_logger_->Logger()->Lock();
  mem_logger_->Reset(offset);
  stable_logger_->Logger()->Unlock();

  return Status::OK();
}

// 提交日志命令到共识协调器
Status ConsensusCoordinator::ProposeLog(const std::shared_ptr<Cmd>& cmd_ptr) {
  std::vector<std::string> keys = cmd_ptr->current_key();

  // 如果是 SAdd 命令且键以特定前缀开头，不添加到 binlog
  if (cmd_ptr->name() == kCmdNameSAdd && !keys.empty() &&
      (keys[0].compare(0, SlotKeyPrefix.length(), SlotKeyPrefix) == 0 ||
       keys[0].compare(0, SlotTagPrefix.length(), SlotTagPrefix) == 0)) {
    return Status::OK();
  }

  // 确保稳定日志和内存日志的一致性
  Status s = InternalAppendLog(cmd_ptr);
  if (!s.ok()) {
    return s;
  }

  // 发出信号通知辅助线程
  g_pika_server->SignalAuxiliary();
  return Status::OK();
}

// 内部追加日志到 binlog
Status ConsensusCoordinator::InternalAppendLog(const std::shared_ptr<Cmd>& cmd_ptr) {
  return InternalAppendBinlog(cmd_ptr);  // 调用内部方法追加到 binlog
}

// 处理领导者的日志
// 预检查前一个偏移是否匹配，如果日志已存在则丢弃该日志
Status ConsensusCoordinator::ProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute) {
  // 获取当前内存日志中的最后偏移
  LogOffset last_index = mem_logger_->last_offset();

  // 如果日志的逻辑 ID 小于当前内存日志中的最后索引，表示该日志已经存在，丢弃此日志
  if (attribute.logic_id() < last_index.l_offset.index) {
    LOG(WARNING) << DBInfo(db_name_).ToString() << "Drop log from leader logic_id " << attribute.logic_id()
                 << " cur last index " << last_index.l_offset.index;
    return Status::OK();
  }

  // 获取命令参数中的第一个键
  auto opt = cmd_ptr->argv()[0];

  // 如果是普通命令，异步应用 binlog 和同步应用数据库
  if (pstd::StringToLower(opt) != kCmdNameFlushdb) {
    // 应用 binlog 同步执行
    Status s = InternalAppendLog(cmd_ptr);
    // 在同步方式下应用数据库
    InternalApplyFollower(cmd_ptr);
  } else {
    // 如果是 flushdb 命令，需要同步应用 binlog 和数据库
    // 在执行 flushdb 之前，确保所有之前提交的写数据库任务都已经完成
    int32_t wait_ms = 250;
    while (g_pika_rm->GetUnfinishedAsyncWriteDBTaskCount(db_name_) > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));  // 等待一定时间
      wait_ms *= 2;                                                     // 每次等待时间加倍
      wait_ms = wait_ms < 3000 ? wait_ms : 3000;                        // 最大等待时间为 3000 毫秒
    }
    // 同步应用 flushdb binlog
    Status s = InternalAppendLog(cmd_ptr);
    // 同步应用数据库
    PikaReplBgWorker::WriteDBInSyncWay(cmd_ptr);
  }

  return Status::OK();
}
// 更新从节点的同步进度
Status ConsensusCoordinator::UpdateSlave(const std::string& ip, int port, const LogOffset& start,
                                         const LogOffset& end) {
  LogOffset committed_index;

  // 更新同步进度
  Status s = sync_pros_.Update(ip, port, start, end, &committed_index);
  if (!s.ok()) {
    return s;  // 如果更新失败，返回错误状态
  }

  return Status::OK();  // 成功更新，返回 OK
}

// 将命令追加到 binlog
Status ConsensusCoordinator::InternalAppendBinlog(const std::shared_ptr<Cmd>& cmd_ptr) {
  // 将命令转换为 Redis 协议格式
  std::string content = cmd_ptr->ToRedisProtocol();

  // 将命令写入稳定日志
  Status s = stable_logger_->Logger()->Put(content);
  if (!s.ok()) {
    // 如果写入失败，标记数据库的 binlog IO 错误
    std::string db_name = cmd_ptr->db_name().empty() ? g_pika_conf->default_db() : cmd_ptr->db_name();
    std::shared_ptr<DB> db = g_pika_server->GetDB(db_name);
    if (db) {
      db->SetBinlogIoError();  // 设置 binlog IO 错误
    }
    return s;  // 返回写入错误状态
  }

  // 检查日志是否成功打开
  return stable_logger_->Logger()->IsOpened();
}

// 添加一个从节点
Status ConsensusCoordinator::AddSlaveNode(const std::string& ip, int port, int session_id) {
  // 调用同步进度管理器添加从节点
  Status s = sync_pros_.AddSlaveNode(ip, port, db_name_, session_id);
  if (!s.ok()) {
    return s;  // 如果添加失败，返回错误状态
  }
  return Status::OK();  // 成功添加，返回 OK
}

// 移除一个从节点
Status ConsensusCoordinator::RemoveSlaveNode(const std::string& ip, int port) {
  // 调用同步进度管理器移除从节点
  Status s = sync_pros_.RemoveSlaveNode(ip, port);
  if (!s.ok()) {
    return s;  // 如果移除失败，返回错误状态
  }
  return Status::OK();  // 成功移除，返回 OK
}

// 更新当前的任期
void ConsensusCoordinator::UpdateTerm(uint32_t term) {
  stable_logger_->Logger()->Lock();  // 锁住稳定日志，确保线程安全

  // 使用读写锁保护 term_ 的更新
  std::lock_guard l(term_rwlock_);
  term_ = term;                             // 更新任期
  stable_logger_->Logger()->SetTerm(term);  // 在稳定日志中设置新任期

  stable_logger_->Logger()->Unlock();  // 解锁
}

// 获取当前的任期
uint32_t ConsensusCoordinator::term() {
  std::shared_lock l(term_rwlock_);  // 使用共享锁读取当前的任期
  return term_;                      // 返回当前的任期
}

// 内部应用跟随者命令，异步写数据库
void ConsensusCoordinator::InternalApplyFollower(const std::shared_ptr<Cmd>& cmd_ptr) {
  g_pika_rm->ScheduleWriteDBTask(cmd_ptr, db_name_);  // 调用任务调度器异步写数据库
}

// 初始化命令解析器
int ConsensusCoordinator::InitCmd(net::RedisParser* parser, const net::RedisCmdArgsType& argv) {
  auto db_name = static_cast<std::string*>(parser->data);  // 获取数据库名称
  std::string opt = argv[0];                               // 获取命令选项

  // 从命令表中获取命令对象
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(pstd::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Command " << opt << " not in the command table";  // 如果命令不存在于命令表，记录警告
    return -1;                                                         // 返回错误
  }

  // 初始化命令对象
  c_ptr->Initial(argv, *db_name);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;  // 如果初始化失败，记录警告
    return -1;                                                       // 返回错误
  }

  // 将命令对象封装在 CmdPtrArg 中，保存到 parser 的 data 中
  parser->data = static_cast<void*>(new CmdPtrArg(c_ptr));
  return 0;  // 初始化成功，返回 0
}
// 截断日志到指定的 LogOffset
Status ConsensusCoordinator::TruncateTo(const LogOffset& offset) {
  LOG(INFO) << DBInfo(db_name_).ToString() << "Truncate to " << offset.ToString();

  // 查找指定位置的逻辑偏移量
  LogOffset founded_offset;
  Status s = FindLogicOffset(offset.b_offset, offset.l_offset.index, &founded_offset);
  if (!s.ok()) {
    return s;  // 如果查找失败，返回错误状态
  }

  LOG(INFO) << DBInfo(db_name_).ToString() << " Founded truncate pos " << founded_offset.ToString();

  // 获取当前已提交的日志索引
  LogOffset committed = committed_index();

  // 锁住稳定日志，防止并发修改
  stable_logger_->Logger()->Lock();

  // 如果找到的截断位置与已提交的索引一致，重置内存日志
  if (founded_offset.l_offset.index == committed.l_offset.index) {
    mem_logger_->Reset(committed);
  } else {
    // 否则，执行内存日志的截断操作
    Status s = mem_logger_->TruncateTo(founded_offset);
    if (!s.ok()) {
      stable_logger_->Logger()->Unlock();
      return s;  // 如果内存日志截断失败，解锁并返回错误
    }
  }

  // 执行稳定日志的截断操作
  s = stable_logger_->TruncateTo(founded_offset);
  if (!s.ok()) {
    stable_logger_->Logger()->Unlock();
    return s;  // 如果稳定日志截断失败，解锁并返回错误
  }

  stable_logger_->Logger()->Unlock();  // 解锁稳定日志
  return Status::OK();                 // 截断成功，返回 OK
}

// 获取指定起始偏移量的 Binlog 偏移量
Status ConsensusCoordinator::GetBinlogOffset(const BinlogOffset& start_offset, LogOffset* log_offset) {
  PikaBinlogReader binlog_reader;

  // 初始化 binlog 阅读器，定位到起始偏移量
  int res = binlog_reader.Seek(stable_logger_->Logger(), start_offset.filenum, start_offset.offset);
  if (res != 0) {
    return Status::Corruption("Binlog reader init failed");  // 如果初始化失败，返回损坏错误
  }

  // 读取一个 binlog 项
  std::string binlog;
  BinlogOffset offset;
  Status s = binlog_reader.Get(&binlog, &(offset.filenum), &(offset.offset));
  if (!s.ok()) {
    return Status::Corruption("Binlog reader get failed");  // 如果读取失败，返回损坏错误
  }

  // 解码 binlog 项
  BinlogItem item;
  if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
    return Status::Corruption("Binlog item decode failed");  // 如果解码失败，返回损坏错误
  }

  // 设置 log_offset
  log_offset->b_offset = offset;
  log_offset->l_offset.term = item.term_id();
  log_offset->l_offset.index = item.logic_id();

  return Status::OK();  // 返回成功
}

// 获取指定范围内的 binlog 偏移量
// 如果 start_offset 为 0,0 且 end_offset 为 1,129，返回结果会包含 binlog (1,129)
// 如果 start_offset 为 0,0 且 end_offset 为 1,0，返回结果不会包含 binlog (1,xxx)
// 如果 start_offset 为 0,0 且 end_offset 为 0,0，结果不会包含 binlog (0,xxx)
Status ConsensusCoordinator::GetBinlogOffset(const BinlogOffset& start_offset, const BinlogOffset& end_offset,
                                             std::vector<LogOffset>* log_offset) {
  PikaBinlogReader binlog_reader;

  // 初始化 binlog 阅读器，定位到起始偏移量
  int res = binlog_reader.Seek(stable_logger_->Logger(), start_offset.filenum, start_offset.offset);
  if (res != 0) {
    return Status::Corruption("Binlog reader init failed");  // 如果初始化失败，返回损坏错误
  }

  // 循环读取 binlog 项，直到读取到 end_offset 或文件结束
  while (true) {
    BinlogOffset b_offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(b_offset.filenum), &(b_offset.offset));
    if (s.IsEndFile()) {
      return Status::OK();  // 如果到达文件末尾，返回成功
    } else if (s.IsCorruption() || s.IsIOError()) {
      return Status::Corruption("Read Binlog error");  // 如果读取发生错误，返回损坏错误
    }

    // 解码 binlog 项
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog, &item)) {
      return Status::Corruption("Binlog item decode failed");  // 如果解码失败，返回损坏错误
    }

    // 构建 LogOffset 对象
    LogOffset offset;
    offset.b_offset = b_offset;
    offset.l_offset.term = item.term_id();
    offset.l_offset.index = item.logic_id();

    // 如果当前偏移量大于 end_offset，结束读取
    if (offset.b_offset > end_offset) {
      return Status::OK();
    }

    // 将当前的 LogOffset 添加到结果中
    log_offset->push_back(offset);
  }

  return Status::OK();  // 成功获取 binlog 偏移量，返回成功
}
// 在指定的 binlogs 中查找目标索引所在的 binlog 文件号
Status ConsensusCoordinator::FindBinlogFileNum(const std::map<uint32_t, std::string>& binlogs, uint64_t target_index,
                                               uint32_t start_filenum, uint32_t* founded_filenum) {
  // 获取 binlogs 的低边界和高边界文件号
  uint32_t lb_binlogs = binlogs.begin()->first;   // binlogs 中最小的文件号
  uint32_t hb_binlogs = binlogs.rbegin()->first;  // binlogs 中最大的文件号

  bool first_time_left = false;      // 是否第一次向左移动
  bool first_time_right = false;     // 是否第一次向右移动
  uint32_t filenum = start_filenum;  // 从指定的起始文件号开始查找

  while (true) {
    // 获取当前文件号的第一个偏移量
    LogOffset first_offset;
    Status s = GetBinlogOffset(BinlogOffset(filenum, 0), &first_offset);
    if (!s.ok()) {
      return s;  // 如果获取偏移量失败，返回错误
    }

    // 如果目标索引小于当前文件的第一个索引，说明需要向左移动
    if (target_index < first_offset.l_offset.index) {
      if (first_time_right) {
        // 如果已经向右移动过，再向左移动就是找到最后一个文件
        filenum = filenum - 1;
        break;
      }
      // 向左移动
      first_time_left = true;
      if (filenum == 0 || filenum - 1 < lb_binlogs) {
        // 如果已经到达左边界，则返回未找到的错误
        return Status::NotFound(std::to_string(target_index) + " hit low boundary");
      }
      filenum = filenum - 1;  // 减小文件号，继续向左查找
    }
    // 如果目标索引大于当前文件的第一个索引，说明需要向右移动
    else if (target_index > first_offset.l_offset.index) {
      if (first_time_left) {
        break;  // 如果已经向左移动过，再向右移动则结束查找
      }
      // 向右移动
      first_time_right = true;
      if (filenum + 1 > hb_binlogs) {
        // 如果已经到达右边界，则结束查找
        break;
      }
      filenum = filenum + 1;  // 增加文件号，继续向右查找
    }
    // 如果目标索引与当前文件的第一个索引匹配，则查找成功
    else {
      break;
    }
  }

  // 返回找到的文件号
  *founded_filenum = filenum;
  return Status::OK();  // 返回成功状态
}

// 根据提供的提示偏移量查找目标逻辑索引对应的 LogOffset
Status ConsensusCoordinator::FindLogicOffsetBySearchingBinlog(const BinlogOffset& hint_offset, uint64_t target_index,
                                                              LogOffset* found_offset) {
  LOG(INFO) << DBInfo(db_name_).ToString() << "FindLogicOffsetBySearchingBinlog hint offset " << hint_offset.ToString()
            << " target_index " << target_index;

  // 获取所有 binlog 文件信息
  BinlogOffset start_offset;
  std::map<uint32_t, std::string> binlogs;
  if (!stable_logger_->GetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");  // 获取 binlog 文件失败，返回损坏错误
  }

  // 如果没有找到 binlog 文件，则返回未找到的错误
  if (binlogs.empty()) {
    return Status::NotFound("Binlogs is empty");
  }

  // 如果提示的文件号不在 binlog 中，设置起始查找文件为最新文件的第一个偏移量
  if (binlogs.find(hint_offset.filenum) == binlogs.end()) {
    start_offset = BinlogOffset(binlogs.crbegin()->first, 0);  // 设置为最新文件的偏移量
  } else {
    start_offset = hint_offset;  // 否则，从提示偏移量开始查找
  }

  uint32_t found_filenum;
  // 查找目标索引所在的 binlog 文件号
  Status s = FindBinlogFileNum(binlogs, target_index, start_offset.filenum, &found_filenum);
  if (!s.ok()) {
    return s;  // 如果查找失败，返回错误
  }

  LOG(INFO) << DBInfo(db_name_).ToString() << "FindBinlogFilenum res "  // NOLINT
            << found_filenum;

  // 根据找到的文件号，设置开始和结束的 binlog 偏移量
  BinlogOffset traversal_start(found_filenum, 0);
  BinlogOffset traversal_end(found_filenum + 1, 0);

  std::vector<LogOffset> offsets;
  // 获取指定范围内的 binlog 偏移量
  s = GetBinlogOffset(traversal_start, traversal_end, &offsets);
  if (!s.ok()) {
    return s;  // 如果获取偏移量失败，返回错误
  }

  // 遍历所有 binlog 偏移量，查找目标逻辑索引
  for (auto& offset : offsets) {
    if (offset.l_offset.index == target_index) {
      LOG(INFO) << DBInfo(db_name_).ToString() << "Founded " << target_index << " " << offset.ToString();
      *found_offset = offset;  // 找到匹配的索引，返回该偏移量
      return Status::OK();     // 返回成功状态
    }
  }

  // 如果未找到目标逻辑索引，返回未找到的错误
  return Status::NotFound("Logic index not found");
}

// 查找逻辑偏移量
Status ConsensusCoordinator::FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index,
                                             LogOffset* found_offset) {
  LogOffset possible_offset;
  // 获取起始偏移量的binlog偏移
  Status s = GetBinlogOffset(start_offset, &possible_offset);

  // 如果获取binlog偏移失败或偏移量的index不等于目标index，则需要通过其他方式查找
  if (!s.ok() || possible_offset.l_offset.index != target_index) {
    if (!s.ok()) {
      LOG(INFO) << DBInfo(db_name_).ToString() << "GetBinlogOffset res: " << s.ToString();
    } else {
      LOG(INFO) << DBInfo(db_name_).ToString() << "GetBInlogOffset res: " << s.ToString() << " possible_offset "
                << possible_offset.ToString() << " target_index " << target_index;
    }
    // 通过搜索binlog文件来查找逻辑偏移量
    return FindLogicOffsetBySearchingBinlog(start_offset, target_index, found_offset);
  }
  // 如果找到了匹配的偏移量，则返回
  *found_offset = possible_offset;
  return Status::OK();
}

// 获取指定偏移量之前的日志
Status ConsensusCoordinator::GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints) {
  BinlogOffset traversal_end = start_offset;
  BinlogOffset traversal_start(traversal_end.filenum, 0);
  // 设置遍历的开始文件号，避免越界
  traversal_start.filenum = traversal_start.filenum == 0 ? 0 : traversal_start.filenum - 1;
  std::map<uint32_t, std::string> binlogs;
  // 获取binlog文件列表
  if (!stable_logger_->GetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  // 如果指定的文件号没有找到，调整开始文件号为结束文件号
  if (binlogs.find(traversal_start.filenum) == binlogs.end()) {
    traversal_start.filenum = traversal_end.filenum;
  }
  std::vector<LogOffset> res;
  // 获取该范围内的binlog偏移
  Status s = GetBinlogOffset(traversal_start, traversal_end, &res);
  if (!s.ok()) {
    return s;
  }
  // 如果日志数大于100，则取最后100条
  if (res.size() > 100) {
    res.assign(res.end() - 100, res.end());
  }
  // 将结果赋值给hints
  *hints = res;
  return Status::OK();
}

// 领导者与追随者的协商
Status ConsensusCoordinator::LeaderNegotiate(const LogOffset& f_last_offset, bool* reject,
                                             std::vector<LogOffset>* hints) {
  uint64_t f_index = f_last_offset.l_offset.index;
  LOG(INFO) << DBInfo(db_name_).ToString() << "LeaderNeotiate follower last offset " << f_last_offset.ToString()
            << " first_offsert " << stable_logger_->first_offset().ToString() << " last_offset "
            << mem_logger_->last_offset().ToString();

  *reject = true;

  // 如果追随者的index大于内存中的最后一个日志偏移，则获取追随者之前的日志
  if (f_index > mem_logger_->last_offset().l_offset.index) {
    // 获取内存日志前100条
    Status s = GetLogsBefore(mem_logger_->last_offset().b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << f_index << " is larger than last index " << mem_logger_->last_offset().ToString()
                   << " get logs before last index failed " << s.ToString();
      return s;
    }
    LOG(INFO) << DBInfo(db_name_).ToString() << "follower index larger then last_offset index, get logs before "
              << mem_logger_->last_offset().ToString();
    return Status::OK();
  }

  // 如果追随者的index小于稳定日志的第一个日志偏移，则需要完全同步
  if (f_index < stable_logger_->first_offset().l_offset.index) {
    LOG(INFO) << DBInfo(db_name_).ToString() << f_index << " not found current first index"
              << stable_logger_->first_offset().ToString();
    return Status::NotFound("logic index");
  }

  // 如果偏移量为0，表示无需拒绝，直接同步
  if (f_last_offset.l_offset.index == 0) {
    *reject = false;
    return Status::OK();
  }

  LogOffset found_offset;
  // 查找逻辑偏移量
  Status s = FindLogicOffset(f_last_offset.b_offset, f_index, &found_offset);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << DBInfo(db_name_).ToString() << f_last_offset.ToString() << " not found " << s.ToString();
      return s;
    } else {
      LOG(WARNING) << DBInfo(db_name_).ToString() << "find logic offset failed" << s.ToString();
      return s;
    }
  }

  // 如果找到的偏移量的term不同或者偏移量的起始位置不同，则重新获取日志
  if (found_offset.l_offset.term != f_last_offset.l_offset.term || !(f_last_offset.b_offset == found_offset.b_offset)) {
    Status s = GetLogsBefore(found_offset.b_offset, hints);
    if (!s.ok()) {
      LOG(WARNING) << DBInfo(db_name_).ToString() << "Try to get logs before " << found_offset.ToString() << " failed";
      return s;
    }
    return Status::OK();
  }

  // 找到了相等的偏移量
  LOG(INFO) << DBInfo(db_name_).ToString() << "Found equal offset " << found_offset.ToString();
  *reject = false;
  return Status::OK();
}
// memlog order: committed_index , [committed_index + 1, memlogger.end()]
// 根据 follower 提供的提示（hints），与当前的 committed_index 进行协商，更新日志的偏移量并返回
Status ConsensusCoordinator::FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset) {
  // 如果提示为空，则返回腐败错误
  if (hints.empty()) {
    return Status::Corruption("hints empty");
  }

  // 打印日志，记录协商范围
  LOG(INFO) << DBInfo(db_name_).ToString() << "FollowerNegotiate from " << hints[0].ToString() << " to "
            << hints[hints.size() - 1].ToString();

  // 如果当前内存日志的最后偏移量小于提示中的第一个偏移量，则直接返回最后偏移量
  if (mem_logger_->last_offset().l_offset.index < hints[0].l_offset.index) {
    *reply_offset = mem_logger_->last_offset();  // 返回内存日志的最后偏移量
    return Status::OK();                         // 成功
  }

  // 如果 committed_index 大于提示中的最后偏移量，说明提示索引无效，返回腐败错误
  if (committed_index().l_offset.index > hints[hints.size() - 1].l_offset.index) {
    return Status::Corruption("invalid hints all smaller than committed_index");
  }

  // 如果当前内存日志的最后偏移量大于提示中的最后偏移量，则截断内存日志到提示的最后偏移量
  if (mem_logger_->last_offset().l_offset.index > hints[hints.size() - 1].l_offset.index) {
    const auto& truncate_offset = hints[hints.size() - 1];
    // 截断到提示中的最后偏移量
    Status s = TruncateTo(truncate_offset);
    if (!s.ok()) {
      return s;  // 如果截断失败，返回错误
    }
  }

  // 获取当前的已提交偏移量
  LogOffset committed = committed_index();

  // 从提示的最后一个偏移量开始向前查找
  for (size_t i = hints.size() - 1; i >= 0; i--) {
    // 如果提示的偏移量小于已提交偏移量，则返回腐败错误
    if (hints[i].l_offset.index < committed.l_offset.index) {
      return Status::Corruption("hints less than committed index");
    }

    // 如果提示的偏移量等于已提交偏移量且属于同一任期，进行截断并返回内存日志的最后偏移量
    if (hints[i].l_offset.index == committed.l_offset.index) {
      if (hints[i].l_offset.term == committed.l_offset.term) {
        // 截断到该偏移量
        Status s = TruncateTo(hints[i]);
        if (!s.ok()) {
          return s;  // 截断失败，返回错误
        }
        *reply_offset = mem_logger_->last_offset();  // 返回最后的偏移量
        return Status::OK();                         // 成功
      }
    }

    // 查找当前提示的偏移量是否存在
    LogOffset found_offset;
    bool res = mem_logger_->FindLogItem(hints[i], &found_offset);
    if (!res) {
      return Status::Corruption("hints not found " + hints[i].ToString());
    }

    // 如果找到的偏移量与提示的偏移量属于同一任期，则进行截断并返回内存日志的最后偏移量
    if (found_offset.l_offset.term == hints[i].l_offset.term) {
      // 截断到找到的偏移量
      Status s = TruncateTo(found_offset);
      if (!s.ok()) {
        return s;  // 截断失败，返回错误
      }
      *reply_offset = mem_logger_->last_offset();  // 返回最后的偏移量
      return Status::OK();                         // 成功
    }
  }

  // 如果没有匹配的偏移量，则截断到提示中的第一个偏移量
  Status s = TruncateTo(hints[0]);
  if (!s.ok()) {
    return s;  // 截断失败，返回错误
  }

  // 返回内存日志的最后偏移量
  *reply_offset = mem_logger_->last_offset();
  return Status::OK();  // 成功
}
