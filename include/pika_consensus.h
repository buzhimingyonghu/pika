// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#ifndef PIKA_CONSENSUS_H_
#define PIKA_CONSENSUS_H_

#include <utility>

#include "include/pika_binlog_transverter.h"
#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "include/pika_slave_node.h"
#include "include/pika_stable_log.h"
#include "pstd/include/env.h"

// 上下文类，负责管理日志的状态和存储
class Context : public pstd::noncopyable {
 public:
  // 构造函数，初始化上下文路径
  Context(std::string path);

  // 初始化方法，用于加载上下文的初始状态
  pstd::Status Init();
  // 将上下文的当前状态保存到稳定存储中（需要持有 RWLock）
  pstd::Status StableSave();
  // 更新已应用的日志索引
  void UpdateAppliedIndex(const LogOffset& offset);
  // 重置已应用的日志索引
  void Reset(const LogOffset& offset);

  // 读写锁，保护成员变量的并发访问
  std::shared_mutex rwlock_;
  // 已应用的日志索引
  LogOffset applied_index_;
  // 已应用的日志窗口，用于记录日志的应用范围
  SyncWindow applied_win_;

  // 返回当前上下文状态的字符串表示
  std::string ToString() {
    std::stringstream tmp_stream;
    std::shared_lock l(rwlock_);
    tmp_stream << "  Applied_index " << applied_index_.ToString() << "\r\n";
    tmp_stream << "  Applied window " << applied_win_.ToStringStatus();
    return tmp_stream.str();
  }

 private:
  // 上下文保存路径
  std::string path_;
  // 文件句柄，用于保存上下文的稳定状态
  std::unique_ptr<pstd::RWFile> save_;
};

class SyncProgress {
 public:
  SyncProgress() = default;
  ~SyncProgress() = default;

  std::shared_ptr<SlaveNode> GetSlaveNode(const std::string& ip, int port);
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> GetAllSlaveNodes();
  pstd::Status AddSlaveNode(const std::string& ip, int port, const std::string& db_name, int session_id);
  pstd::Status RemoveSlaveNode(const std::string& ip, int port);
  // 更新从节点的同步进度，并返回已提交的日志索引
  pstd::Status Update(const std::string& ip, int port, const LogOffset& start, const LogOffset& end,
                      LogOffset* committed_index);
  // 获取当前从节点的数量
  int SlaveSize();

 private:
  std::shared_mutex rwlock_;
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves_;
  std::unordered_map<std::string, LogOffset> match_index_;
};

class MemLog {
 public:
  struct LogItem {
    // 构造函数，初始化日志项
    LogItem(const LogOffset& _offset, std::shared_ptr<Cmd> _cmd_ptr, std::shared_ptr<PikaClientConn> _conn_ptr,
            std::shared_ptr<std::string> _resp_ptr)
        : offset(_offset),
          cmd_ptr(std::move(_cmd_ptr)),
          conn_ptr(std::move(_conn_ptr)),
          resp_ptr(std::move(_resp_ptr)) {}

    // 日志偏移量
    LogOffset offset;
    // 命令对象的共享指针
    std::shared_ptr<Cmd> cmd_ptr;
    // 客户端连接的共享指针
    std::shared_ptr<PikaClientConn> conn_ptr;
    // 响应消息的共享指针
    std::shared_ptr<std::string> resp_ptr;
  };

  MemLog();
  int Size();

  // 追加日志项到内存日志中
  void AppendLog(const LogItem& item) {
    std::lock_guard lock(logs_mu_);  // 加锁保护并发访问
    logs_.push_back(item);           // 将日志项添加到日志列表
    last_offset_ = item.offset;      // 更新最后的日志偏移量
  }

  // 截断日志到指定的偏移量
  pstd::Status TruncateTo(const LogOffset& offset);

  // 重置日志并设置偏移量
  void Reset(const LogOffset& offset);

  // 获取最后的日志偏移量
  LogOffset last_offset() {
    std::lock_guard lock(logs_mu_);  // 加锁保护读取
    return last_offset_;
  }

  // 设置最后的日志偏移量
  void SetLastOffset(const LogOffset& offset) {
    std::lock_guard lock(logs_mu_);  // 加锁保护写入
    last_offset_ = offset;
  }

  // 根据偏移量查找日志项
  bool FindLogItem(const LogOffset& offset, LogOffset* found_offset);

 private:
  // 内部方法：根据二进制日志偏移查找日志索引
  int InternalFindLogByBinlogOffset(const LogOffset& offset);

  // 内部方法：根据逻辑索引查找日志索引
  int InternalFindLogByLogicIndex(const LogOffset& offset);

  // 日志互斥锁，保护日志列表的并发访问
  pstd::Mutex logs_mu_;
  // 日志列表，存储所有的日志项
  std::vector<LogItem> logs_;
  // 最后一个日志项的偏移量
  LogOffset last_offset_;
};

class ConsensusCoordinator {
 public:
  // 构造函数，初始化协调器实例
  ConsensusCoordinator(const std::string& db_name);
  // 析构函数，释放资源
  ~ConsensusCoordinator();

  // 初始化方法，由构造函数调用，无需持有锁
  void Init();
  // 重置日志偏移，由数据库同步进程调用
  pstd::Status Reset(const LogOffset& offset);

  pstd::Status ProposeLog(const std::shared_ptr<Cmd>& cmd_ptr);

  pstd::Status UpdateSlave(const std::string& ip, int port, const LogOffset& start, const LogOffset& end);

  pstd::Status AddSlaveNode(const std::string& ip, int port, int session_id);

  pstd::Status RemoveSlaveNode(const std::string& ip, int port);

  void UpdateTerm(uint32_t term);

  uint32_t term();

  pstd::Status ProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute);

  // 领导者协商：根据从节点的最后日志偏移判断是否需要拒绝请求
  pstd::Status LeaderNegotiate(const LogOffset& f_last_offset, bool* reject, std::vector<LogOffset>* hints);
  // 从节点协商：根据提示的日志偏移返回协商结果
  pstd::Status FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset);

  SyncProgress& SyncPros() { return sync_pros_; }
  std::shared_ptr<StableLog> StableLogger() { return stable_logger_; }
  std::shared_ptr<MemLog> MemLogger() { return mem_logger_; }

  // 获取已提交日志索引（需要加锁）
  LogOffset committed_index() {
    std::lock_guard lock(index_mu_);
    return committed_index_;
  }

  std::shared_ptr<Context> context() { return context_; }

  // redis parser cb
  struct CmdPtrArg {
    CmdPtrArg(std::shared_ptr<Cmd> ptr) : cmd_ptr(std::move(ptr)) {}
    std::shared_ptr<Cmd> cmd_ptr;
  };
  static int InitCmd(net::RedisParser* parser, const net::RedisCmdArgsType& argv);

  std::string ToStringStatus() {
    std::stringstream tmp_stream;
    {
      std::lock_guard lock(index_mu_);
      tmp_stream << "  Committed_index: " << committed_index_.ToString() << "\r\n";
    }
    tmp_stream << "  Context: "
               << "\r\n"
               << context_->ToString();
    {
      std::shared_lock lock(term_rwlock_);
      tmp_stream << "  Term: " << term_ << "\r\n";
    }
    tmp_stream << "  Mem_logger size: " << mem_logger_->Size() << " last offset "
               << mem_logger_->last_offset().ToString() << "\r\n";
    tmp_stream << "  Stable_logger first offset " << stable_logger_->first_offset().ToString() << "\r\n";
    LogOffset log_status;
    stable_logger_->Logger()->GetProducerStatus(&(log_status.b_offset.filenum), &(log_status.b_offset.offset),
                                                &(log_status.l_offset.term), &(log_status.l_offset.index));
    tmp_stream << "  Physical Binlog Status: " << log_status.ToString() << "\r\n";
    return tmp_stream.str();
  }

 private:
  // 截断日志到指定的 LogOffset
  pstd::Status TruncateTo(const LogOffset& offset);

  // 内部方法：追加日志到内存日志中
  pstd::Status InternalAppendLog(const std::shared_ptr<Cmd>& cmd_ptr);
  // 内部方法：追加日志到二进制日志中
  pstd::Status InternalAppendBinlog(const std::shared_ptr<Cmd>& cmd_ptr);
  // 内部方法：将内存日志中的日志项应用到状态机
  void InternalApply(const MemLog::LogItem& log);
  // 内部方法：以从节点角色应用命令到状态机
  void InternalApplyFollower(const std::shared_ptr<Cmd>& cmd_ptr);

  // 获取二进制日志对应的逻辑日志偏移
  pstd::Status GetBinlogOffset(const BinlogOffset& start_offset, LogOffset* log_offset);
  // 获取某范围内的二进制日志对应的逻辑日志偏移
  pstd::Status GetBinlogOffset(const BinlogOffset& start_offset, const BinlogOffset& end_offset,
                               std::vector<LogOffset>* log_offset);
  // 根据目标索引值在二进制日志文件中查找文件编号
  pstd::Status FindBinlogFileNum(const std::map<uint32_t, std::string>& binlogs, uint64_t target_index,
                                 uint32_t start_filenum, uint32_t* founded_filenum);
  // 通过遍历二进制日志找到目标逻辑偏移
  pstd::Status FindLogicOffsetBySearchingBinlog(const BinlogOffset& hint_offset, uint64_t target_index,
                                                LogOffset* found_offset);
  // 从指定偏移开始查找目标逻辑偏移
  pstd::Status FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index, LogOffset* found_offset);
  // 获取在指定偏移之前的日志
  pstd::Status GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints);

 private:
  pstd::Mutex order_mu_;
  pstd::Mutex index_mu_;
  // 已提交的逻辑日志索引
  LogOffset committed_index_;

  // 上下文对象的共享指针
  std::shared_ptr<Context> context_;

  std::shared_mutex term_rwlock_;
  uint32_t term_ = 0;

  // 数据库名称
  std::string db_name_;

  // 同步进度管理器
  SyncProgress sync_pros_;
  // 稳定日志对象的共享指针
  std::shared_ptr<StableLog> stable_logger_;
  // 内存日志对象的共享指针
  std::shared_ptr<MemLog> mem_logger_;
};
#endif  // INCLUDE_PIKA_CONSENSUS_H_
