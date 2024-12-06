// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_STABLE_LOG_H_
#define PIKA_STABLE_LOG_H_

#include <map>
#include <memory>

#include "include/pika_binlog.h"

// StableLog 类是一个日志管理类，用于管理与数据库表相关的日志操作
// 它依赖 Binlog 类来记录日志，同时支持日志截断和清理操作
class StableLog : public std::enable_shared_from_this<StableLog> {
 public:
  // 构造函数：初始化 StableLog 类，传入表名和日志路径
  StableLog(std::string table_name, std::string log_path);

  // 析构函数：释放相关资源
  ~StableLog();

  // 返回 Binlog 对象的共享指针，用于访问底层日志记录器
  std::shared_ptr<Binlog> Logger() { return stable_logger_; }

  // 离开当前日志管理，可能是释放资源的操作
  void Leave();

  // 设置日志的第一个偏移值（首条日志的位置）
  void SetFirstOffset(const LogOffset& offset) {
    std::lock_guard l(offset_rwlock_);  // 写入时加互斥锁保护
    first_offset_ = offset;
  }

  // 获取日志的第一个偏移值（首条日志的位置）
  LogOffset first_offset() {
    std::shared_lock l(offset_rwlock_);  // 读取时加共享锁保护
    return first_offset_;
  }

  // 截断日志到指定的偏移位置（需要持有 Binlog 锁）
  pstd::Status TruncateTo(const LogOffset& offset);

  // 清理过期日志文件到指定文件编号（支持手动和自动模式）
  bool PurgeStableLogs(uint32_t to = 0, bool manual = false);

  // 清理日志操作的相关状态
  void ClearPurge();

  // 获取所有日志文件名及编号信息，存储在传入的 map 中
  bool GetBinlogFiles(std::map<uint32_t, std::string>* binlogs);

  // 删除指定编号之后的日志文件
  pstd::Status PurgeFileAfter(uint32_t filenum);

 private:
  // 关闭日志文件
  void Close();

  // 删除日志目录，清理所有相关日志文件
  void RemoveStableLogDir();

  // 更新第一个日志偏移值，基于指定的文件编号
  void UpdateFirstOffset(uint32_t filenum);

  /*
   * PurgeStableLogs 的内部操作函数
   * 该函数可能运行在后台线程中，负责执行具体的日志清理操作
   */
  static void DoPurgeStableLogs(void* arg);

  // 实际清理日志文件，返回清理是否成功
  bool PurgeFiles(uint32_t to, bool manual);

  // 原子变量，标志日志清理是否正在进行，避免并发冲突
  std::atomic<bool> purging_;

  // 数据库名称
  std::string db_name_;

  // 日志路径
  std::string log_path_;

  // Binlog 的共享指针，用于实际的日志操作
  std::shared_ptr<Binlog> stable_logger_;

  // 读写锁，用于保护 first_offset_ 的读写操作
  std::shared_mutex offset_rwlock_;

  // 当前日志的第一个偏移值
  LogOffset first_offset_;
};

struct PurgeStableLogArg {
  std::shared_ptr<StableLog> logger;
  uint32_t to = 0;
  bool manual = false;
  bool force = false;  // Ignore the delete window
};

#endif  // PIKA_STABLE_LOG_H_
