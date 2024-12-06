// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_H_
#define PIKA_BINLOG_H_

#include <atomic>

#include "include/pika_define.h"
#include "pstd/include/env.h"
#include "pstd/include/noncopyable.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"

std::string NewFileName(const std::string& name, uint32_t current);

class Version final : public pstd::noncopyable {
 public:
  Version(const std::shared_ptr<pstd::RWFile>& save);
  ~Version();

  pstd::Status Init();

  // RWLock should be held when access members.
  pstd::Status StableSave();

  uint32_t pro_num_ = 0;
  uint64_t pro_offset_ = 0;
  uint64_t logic_id_ = 0;
  uint32_t term_ = 0;

  std::shared_mutex rwlock_;

  void debug() {
    std::shared_lock l(rwlock_);
    printf("Current pro_num %u pro_offset %llu\n", pro_num_, pro_offset_);
  }

 private:
  // shared with versionfile_
  std::shared_ptr<pstd::RWFile> save_;
};
// Binlog 类用于管理二进制日志文件的读写、状态管理等功能。
// 它提供了线程安全的接口，通过锁机制确保多线程环境中的日志操作一致性。
// 此类继承自 pstd::noncopyable，表示该类不可复制或赋值。

class Binlog : public pstd::noncopyable {
 public:
  // 构造函数：初始化 Binlog 的文件路径和单个日志文件的最大大小（默认 100MB）。
  Binlog(std::string Binlog_path, int file_size = 100 * 1024 * 1024);

  // 析构函数：用于释放资源，如关闭文件句柄。
  ~Binlog();

  // 加锁和解锁方法，确保线程安全。
  void Lock() { mutex_.lock(); }
  void Unlock() { mutex_.unlock(); }

  // 向 Binlog 中追加一条记录。
  pstd::Status Put(const std::string& item);

  // 检查 Binlog 是否处于打开状态。
  pstd::Status IsOpened();

  // 获取生产者的状态，包括当前文件编号、写入偏移、任期（可选）和逻辑 ID（可选）。
  pstd::Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint32_t* term = nullptr,
                                 uint64_t* logic_id = nullptr);

  // 设置生产者状态（包括文件编号和偏移），带锁保护。
  pstd::Status SetProducerStatus(uint32_t pro_num, uint64_t pro_offset, uint32_t term = 0, uint64_t index = 0);

  // 截断日志到指定的生产者状态，需要提前持有锁。
  pstd::Status Truncate(uint32_t pro_num, uint64_t pro_offset, uint64_t index);

  // 获取当前正在操作的日志文件名。
  std::string filename() { return filename_; }

  // 设置当前任期（term），需要持有锁。
  // 任期变化会被保存到稳定存储中（`StableSave`）。
  void SetTerm(uint32_t term) {
    std::lock_guard l(version_->rwlock_);  // 加写锁保护 term 修改
    version_->term_ = term;
    version_->StableSave();  // 保存到磁盘
  }

  // 获取当前任期（term），使用共享锁保护读取。
  uint32_t term() {
    std::shared_lock l(version_->rwlock_);  // 加读锁保护 term 读取
    return version_->term_;
  }

  // 关闭 Binlog，释放文件资源。
  void Close();

 private:
  // 内部方法：向 Binlog 写入一段原始数据。
  pstd::Status Put(const char* item, int len);

  // 内部方法：向物理文件中写入一条记录。
  pstd::Status EmitPhysicalRecord(RecordType t, const char* ptr, size_t n, int* temp_pro_offset);

  // 内部方法：在日志文件中添加填充以对齐。
  static pstd::Status AppendPadding(pstd::WritableFile* file, uint64_t* len);

  // 初始化日志文件，包括文件创建和版本信息加载。
  void InitLogFile();

  // 内部生产接口：将一条记录写入日志并更新偏移量。
  pstd::Status Produce(const pstd::Slice& item, int* pro_offset);

  // 是否已打开 Binlog 的标志。
  std::atomic<bool> opened_;

  // 管理 Binlog 版本信息的对象，包含任期和稳定存储操作。
  std::unique_ptr<Version> version_;

  // 当前正在写入的 Binlog 文件。
  std::unique_ptr<pstd::WritableFile> queue_;

  // 版本文件的共享指针，用于在版本管理中保证资源安全。
  std::shared_ptr<pstd::RWFile> versionfile_;

  // 线程互斥锁，用于保护共享数据的访问。
  pstd::Mutex mutex_;

  // 当前生产者的文件编号。
  uint32_t pro_num_ = 0;

  // 当前块内的偏移量。
  int block_offset_ = 0;

  // Binlog 文件的路径。
  const std::string binlog_path_;

  // 单个 Binlog 文件的最大大小。
  uint64_t file_size_ = 0;

  // 当前 Binlog 文件名。
  std::string filename_;

  // 标志 Binlog 是否发生 IO 错误。
  std::atomic<bool> binlog_io_error_;
};

#endif
