// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_reader.h"

#include <glog/logging.h>

using pstd::Status;

PikaBinlogReader::PikaBinlogReader(uint32_t cur_filenum, uint64_t cur_offset)
    : cur_filenum_(cur_filenum),
      cur_offset_(cur_offset),
      backing_store_(std::make_unique<char[]>(kBlockSize)),
      buffer_() {
  last_record_offset_ = cur_offset % kBlockSize;
}

PikaBinlogReader::PikaBinlogReader() : backing_store_(std::make_unique<char[]>(kBlockSize)), buffer_() {
  last_record_offset_ = 0 % kBlockSize;
}

void PikaBinlogReader::GetReaderStatus(uint32_t* cur_filenum, uint64_t* cur_offset) {
  std::shared_lock l(rwlock_);
  *cur_filenum = cur_filenum_;
  *cur_offset = cur_offset_;
}

bool PikaBinlogReader::ReadToTheEnd() {
  uint32_t pro_num;
  uint64_t pro_offset;
  logger_->GetProducerStatus(&pro_num, &pro_offset);
  std::shared_lock l(rwlock_);
  return (pro_num == cur_filenum_ && pro_offset == cur_offset_);
}
int PikaBinlogReader::Seek(const std::shared_ptr<Binlog>& logger, uint32_t filenum, uint64_t offset) {
  // 根据日志文件名和文件号构造目标文件的名称
  std::string confile = NewFileName(logger->filename(), filenum);

  // 检查文件是否存在
  if (!pstd::FileExists(confile)) {
    LOG(WARNING) << confile << " not exists";  // 如果文件不存在，打印警告并返回错误码
    return -1;
  }

  // 创建一个 SequentialFile 对象用于顺序读取
  std::unique_ptr<pstd::SequentialFile> readfile;
  // 如果创建失败，打印警告并返回错误码
  if (!pstd::NewSequentialFile(confile, readfile).ok()) {
    LOG(WARNING) << "New sequential " << confile << " failed";  // 创建文件失败
    return -1;
  }

  // 如果队列已存在，则先重置队列
  if (queue_) {
    queue_.reset();
  }

  // 将新文件指针赋给队列，并记录当前的 logger
  queue_ = std::move(readfile);
  logger_ = logger;

  // 使用写锁保护当前文件号、当前偏移量和上次记录的偏移量
  std::lock_guard l(rwlock_);
  cur_filenum_ = filenum;                           // 更新当前文件号
  cur_offset_ = offset;                             // 更新当前偏移量
  last_record_offset_ = cur_filenum_ % kBlockSize;  // 计算上次记录的偏移量，按块大小取模

  pstd::Status s;
  // 计算要跳过的起始块位置，按块大小对齐
  uint64_t start_block = (cur_offset_ / kBlockSize) * kBlockSize;

  // 跳过到文件中的起始块
  s = queue_->Skip(start_block);
  uint64_t block_offset = cur_offset_ % kBlockSize;  // 计算当前偏移量在当前块中的位置
  uint64_t ret = 0;
  uint64_t res = 0;
  bool is_error = false;

  // 读取日志直到偏移量匹配
  while (true) {
    // 如果已经读取到目标偏移量位置，退出循环
    if (res >= block_offset) {
      cur_offset_ = start_block + res;  // 更新当前偏移量
      break;
    }

    ret = 0;
    // 获取下一条记录，返回值保存在 ret 中
    is_error = GetNext(&ret);

    // 如果获取下一条记录失败，返回错误
    if (is_error) {
      return -1;
    }

    res += ret;  // 更新已经读取的字节数
  }

  // 更新最后记录的偏移量
  last_record_offset_ = cur_offset_ % kBlockSize;
  return 0;  // 成功返回 0
}

bool PikaBinlogReader::GetNext(uint64_t* size) {
  uint64_t offset = 0;
  pstd::Status s;
  bool is_error = false;

  while (true) {
    buffer_.clear();
    s = queue_->Read(kHeaderSize, &buffer_, backing_store_.get());
    if (!s.ok()) {
      is_error = true;
      return is_error;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[7];
    const uint32_t length = a | (b << 8) | (c << 16);

    if (length > (kBlockSize - kHeaderSize)) {
      return true;
    }

    if (type == kFullType) {
      s = queue_->Read(length, &buffer_, backing_store_.get());
      offset += kHeaderSize + length;
      break;
    } else if (type == kFirstType) {
      s = queue_->Read(length, &buffer_, backing_store_.get());
      offset += kHeaderSize + length;
    } else if (type == kMiddleType) {
      s = queue_->Read(length, &buffer_, backing_store_.get());
      offset += kHeaderSize + length;
    } else if (type == kLastType) {
      s = queue_->Read(length, &buffer_, backing_store_.get());
      offset += kHeaderSize + length;
      break;
    } else if (type == kBadRecord) {
      s = queue_->Read(length, &buffer_, backing_store_.get());
      offset += kHeaderSize + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  *size = offset;
  return is_error;
}
// ReadPhysicalRecord 方法用于从队列中读取一个物理记录（包括头部和数据）
unsigned int PikaBinlogReader::ReadPhysicalRecord(pstd::Slice* result, uint32_t* filenum, uint64_t* offset) {
  pstd::Status s;

  // 如果当前记录偏移量到达块大小，跳过当前块剩余部分，重置偏移量
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(kBlockSize - last_record_offset_);     // 跳过当前块剩余的部分
    std::lock_guard l(rwlock_);                         // 使用锁确保线程安全
    cur_offset_ += (kBlockSize - last_record_offset_);  // 更新当前偏移量
    last_record_offset_ = 0;                            // 重置当前记录偏移量
  }

  // 清空缓冲区并读取记录头部
  buffer_.clear();
  s = queue_->Read(kHeaderSize, &buffer_, backing_store_.get());  // 读取头部大小的数据
  if (s.IsEndFile()) {
    return kEof;  // 如果到达文件末尾，返回结束标志
  } else if (!s.ok()) {
    return kBadRecord;  // 如果读取失败，返回坏记录标志
  }

  // 解析头部内容
  const char* header = buffer_.data();  // 获取头部数据的指针
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[7];               // 获取记录类型（头部最后一个字节）
  const uint32_t length = a | (b << 8) | (c << 16);  // 解析记录长度（3字节）

  // 如果记录长度大于一个块的大小（减去头部），返回坏记录标志
  if (length > (kBlockSize - kHeaderSize)) {
    return kBadRecord;
  }

  // 如果记录类型是零类型或记录长度为零，则返回旧记录标志
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  // 读取记录数据（头部之后的部分）
  buffer_.clear();
  s = queue_->Read(length, &buffer_, backing_store_.get());  // 读取数据部分
  *result = pstd::Slice(buffer_.data(), buffer_.size());     // 将读取的数据存入 result
  last_record_offset_ += kHeaderSize + length;               // 更新当前记录的偏移量

  // 如果读取成功，更新文件编号和偏移量，并返回记录类型
  if (s.ok()) {
    std::lock_guard l(rwlock_);             // 使用锁确保线程安全
    *filenum = cur_filenum_;                // 设置文件编号
    cur_offset_ += (kHeaderSize + length);  // 更新当前偏移量
    *offset = cur_offset_;                  // 返回当前偏移量
  }

  return type;  // 返回记录类型
}

// Consume 一条 binlog 消息
// 根据不同的记录类型，处理数据并返回相应的状态
Status PikaBinlogReader::Consume(std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  Status s;

  pstd::Slice fragment;
  while (true) {
    // 读取物理记录，获取记录类型、文件号和偏移量
    const unsigned int record_type = ReadPhysicalRecord(&fragment, filenum, offset);

    // 根据不同的记录类型执行不同的处理逻辑
    switch (record_type) {
      case kFullType:
        // 完整记录，直接将数据赋给 scratch 并返回成功状态
        *scratch = std::string(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kFirstType:
        // 第一部分记录，初始化 scratch 并返回 "NotFound" 状态，表示中间状态
        scratch->assign(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        // 中间部分记录，追加到 scratch 中，并继续返回 "NotFound" 状态
        scratch->append(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kLastType:
        // 最后一部分记录，追加到 scratch 中并返回成功状态
        scratch->append(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kEof:
        // 文件结束，返回 "EndFile" 状态
        return Status::EndFile("Eof");
      case kBadRecord:
        // 读取到无效记录，日志警告并返回 "IOError" 状态
        LOG(WARNING)
            << "Read BadRecord record, will decode failed, this record may dbsync padded record, not processed here";
        return Status::IOError("Data Corruption");
      case kOldRecord:
        // 读取到旧记录，文件结束，返回 "EndFile" 状态
        return Status::EndFile("Eof");
      default:
        // 未知错误，返回 "IOError" 状态
        return Status::IOError("Unknow reason");
    }

    // 如果状态为 OK，跳出循环，表示成功处理
    if (s.ok()) {
      break;
    }
  }
  // DLOG(INFO) << "Binlog Sender consumer a msg: " << scratch; // 可选调试日志
  return Status::OK();
}

// 获取一条完整的 binlog 消息并附加到 scratch，返回处理结果状态（OK, IOError, Corruption, EndFile）
Status PikaBinlogReader::Get(std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  // 如果没有有效的 logger 或队列，返回腐败错误
  if (!logger_ || !queue_) {
    return Status::Corruption("Not seek");
  }
  scratch->clear();  // 清空 scratch 内容
  Status s = Status::OK();

  do {
    // 如果已经读取到文件末尾，返回文件结束状态
    if (ReadToTheEnd()) {
      return Status::EndFile("End of cur log file");
    }

    // 调用 Consume 函数处理当前记录
    s = Consume(scratch, filenum, offset);

    // 如果遇到文件结束，处理下一文件
    if (s.IsEndFile()) {
      // 构建下一个文件名
      std::string confile = NewFileName(logger_->filename(), cur_filenum_ + 1);

      // 等待 10 毫秒，等生产线程生成新的 binlog
      usleep(10000);

      // 如果下一个文件存在，切换到新文件并继续读取
      if (pstd::FileExists(confile)) {
        DLOG(INFO) << "BinlogSender roll to new binlog" << confile;
        queue_.reset();  // 重置队列
        queue_ = nullptr;

        // 打开新的 binlog 文件
        pstd::NewSequentialFile(confile, queue_);
        {
          std::lock_guard l(rwlock_);
          cur_filenum_++;   // 更新当前文件号
          cur_offset_ = 0;  // 重置当前偏移量
        }
        last_record_offset_ = 0;  // 重置最后的记录偏移量
      } else {
        // 如果文件不存在，返回 IO 错误
        return Status::IOError("File Does Not Exists");
      }
    } else {
      break;  // 如果不是文件结束，跳出循环
    }
  } while (s.IsEndFile());  // 继续处理，直到遇到文件结束

  return Status::OK();  // 成功处理一条消息
}
