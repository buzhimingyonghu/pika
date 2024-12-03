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
  std::string confile = NewFileName(logger->filename(), filenum);
  if (!pstd::FileExists(confile)) {
    LOG(WARNING) << confile << " not exits";
    return -1;
  }
  std::unique_ptr<pstd::SequentialFile> readfile;
  if (!pstd::NewSequentialFile(confile, readfile).ok()) {
    LOG(WARNING) << "New swquential " << confile << " failed";
    return -1;
  }
  if (queue_) {
    queue_.reset();
  }
  queue_ = std::move(readfile);
  logger_ = logger;

  std::lock_guard l(rwlock_);
  cur_filenum_ = filenum;
  cur_offset_ = offset;
  last_record_offset_ = cur_filenum_ % kBlockSize;

  pstd::Status s;
  uint64_t start_block = (cur_offset_ / kBlockSize) * kBlockSize;
  s = queue_->Skip((cur_offset_ / kBlockSize) * kBlockSize);
  uint64_t block_offset = cur_offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;
  bool is_error = false;

  while (true) {
    if (res >= block_offset) {
      cur_offset_ = start_block + res;
      break;
    }
    ret = 0;
    is_error = GetNext(&ret);
    if (is_error) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = cur_offset_ % kBlockSize;
  return 0;
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

unsigned int PikaBinlogReader::ReadPhysicalRecord(pstd::Slice* result, uint32_t* filenum, uint64_t* offset) {
  pstd::Status s;
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    queue_->Skip(kBlockSize - last_record_offset_);
    std::lock_guard l(rwlock_);
    cur_offset_ += (kBlockSize - last_record_offset_);
    last_record_offset_ = 0;
  }
  buffer_.clear();
  s = queue_->Read(kHeaderSize, &buffer_, backing_store_.get());
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kBadRecord;
  }

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[7];
  const uint32_t length = a | (b << 8) | (c << 16);

  if (length > (kBlockSize - kHeaderSize)) {
    return kBadRecord;
  }

  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  buffer_.clear();
  s = queue_->Read(length, &buffer_, backing_store_.get());
  *result = pstd::Slice(buffer_.data(), buffer_.size());
  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    std::lock_guard l(rwlock_);
    *filenum = cur_filenum_;
    cur_offset_ += (kHeaderSize + length);
    *offset = cur_offset_;
  }
  return type;
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
