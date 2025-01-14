// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/time.h>

#include <utility>

#include "include/pika_binlog_transverter.h"
#include "pstd/include/pstd_defer.h"
#include "pstd_status.h"

using pstd::Status;

std::string NewFileName(const std::string& name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return {buf};
}

/*
 * Version
 */
Version::Version(const std::shared_ptr<pstd::RWFile>& save) : save_(save) { assert(save_ != nullptr); }

Version::~Version() { StableSave(); }

Status Version::StableSave() {
  char* p = save_->GetData();
  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &logic_id_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &term_, sizeof(uint32_t));
  return Status::OK();
}

Status Version::Init() {
  Status s;
  if (save_->GetData()) {
    memcpy(reinterpret_cast<char*>(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&pro_offset_), save_->GetData() + 4, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&logic_id_), save_->GetData() + 12, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&term_), save_->GetData() + 20, sizeof(uint32_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

/*
 * Binlog
 */
Binlog::Binlog(std::string binlog_path, const int file_size)
    : opened_(false), binlog_path_(std::move(binlog_path)), file_size_(file_size), binlog_io_error_(false) {
  // To intergrate with old version, we don't set mmap file size to 100M;
  // pstd::SetMmapBoundSize(file_size);
  // pstd::kMmapBoundSize = 1024 * 1024 * 100;

  Status s;

  pstd::CreateDir(binlog_path_);

  filename_ = binlog_path_ + kBinlogPrefix;
  const std::string manifest = binlog_path_ + kManifest;
  std::string profile;

  if (!pstd::FileExists(manifest)) {
    LOG(INFO) << "Binlog: Manifest file not exist, we create a new one.";

    profile = NewFileName(filename_, pro_num_);
    s = pstd::NewWritableFile(profile, queue_);
    if (!s.ok()) {
      LOG(FATAL) << "Binlog: new " << filename_ << " " << s.ToString();
    }
    std::unique_ptr<pstd::RWFile> tmp_file;
    s = pstd::NewRWFile(manifest, tmp_file);
    versionfile_.reset(tmp_file.release());
    if (!s.ok()) {
      LOG(FATAL) << "Binlog: new versionfile error " << s.ToString();
    }

    version_ = std::make_unique<Version>(versionfile_);
    version_->StableSave();
  } else {
    LOG(INFO) << "Binlog: Find the exist file.";
    std::unique_ptr<pstd::RWFile> tmp_file;
    s = pstd::NewRWFile(manifest, tmp_file);
    versionfile_.reset(tmp_file.release());
    if (s.ok()) {
      version_ = std::make_unique<Version>(versionfile_);
      version_->Init();
      pro_num_ = version_->pro_num_;

      // Debug
      // version_->debug();
    } else {
      LOG(FATAL) << "Binlog: open versionfile error";
    }

    profile = NewFileName(filename_, pro_num_);
    DLOG(INFO) << "Binlog: open profile " << profile;
    s = pstd::AppendWritableFile(profile, queue_, version_->pro_offset_);
    if (!s.ok()) {
      LOG(FATAL) << "Binlog: Open file " << profile << " error " << s.ToString();
    }

    uint64_t filesize = queue_->Filesize();
    DLOG(INFO) << "Binlog: filesize is " << filesize;
  }

  InitLogFile();
}

Binlog::~Binlog() {
  std::lock_guard l(mutex_);
  Close();
}

void Binlog::Close() {
  if (!opened_.load()) {
    return;
  }
  opened_.store(false);
}

void Binlog::InitLogFile() {
  assert(queue_ != nullptr);

  uint64_t filesize = queue_->Filesize();
  block_offset_ = static_cast<int32_t>(filesize % kBlockSize);

  opened_.store(true);
}

Status Binlog::IsOpened() {
  if (!opened_.load()) {
    return Status::Busy("Binlog is not open yet");
  }
  return Status::OK();
}

// GetProducerStatus 方法用于获取生产者的状态，包括文件号、偏移量、逻辑ID和term。
Status Binlog::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint32_t* term, uint64_t* logic_id) {
  // 检查 binlog 是否已经打开，如果没有打开，返回 Busy 状态。
  if (!opened_.load()) {
    return Status::Busy("Binlog is not open yet");  // 如果未打开，返回一个忙碌的状态。
  }

  // 使用 std::shared_lock 进行读锁定，确保对 version_ 的并发读取是安全的。
  std::shared_lock l(version_->rwlock_);

  // 从 version_ 中读取生产者的状态信息，并赋值给传入的参数
  *filenum = version_->pro_num_;        // 获取当前生产者的文件编号
  *pro_offset = version_->pro_offset_;  // 获取当前生产者的偏移量

  // 如果传入的指针 logic_id 不为空，设置 logic_id。
  if (logic_id) {
    *logic_id = version_->logic_id_;  // 获取当前生产者的逻辑ID
  }

  // 如果传入的指针 term 不为空，设置 term。
  if (term) {
    *term = version_->term_;  // 获取当前生产者的 term
  }

  // 返回 OK 状态，表示获取生产者状态成功。
  return Status::OK();
}

// Note: mutex lock should be held
// 将一条新的 binlog 项写入文件
Status Binlog::Put(const std::string& item) {
  // 如果 binlog 文件未打开，返回忙碌状态
  if (!opened_.load()) {
    return Status::Busy("Binlog is not open yet");
  }

  uint32_t filenum = 0;   // 用于存储文件编号
  uint32_t term = 0;      // 用于存储术语（例如: 日志的循环周期）
  uint64_t offset = 0;    // 用于存储文件偏移量
  uint64_t logic_id = 0;  // 用于存储逻辑ID（每条记录的唯一标识）

  Lock();  // 锁住 binlog，确保线程安全

  DEFER {
    Unlock();  // 在函数结束时解锁
  };

  // 获取当前生产者状态，读取文件编号、偏移量、术语和逻辑ID
  Status s = GetProducerStatus(&filenum, &offset, &term, &logic_id);
  if (!s.ok()) {
    return s;  // 获取状态失败，返回错误
  }

  // 增加逻辑ID
  logic_id++;

  // 将日志项编码为 binlog 数据
  std::string data = PikaBinlogTransverter::BinlogEncode(BinlogType::TypeFirst, time(nullptr), term, logic_id, filenum,
                                                         offset, item, {});

  // 将编码后的 binlog 数据写入
  s = Put(data.c_str(), static_cast<int>(data.size()));
  if (!s.ok()) {
    binlog_io_error_.store(true);  // 如果写入失败，记录 I/O 错误状态
  }

  return s;  // 返回写入操作的状态
}

// Note: mutex lock should be held
// 将一个二进制的日志项（以字符数组形式）写入文件
Status Binlog::Put(const char* item, int len) {
  Status s;

  /* Check to roll log file */
  // 获取当前 binlog 文件的大小
  uint64_t filesize = queue_->Filesize();

  // 如果文件大小超过预定的最大值，则创建新的日志文件
  if (filesize > file_size_) {
    std::unique_ptr<pstd::WritableFile> queue;
    std::string profile = NewFileName(filename_, pro_num_ + 1);  // 生成新的日志文件名
    s = pstd::NewWritableFile(profile, queue);                   // 创建新文件
    if (!s.ok()) {
      // 如果文件创建失败，记录错误并返回
      LOG(ERROR) << "Binlog: new " << filename_ << " " << s.ToString();
      return s;
    }

    // 关闭当前文件并切换到新的文件
    queue_.reset();
    queue_ = std::move(queue);
    pro_num_++;  // 增加文件的序号

    {
      std::lock_guard l(version_->rwlock_);  // 锁住版本信息
      version_->pro_offset_ = 0;             // 重置文件偏移量
      version_->pro_num_ = pro_num_;         // 更新文件序号
      version_->StableSave();                // 持久化保存版本信息
    }
    InitLogFile();  // 初始化新日志文件
  }

  // 将日志项写入当前日志文件
  int pro_offset;
  s = Produce(pstd::Slice(item, len), &pro_offset);  // 生产日志条目并获取偏移量
  if (s.ok()) {
    std::lock_guard l(version_->rwlock_);  // 锁住版本信息
    version_->pro_offset_ = pro_offset;    // 更新文件的偏移量
    version_->logic_id_++;                 // 增加逻辑ID
    version_->StableSave();                // 持久化保存版本信息
  }

  return s;  // 返回写入操作的状态
}

Status Binlog::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n, int* temp_pro_offset) {
  Status s;
  assert(n <= 0xffffff);
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  char buf[kHeaderSize];

  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  now = tv.tv_sec;
  buf[0] = static_cast<char>(n & 0xff);
  buf[1] = static_cast<char>((n & 0xff00) >> 8);
  buf[2] = static_cast<char>(n >> 16);
  buf[3] = static_cast<char>(now & 0xff);
  buf[4] = static_cast<char>((now & 0xff00) >> 8);
  buf[5] = static_cast<char>((now & 0xff0000) >> 16);
  buf[6] = static_cast<char>((now & 0xff000000) >> 24);
  buf[7] = static_cast<char>(t);

  s = queue_->Append(pstd::Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = queue_->Append(pstd::Slice(ptr, n));
    if (s.ok()) {
      s = queue_->Flush();
    }
  }
  block_offset_ += static_cast<int32_t>(kHeaderSize + n);

  *temp_pro_offset += static_cast<int32_t>(kHeaderSize + n);
  return s;
}

Status Binlog::Produce(const pstd::Slice& item, int* temp_pro_offset) {
  Status s;                       // 用于存储操作状态
  const char* ptr = item.data();  // 获取数据项的指针
  size_t left = item.size();      // 获取数据项的剩余字节数
  bool begin = true;              // 标记是否是数据块的开始

  *temp_pro_offset = static_cast<int>(version_->pro_offset_);  // 设置生产偏移量
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;  // 当前块中剩余的可用空间
    assert(leftover >= 0);                                              // 确保剩余空间不会小于0

    // 如果当前剩余空间不足以容纳头部，先填充剩余部分并提交
    if (static_cast<size_t>(leftover) < kHeaderSize) {
      if (leftover > 0) {
        // 填充剩余空间，使用空字节 (\x00)
        s = queue_->Append(pstd::Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        if (!s.ok()) {
          return s;  // 如果操作失败，返回状态
        }
        *temp_pro_offset += leftover;  // 更新生产偏移量
      }
      block_offset_ = 0;  // 重置块偏移量
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;  // 当前块可用空间
    const size_t fragment_length = (left < avail) ? left : avail;  // 当前片段的长度，取剩余数据长度或可用空间的最小值
    RecordType type;                                               // 记录类型
    const bool end = (left == fragment_length);  // 判断当前片段是否是最后一个片段

    // 根据当前片段的位置设置记录类型
    if (begin && end) {
      type = kFullType;  // 如果是第一个也是最后一个片段，记录类型为完整类型
    } else if (begin) {
      type = kFirstType;  // 如果是第一个片段，记录类型为开始类型
    } else if (end) {
      type = kLastType;  // 如果是最后一个片段，记录类型为结束类型
    } else {
      type = kMiddleType;  // 否则是中间类型
    }

    // 发出物理记录
    s = EmitPhysicalRecord(type, ptr, fragment_length, temp_pro_offset);
    if (!s.ok()) {
      return s;  // 如果操作失败，返回状态
    }

    // 更新数据指针和剩余数据长度
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;  // 之后不再是开始片段
  } while (s.ok() && left > 0);  // 如果还有剩余数据且操作成功，继续处理

  return s;  // 返回操作状态
}
// 为 Binlog 文件追加填充数据，以确保长度对齐块大小
Status Binlog::AppendPadding(pstd::WritableFile* file, uint64_t* len) {
  // 如果长度小于头部大小，直接返回成功
  if (*len < kHeaderSize) {
    return Status::OK();
  }

  Status s;
  char buf[kBlockSize];  // 定义块大小的缓冲区
  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, nullptr);  // 获取当前时间
  now = tv.tv_sec;             // 当前时间的秒数

  uint64_t left = *len;  // 剩余需要填充的长度
  while (left > 0 && s.ok()) {
    // 确定本次写入的大小，不能超过剩余长度或块大小
    uint32_t size = (left >= kBlockSize) ? kBlockSize : left;

    // 如果当前块大小小于头部大小，停止填充
    if (size < kHeaderSize) {
      break;
    } else {
      uint32_t bsize = size - kHeaderSize;  // 实际数据大小
      std::string binlog(bsize, '*');       // 创建填充数据（用 '*' 填充）

      // 填充头部信息
      buf[0] = static_cast<char>(bsize & 0xff);
      buf[1] = static_cast<char>((bsize & 0xff00) >> 8);
      buf[2] = static_cast<char>(bsize >> 16);
      buf[3] = static_cast<char>(now & 0xff);
      buf[4] = static_cast<char>((now & 0xff00) >> 8);
      buf[5] = static_cast<char>((now & 0xff0000) >> 16);
      buf[6] = static_cast<char>((now & 0xff000000) >> 24);
      buf[7] = static_cast<char>(kBadRecord);  // 表示这是一个坏记录标识符

      // 将头部信息追加到文件中
      s = file->Append(pstd::Slice(buf, kHeaderSize));
      if (s.ok()) {
        // 将填充数据追加到文件中
        s = file->Append(pstd::Slice(binlog.data(), binlog.size()));
        if (s.ok()) {
          s = file->Flush();  // 刷新文件
          left -= size;       // 减少剩余需要填充的长度
        }
      }
    }
  }

  *len -= left;  // 更新剩余长度
  if (left != 0) {
    // 如果还有剩余长度小于头部大小，记录警告日志
    LOG(WARNING) << "AppendPadding left bytes: " << left << " is less than kHeaderSize";
  }
  return s;
}

// 设置生产者状态，包括当前的文件号、偏移量、任期和逻辑索引
Status Binlog::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset, uint32_t term, uint64_t index) {
  // 如果 Binlog 尚未打开，返回繁忙状态
  if (!opened_.load()) {
    return Status::Busy("Binlog is not open yet");
  }

  std::lock_guard l(mutex_);  // 加锁以确保线程安全

  // 如果偏移量小于 4（最小头部长度），将其设置为 0
  if (pro_offset < 4) {
    pro_offset = 0;
  }

  // 重置队列
  queue_.reset();

  // 删除初始化文件
  std::string init_profile = NewFileName(filename_, 0);
  if (pstd::FileExists(init_profile)) {
    pstd::DeleteFile(init_profile);
  }

  // 删除目标文件
  std::string profile = NewFileName(filename_, pro_num);
  if (pstd::FileExists(profile)) {
    pstd::DeleteFile(profile);
  }

  // 创建新的可写文件并初始化填充
  pstd::NewWritableFile(profile, queue_);
  Binlog::AppendPadding(queue_.get(), &pro_offset);

  // 更新当前文件号
  pro_num_ = pro_num;

  {
    // 更新版本信息并保存
    std::lock_guard l(version_->rwlock_);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->term_ = term;
    version_->logic_id_ = index;
    version_->StableSave();  // 保存版本到磁盘
  }

  InitLogFile();        // 初始化日志文件
  return Status::OK();  // 返回成功状态
}

Status Binlog::Truncate(uint32_t pro_num, uint64_t pro_offset, uint64_t index) {
  queue_.reset();
  std::string profile = NewFileName(filename_, pro_num);
  const int fd = open(profile.c_str(), O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IOError("fd open failed");
  }
  if (ftruncate(fd, static_cast<int64_t>(pro_offset)) != 0) {
    return Status::IOError("ftruncate failed");
  }
  close(fd);

  pro_num_ = pro_num;
  {
    std::lock_guard l(version_->rwlock_);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->logic_id_ = index;
    version_->StableSave();
  }

  Status s = pstd::AppendWritableFile(profile, queue_, version_->pro_offset_);
  if (!s.ok()) {
    return s;
  }

  InitLogFile();

  return Status::OK();
}
