// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SLAVE_NODE_H_
#define PIKA_SLAVE_NODE_H_

#include <deque>
#include <memory>

#include "include/pika_binlog_reader.h"
#include "include/pika_define.h"

// SyncWinItem 结构体表示在主从同步过程中一个同步窗口中的单个项，
// 主要包含 Binlog 的偏移量、Binlog 的大小以及该项是否被确认 (acked)。
struct SyncWinItem {
  LogOffset offset_;             // Binlog 的偏移量信息，标识该项的位置
  std::size_t binlog_size_ = 0;  // Binlog 的大小，单位为字节
  bool acked_ = false;           // 标志该 Binlog 是否已被确认

  // 重载运算符，用于比较两个 SyncWinItem 是否相等
  bool operator==(const SyncWinItem& other) const {
    return offset_.b_offset.filenum == other.offset_.b_offset.filenum &&
           offset_.b_offset.offset == other.offset_.b_offset.offset;
  }

  // 构造函数，初始化一个 SyncWinItem，传入 Binlog 的偏移量和大小（默认大小为 0）
  explicit SyncWinItem(const LogOffset& offset, std::size_t binlog_size = 0)
      : offset_(offset), binlog_size_(binlog_size) {}

  // 返回 SyncWinItem 的字符串表示，包含 Binlog 偏移量、大小和确认状态
  std::string ToString() const {
    return offset_.ToString() + " binglog size: " + std::to_string(binlog_size_) +
           " acked: " + std::to_string(static_cast<int>(acked_));
  }
};

// SyncWindow 类表示一个同步窗口，
// 该窗口用于管理一系列 Binlog 项，确保从节点能够按顺序同步 Binlog 数据。
// 它支持添加同步项、更新窗口状态以及查看窗口状态。
class SyncWindow {
 public:
  SyncWindow() = default;

  // 将一个新的 SyncWinItem 添加到同步窗口
  void Push(const SyncWinItem& item);

  // 更新同步窗口的状态，表示同步窗口中的数据已经处理完毕。
  // 返回值为 true 表示更新成功，false 表示失败。
  bool Update(const SyncWinItem& start_item, const SyncWinItem& end_item, LogOffset* acked_offset);

  // 获取窗口中剩余未处理的 Binlog 项数量
  int Remaining();

  // 返回当前同步窗口的状态，以字符串形式显示窗口大小及窗口的首尾元素
  std::string ToStringStatus() const {
    if (win_.empty()) {
      return "      Size: " + std::to_string(win_.size()) + "\r\n";
    } else {
      std::string res;
      res += "      Size: " + std::to_string(win_.size()) + "\r\n";
      res += ("      Begin_item: " + win_.begin()->ToString() + "\r\n");
      res += ("      End_item: " + win_.rbegin()->ToString() + "\r\n");
      return res;
    }
  }

  // 获取窗口中所有 Binlog 项的总大小
  std::size_t GetTotalBinlogSize() { return total_size_; }

  // 重置同步窗口，清空所有项并重置总大小
  void Reset() {
    win_.clear();
    total_size_ = 0;
  }

 private:
  // 使用 deque 存储同步窗口中的所有 SyncWinItem 项
  std::deque<SyncWinItem> win_;

  // 存储同步窗口中所有 Binlog 项的总大小
  std::size_t total_size_ = 0;
};

// SlaveNode 类表示从节点（Slave），从节点通过该类管理与主节点的同步工作。
// 它包括同步状态、Binlog 读取器以及与同步窗口相关的操作。
class SlaveNode : public RmNode {
 public:
  // 构造函数，初始化从节点的 IP、端口、数据库名称和会话 ID
  SlaveNode(const std::string& ip, int port, const std::string& db_name, int session_id);

  // 析构函数，用于释放资源
  ~SlaveNode() override;

  // 锁定从节点的状态，确保线程安全
  void Lock() { slave_mu.lock(); }

  // 解锁从节点的状态
  void Unlock() { slave_mu.unlock(); }

  // 当前从节点的同步状态
  SlaveState slave_state{kSlaveNotSync};

  // Binlog 同步状态
  BinlogSyncState b_state{kNotSync};

  // 当前从节点的同步窗口
  SyncWindow sync_win;

  // 当前发送的 Binlog 偏移量
  LogOffset sent_offset;

  // 当前确认的 Binlog 偏移量
  LogOffset acked_offset;

  // 获取从节点的状态信息
  std::string ToStringStatus();

  // 用于读取 Binlog 文件的 Binlog 读取器
  std::shared_ptr<PikaBinlogReader> binlog_reader;

  // 初始化 Binlog 文件读取器，设置起始的 Binlog 偏移量
  pstd::Status InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset);

  // 更新从节点的同步状态，处理 Binlog 数据的同步进度
  pstd::Status Update(const LogOffset& start, const LogOffset& end, LogOffset* updated_offset);

  // 用于从节点同步的互斥锁，确保同步过程中线程安全
  pstd::Mutex slave_mu;
};

#endif  // PIKA_SLAVE_NODE_H_
