// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slave_node.h"
#include "include/pika_conf.h"

using pstd::Status;

extern std::unique_ptr<PikaConf> g_pika_conf;

/* SyncWindow */
// 向同步窗口中添加一个新的项，并更新窗口的总大小
void SyncWindow::Push(const SyncWinItem& item) {
  win_.push_back(item);              // 将新项添加到窗口尾部
  total_size_ += item.binlog_size_;  // 更新窗口的总大小
}
// 更新同步窗口中指定范围内的项的状态为已确认，并调整窗口内容
bool SyncWindow::Update(const SyncWinItem& start_item, const SyncWinItem& end_item, LogOffset* acked_offset) {
  size_t start_pos = win_.size();  // 起始项的位置，默认为未找到
  size_t end_pos = win_.size();    // 结束项的位置，默认为未找到

  // 遍历同步窗口，查找 start_item 和 end_item 的位置
  for (size_t i = 0; i < win_.size(); ++i) {
    if (win_[i] == start_item) {
      start_pos = i;
    }
    if (win_[i] == end_item) {
      end_pos = i;
      break;  // 找到结束项后可提前退出
    }
  }

  // 如果未找到起始或结束项，记录警告日志并返回 false
  if (start_pos == win_.size() || end_pos == win_.size()) {
    LOG(WARNING) << "Ack offset Start: " << start_item.ToString() << "End: " << end_item.ToString()
                 << " not found in binlog controller window." << std::endl
                 << "window status " << std::endl
                 << ToStringStatus();
    return false;
  }

  // 将指定范围内的项标记为已确认，并更新总大小
  for (size_t i = start_pos; i <= end_pos; ++i) {
    win_[i].acked_ = true;                // 标记为已确认
    total_size_ -= win_[i].binlog_size_;  // 减少已确认项的大小
  }

  // 移除窗口中前面已确认的项，并更新最后确认的偏移量
  while (!win_.empty()) {
    if (win_[0].acked_) {
      *acked_offset = win_[0].offset_;  // 更新确认的偏移量
      win_.pop_front();                 // 移除确认项
    } else {
      break;  // 遇到未确认项则停止
    }
  }
  return true;  // 更新成功
}
// 计算同步窗口中剩余可用空间大小
int SyncWindow::Remaining() {
  std::size_t remaining_size = g_pika_conf->sync_window_size() - win_.size();  // 计算剩余空间
  return static_cast<int>(remaining_size > 0 ? remaining_size : 0);            // 如果空间不足返回 0
}
// 构造函数：初始化从节点信息，包括 IP、端口、数据库名称及会话 ID
SlaveNode::SlaveNode(const std::string& ip, int port, const std::string& db_name, int session_id)
    : RmNode(ip, port, db_name, session_id) {}

// 析构函数：释放资源，默认为默认实现
SlaveNode::~SlaveNode() = default;
// 初始化 Binlog 文件读取器
Status SlaveNode::InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset) {
  binlog_reader = std::make_shared<PikaBinlogReader>();                  // 创建 Binlog 读取器实例
  int res = binlog_reader->Seek(binlog, offset.filenum, offset.offset);  // 定位到指定的 Binlog 偏移量
  if (res != 0) {                                                        // 如果定位失败，返回状态错误
    return Status::Corruption(ToString() + "  binlog reader init failed");
  }
  return Status::OK();  // 初始化成功
}
// 将从节点的状态转换为字符串，便于调试与日志记录
std::string SlaveNode::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << "    Slave_state: " << SlaveStateMsg[slave_state] << "\r\n";         // 从节点状态
  tmp_stream << "    Binlog_sync_state: " << BinlogSyncStateMsg[b_state] << "\r\n";  // Binlog 同步状态
  tmp_stream << "    Sync_window: "
             << "\r\n"
             << sync_win.ToStringStatus();                                                // 同步窗口状态
  tmp_stream << "    Sent_offset: " << sent_offset.ToString() << "\r\n";                  // 已发送的偏移量
  tmp_stream << "    Acked_offset: " << acked_offset.ToString() << "\r\n";                // 已确认的偏移量
  tmp_stream << "    Binlog_reader activated: " << (binlog_reader != nullptr) << "\r\n";  // Binlog 读取器是否激活
  return tmp_stream.str();
}

// 更新从节点的同步窗口状态及已确认偏移量
Status SlaveNode::Update(const LogOffset& start, const LogOffset& end, LogOffset* updated_offset) {
  if (slave_state != kSlaveBinlogSync) {  // 如果从节点状态不是 Binlog 同步状态，返回错误
    return Status::Corruption(ToString() + "state not BinlogSync");
  }

  *updated_offset = LogOffset();  // 初始化更新后的偏移量

  // 调用同步窗口的 Update 方法更新状态
  bool res = sync_win.Update(SyncWinItem(start), SyncWinItem(end), updated_offset);
  if (!res) {  // 如果更新失败，返回错误
    return Status::Corruption("UpdateAckedInfo failed");
  }

  // 如果没有更新，返回当前已确认的偏移量
  if (*updated_offset == LogOffset()) {
    *updated_offset = acked_offset;  // 返回当前确认的偏移量
    return Status::OK();
  }

  // 更新已确认的偏移量
  acked_offset = *updated_offset;
  return Status::OK();
}
