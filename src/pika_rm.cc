// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_rm.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <utility>

#include "net/include/net_cli.h"

#include "include/pika_conf.h"
#include "include/pika_server.h"

#include "include/pika_admin.h"
#include "include/pika_command.h"

using pstd::Status;

extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern PikaServer* g_pika_server;

/* SyncDB */

SyncDB::SyncDB(const std::string& db_name) : db_info_(db_name) {}

std::string SyncDB::DBName() { return db_info_.db_name_; }

/* SyncMasterDB*/
// 构造函数：初始化同步主数据库对象
SyncMasterDB::SyncMasterDB(const std::string& db_name) : SyncDB(db_name), coordinator_(db_name) {}

// 获取从库节点数量
int SyncMasterDB::GetNumberOfSlaveNode() {
  return coordinator_.SyncPros().SlaveSize();  // 从协调器获取从库节点数量
}

// 检查指定的从库节点是否存在
bool SyncMasterDB::CheckSlaveNodeExist(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);  // 获取指定 IP 和端口的从库节点
  return static_cast<bool>(slave_ptr);  // 如果找到，从库节点指针不为空，返回 true，否则返回 false
}

// 获取指定从库节点的会话ID
Status SyncMasterDB::GetSlaveNodeSession(const std::string& ip, int port, int32_t* session) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);  // 获取指定 IP 和端口的从库节点
  if (!slave_ptr) {
    return Status::NotFound("slave " + ip + ":" + std::to_string(port) +
                            " not found");  // 如果找不到从库，返回未找到错误
  }

  slave_ptr->Lock();                  // 锁定从库节点，确保线程安全
  *session = slave_ptr->SessionId();  // 获取从库的会话ID
  slave_ptr->Unlock();                // 解锁从库节点

  return Status::OK();  // 返回成功状态
}

// 向同步主数据库添加从库节点
Status SyncMasterDB::AddSlaveNode(const std::string& ip, int port, int session_id) {
  Status s = coordinator_.AddSlaveNode(ip, port, session_id);  // 通过协调器添加从库节点
  if (!s.ok()) {
    LOG(WARNING) << "Add Slave Node Failed, db: " << SyncDBInfo().ToString() << ", ip_port: " << ip << ":" << port;
    return s;  // 如果添加失败，记录警告日志并返回错误
  }
  LOG(INFO) << "Add Slave Node, db: " << SyncDBInfo().ToString() << ", ip_port: " << ip << ":" << port;
  return Status::OK();  // 返回成功状态
}

// 从同步主数据库移除从库节点
Status SyncMasterDB::RemoveSlaveNode(const std::string& ip, int port) {
  Status s = coordinator_.RemoveSlaveNode(ip, port);  // 通过协调器移除从库节点
  if (!s.ok()) {
    LOG(WARNING) << "Remove Slave Node Failed, db: " << SyncDBInfo().ToString() << ", ip_port: " << ip << ":" << port;
    return s;  // 如果移除失败，记录警告日志并返回错误
  }
  LOG(INFO) << "Remove Slave Node, DB: " << SyncDBInfo().ToString() << ", ip_port: " << ip << ":" << port;
  return Status::OK();  // 返回成功状态
}

// 激活从库的 binlog 同步
Status SyncMasterDB::ActivateSlaveBinlogSync(const std::string& ip, int port, const LogOffset& offset) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);  // 获取指定 IP 和端口的从库节点
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));  // 如果找不到从库节点，返回未找到错误
  }

  {
    std::lock_guard l(slave_ptr->slave_mu);     // 锁定从库节点，确保线程安全
    slave_ptr->slave_state = kSlaveBinlogSync;  // 设置从库状态为 binlog 同步
    slave_ptr->sent_offset = offset;            // 设置已发送的 binlog 偏移量
    slave_ptr->acked_offset = offset;           // 设置已确认的 binlog 偏移量

    // 初始化 binlog 文件读取器
    Status s = slave_ptr->InitBinlogFileReader(Logger(), offset.b_offset);
    if (!s.ok()) {
      return Status::Corruption("Init binlog file reader failed" + s.ToString());  // 如果初始化失败，返回错误状态
    }

    // 初始化了新的 binlog 文件读取器后，清空写队列中的项，并重置同步窗口。
    g_pika_rm->DropItemInOneWriteQueue(ip, port, slave_ptr->DBName());  // 清空写队列
    slave_ptr->sync_win.Reset();                                        // 重置同步窗口
    slave_ptr->b_state = kReadFromFile;                                 // 设置从库状态为读取文件
  }

  // 将 binlog 同步到写队列
  Status s = SyncBinlogToWq(ip, port);
  if (!s.ok()) {
    return s;  // 如果同步失败，返回错误状态
  }

  return Status::OK();  // 返回成功状态
}
// 将 binlog 数据同步到工作队列
Status SyncMasterDB::SyncBinlogToWq(const std::string& ip, int port) {
  // 获取指定 IP 和端口的从库节点
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    // 如果没有找到该从库节点，返回 NotFound 错误
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  Status s;
  slave_ptr->Lock();  // 锁定从库节点，防止其他线程修改其状态
  // 从 binlog 文件中读取数据并同步到工作队列
  s = ReadBinlogFileToWq(slave_ptr);
  slave_ptr->Unlock();  // 解锁

  if (!s.ok()) {
    // 如果同步失败，返回失败的状态
    return s;
  }

  // 如果成功，返回 OK 状态
  return Status::OK();
}

// 激活从库的数据库同步
Status SyncMasterDB::ActivateSlaveDbSync(const std::string& ip, int port) {
  // 获取指定 IP 和端口的从库节点
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    // 如果没有找到该从库节点，返回 NotFound 错误
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();  // 锁定从库节点，防止其他线程修改其状态
  // 将从库状态设置为数据库同步状态
  slave_ptr->slave_state = kSlaveDbSync;
  // 触发数据库同步操作
  slave_ptr->Unlock();  // 解锁

  // 返回 OK 状态，表示成功激活从库数据库同步
  return Status::OK();
}

// 功能：根据从节点的同步窗口（sync_win），读取 binlog
// 数据，并生成对应的写任务，如果读取过程没有错误，将任务发送到写队列。
Status SyncMasterDB::ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr) {
  //

  // 计算同步窗口中剩余可用空间大小
  int cnt = slave_ptr->sync_win.Remaining();

  // 获取当前从节点的 binlog 阅读器
  std::shared_ptr<PikaBinlogReader> reader = slave_ptr->binlog_reader;

  // 如果从节点没有 binlog 阅读器，则直接返回成功状态
  if (!reader) {
    return Status::OK();
  }

  // 用于存储本次读取的写任务
  std::vector<WriteTask> tasks;

  for (int i = 0; i < cnt; ++i) {
    std::string msg;   // 用于存储从 binlog 读取的消息
    uint32_t filenum;  // 用于存储 binlog 文件编号
    uint64_t offset;   // 用于存储当前 binlog 数据的偏移量

    // 如果同步窗口中的总 binlog 数据量超过 2 倍的最大缓冲区大小，停止读取
    if (slave_ptr->sync_win.GetTotalBinlogSize() > PIKA_MAX_CONN_RBUF_HB * 2) {
      LOG(INFO) << slave_ptr->ToString()
                << " total binlog size in sync window is :" << slave_ptr->sync_win.GetTotalBinlogSize();
      break;
    }

    // 从 binlog 阅读器中获取一条数据
    Status s = reader->Get(&msg, &filenum, &offset);

    // 如果已到达文件结尾，则停止读取
    if (s.IsEndFile()) {
      break;
    }
    // 如果发生损坏或 I/O 错误，记录警告并返回错误
    else if (s.IsCorruption() || s.IsIOError()) {
      LOG(WARNING) << SyncDBInfo().ToString() << " Read Binlog error : " << s.ToString();
      return s;
    }

    // 将读取到的消息解码为 binlog 项
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, msg, &item)) {
      LOG(WARNING) << "Binlog item decode failed";
      return Status::Corruption("Binlog item decode failed");
    }

    // 创建对应的 binlog 偏移量对象
    BinlogOffset sent_b_offset = BinlogOffset(filenum, offset);
    LogicOffset sent_l_offset = LogicOffset(item.term_id(), item.logic_id());
    LogOffset sent_offset(sent_b_offset, sent_l_offset);

    // 将当前读取的 binlog 数据和偏移量信息推送到同步窗口
    slave_ptr->sync_win.Push(SyncWinItem(sent_offset, msg.size()));

    // 更新从节点的最后发送时间
    slave_ptr->SetLastSendTime(pstd::NowMicros());

    // 构造用于发送到从节点的写任务
    RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->DBName(), slave_ptr->SessionId());
    WriteTask task(rm_node, BinlogChip(sent_offset, msg), slave_ptr->sent_offset);

    // 将写任务添加到任务列表
    tasks.push_back(task);

    // 更新从节点的已发送偏移量
    slave_ptr->sent_offset = sent_offset;
  }

  // 如果有任务需要发送，调用 ProduceWriteQueue 将任务推送到写队列
  if (!tasks.empty()) {
    g_pika_rm->ProduceWriteQueue(slave_ptr->Ip(), slave_ptr->Port(), db_info_.db_name_, tasks);
  }

  // 返回成功状态
  return Status::OK();
}

// 更新从库的同步日志偏移量
Status SyncMasterDB::ConsensusUpdateSlave(const std::string& ip, int port, const LogOffset& start,
                                          const LogOffset& end) {
  // 调用协调器更新从库的同步状态
  Status s = coordinator_.UpdateSlave(ip, port, start, end);
  if (!s.ok()) {
    // 如果更新失败，记录警告日志并返回错误
    LOG(WARNING) << SyncDBInfo().ToString() << s.ToString();
    return s;
  }

  // 如果更新成功，返回 OK 状态
  return Status::OK();
}

// 获取指定从库的同步 binlog 信息，包括发送偏移量和确认偏移量
Status SyncMasterDB::GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset,
                                            BinlogOffset* acked_offset) {
  // 获取指定 IP 和端口的从库节点
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    // 如果没有找到该从库节点，返回 NotFound 错误
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  // 锁定从库节点，获取其发送和确认的 binlog 偏移量
  slave_ptr->Lock();
  *sent_offset = slave_ptr->sent_offset.b_offset;    // 获取发送偏移量
  *acked_offset = slave_ptr->acked_offset.b_offset;  // 获取确认偏移量
  slave_ptr->Unlock();

  // 返回 OK 状态，表示成功获取
  return Status::OK();
}

// 获取指定从库的同步状态
Status SyncMasterDB::GetSlaveState(const std::string& ip, int port, SlaveState* const slave_state) {
  // 获取指定 IP 和端口的从库节点
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    // 如果没有找到该从库节点，返回 NotFound 错误
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  // 锁定从库节点，获取其同步状态
  slave_ptr->Lock();
  *slave_state = slave_ptr->slave_state;  // 获取从库的同步状态
  slave_ptr->Unlock();

  // 返回 OK 状态，表示成功获取状态
  return Status::OK();
}

Status SyncMasterDB::WakeUpSlaveBinlogSync() {
  // 函数作用：检查所有从节点是否已同步完毕，如果同步完成，读取 binlog 文件并发送到从节点。
  // 功能：遍历所有从节点，判断每个从节点的偏移量是否一致，若一致，读取 binlog 数据并写入队列，如果失败则删除该从节点。

  // 获取所有从节点信息
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();

  // 用于存储需要删除的从节点列表
  std::vector<std::shared_ptr<SlaveNode>> to_del;

  // 遍历所有的从节点
  for (auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;

    // 使用锁保护对每个从节点的操作，确保线程安全
    std::lock_guard l(slave_ptr->slave_mu);

    // 检查该从节点的已发送偏移量与已确认偏移量是否相等
    if (slave_ptr->sent_offset == slave_ptr->acked_offset) {
      // 如果偏移量一致，表示该从节点已经同步完成，准备从日志文件读取 binlog 数据并写入写队列
      Status s = ReadBinlogFileToWq(slave_ptr);
      if (!s.ok()) {
        // 如果读取 binlog 文件失败，将该从节点加入到删除列表
        to_del.push_back(slave_ptr);
        LOG(WARNING) << "WakeUpSlaveBinlogSync failed, Delete from RM, slave: " << slave_ptr->ToStringStatus() << " "
                     << s.ToString();
      }
    }
  }

  // 遍历所有失败的从节点并将其从管理系统中删除
  for (auto& to_del_slave : to_del) {
    RemoveSlaveNode(to_del_slave->Ip(), to_del_slave->Port());
  }

  // 返回操作成功状态
  return Status::OK();
}

// 设置从库的最后接收时间
Status SyncMasterDB::SetLastRecvTime(const std::string& ip, int port, uint64_t time) {
  // 获取指定 IP 和端口的从库节点
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    // 如果没有找到该从库节点，返回错误
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  // 锁定从库节点，设置最后接收时间
  slave_ptr->Lock();
  slave_ptr->SetLastRecvTime(time);
  slave_ptr->Unlock();

  // 返回成功状态
  return Status::OK();
}

// 获取安全的删除 binlog 文件编号
Status SyncMasterDB::GetSafetyPurgeBinlog(std::string* safety_purge) {
  BinlogOffset boffset;

  // 获取当前生产者的日志状态（文件编号和偏移量）
  Status s = Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    return s;  // 如果获取日志状态失败，返回错误
  }

  bool success = false;
  uint32_t purge_max = boffset.filenum;  // 初始化最大删除文件编号

  // 如果文件编号大于等于 10，则可以进行 binlog 清理
  if (purge_max >= 10) {
    success = true;
    purge_max -= 10;  // 设置允许删除的最大文件编号

    // 获取所有从库节点
    std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
    for (const auto& slave_iter : slaves) {
      std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;

      // 锁定从库节点，检查从库状态
      std::lock_guard l(slave_ptr->slave_mu);

      // 如果从库处于 binlog 同步状态，并且 acked_offset 比较有效，更新 purge_max
      if (slave_ptr->slave_state == SlaveState::kSlaveBinlogSync && slave_ptr->acked_offset.b_offset.filenum > 0) {
        purge_max = std::min(slave_ptr->acked_offset.b_offset.filenum - 1, purge_max);  // 保证不会删除过早的 binlog
      } else {
        success = false;
        break;  // 如果某个从库不符合条件，退出循环
      }
    }
  }

  // 返回可以安全删除的 binlog 文件编号，如果不符合条件则返回 "none"
  *safety_purge = (success ? kBinlogPrefix + std::to_string(static_cast<int32_t>(purge_max)) : "none");
  return Status::OK();
}

// 判断是否可以安全清理指定编号的 binlog 文件
bool SyncMasterDB::BinlogCloudPurge(uint32_t index) {
  BinlogOffset boffset;

  // 获取当前生产者的日志状态（文件编号和偏移量）
  Status s = Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    return false;  // 如果获取日志状态失败，返回 false
  }

  // 如果要删除的文件编号大于当前文件编号减去 10，表示保留一些历史数据，不能删除
  if (index > (boffset.filenum - 10)) {
    return false;
  } else {
    // 获取所有从库节点
    std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
    for (const auto& slave_iter : slaves) {
      std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;

      // 锁定从库节点，检查其状态
      std::lock_guard l(slave_ptr->slave_mu);

      // 如果从库处于 DB 同步状态，不能进行 binlog 清理
      if (slave_ptr->slave_state == SlaveState::kSlaveDbSync) {
        return false;
      }
      // 如果从库处于 binlog 同步状态，检查其 acked_offset 是否超过了要删除的文件编号
      else if (slave_ptr->slave_state == SlaveState::kSlaveBinlogSync) {
        if (index >= slave_ptr->acked_offset.b_offset.filenum) {
          return false;  // 如果从库的 acked_offset 大于或等于要删除的文件编号，不能删除该文件
        }
      }
    }
  }

  // 如果所有条件都满足，返回 true 表示可以清理
  return true;
}
// 检查同步是否超时，并根据超时情况处理从库节点
Status SyncMasterDB::CheckSyncTimeout(uint64_t now) {
  // 获取所有从库节点
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();

  // 用于存储超时的从库节点，待删除
  std::vector<Node> to_del;

  // 遍历所有从库节点
  for (auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;

    // 锁定从库节点
    std::lock_guard l(slave_ptr->slave_mu);

    // 如果接收超时，加入待删除列表
    if (slave_ptr->LastRecvTime() + kRecvKeepAliveTimeout < now) {
      to_del.emplace_back(slave_ptr->Ip(), slave_ptr->Port());
    }
    // 如果发送超时且没有数据发送/接收，发送 ping 请求
    else if (slave_ptr->LastSendTime() + kSendKeepAliveTimeout < now &&
             slave_ptr->sent_offset == slave_ptr->acked_offset) {
      std::vector<WriteTask> task;

      // 创建空的写任务作为 ping 请求
      RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->DBName(), slave_ptr->SessionId());
      WriteTask empty_task(rm_node, BinlogChip(LogOffset(), ""), LogOffset());
      task.push_back(empty_task);

      // 发送 ping 请求
      Status s = g_pika_rm->SendSlaveBinlogChipsRequest(slave_ptr->Ip(), slave_ptr->Port(), task);

      // 更新发送时间
      slave_ptr->SetLastSendTime(now);

      // 如果发送失败，记录日志并返回错误
      if (!s.ok()) {
        LOG(INFO) << "Send ping failed: " << s.ToString();
        return Status::Corruption("Send ping failed: " + slave_ptr->Ip() + ":" + std::to_string(slave_ptr->Port()));
      }
    }
  }

  // 删除所有超时的从库节点
  for (auto& node : to_del) {
    coordinator_.SyncPros().RemoveSlaveNode(node.Ip(), node.Port());       // 从协调器移除
    g_pika_rm->DropItemInOneWriteQueue(node.Ip(), node.Port(), DBName());  // 从写队列中移除
    LOG(WARNING) << SyncDBInfo().ToString() << " Master del Recv Timeout slave success "
                 << node.ToString();  // 记录日志
  }

  // 返回操作成功状态
  return Status::OK();
}

// 返回当前同步主库的状态信息
std::string SyncMasterDB::ToStringStatus() {
  std::stringstream tmp_stream;

  // 输出当前主库的会话 ID
  tmp_stream << " Current Master Session: " << session_id_ << "\r\n";
  tmp_stream << "  Consensus: "
             << "\r\n"
             << coordinator_.ToStringStatus();  // 输出协调器状态

  // 获取所有从库节点
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();

  int i = 0;
  // 遍历每个从库节点，输出其状态信息
  for (const auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    std::lock_guard l(slave_ptr->slave_mu);

    tmp_stream << "  slave[" << i << "]: " << slave_ptr->ToString() << "\r\n" << slave_ptr->ToStringStatus();
    i++;
  }

  // 返回拼接好的状态字符串
  return tmp_stream.str();
}

// 生成一个新的会话 ID
int32_t SyncMasterDB::GenSessionId() {
  std::lock_guard ml(session_mu_);
  return session_id_++;  // 增加并返回新的会话 ID
}

// 检查从库的会话 ID 是否与主库一致
bool SyncMasterDB::CheckSessionId(const std::string& ip, int port, const std::string& db_name, int session_id) {
  // 获取指定 IP 和端口的从库节点
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    // 如果没有找到该从库节点，记录日志并返回 false
    LOG(WARNING) << "Check SessionId Get Slave Node Error: " << ip << ":" << port << "," << db_name;
    return false;
  }

  // 锁定从库节点，检查会话 ID 是否匹配
  std::lock_guard l(slave_ptr->slave_mu);
  if (session_id != slave_ptr->SessionId()) {
    // 如果会话 ID 不匹配，记录日志并返回 false
    LOG(WARNING) << "Check SessionId Mismatch: " << ip << ":" << port << ", " << db_name << "_"
                 << " expected_session: " << session_id << ", actual_session:" << slave_ptr->SessionId();
    return false;
  }

  // 如果会话 ID 匹配，返回 true
  return true;
}

Status SyncMasterDB::ConsensusProposeLog(const std::shared_ptr<Cmd>& cmd_ptr) {
  return coordinator_.ProposeLog(cmd_ptr);
}

Status SyncMasterDB::ConsensusProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute) {
  return coordinator_.ProcessLeaderLog(cmd_ptr, attribute);
}

LogOffset SyncMasterDB::ConsensusCommittedIndex() { return coordinator_.committed_index(); }

LogOffset SyncMasterDB::ConsensusLastIndex() { return coordinator_.MemLogger()->last_offset(); }

std::shared_ptr<SlaveNode> SyncMasterDB::GetSlaveNode(const std::string& ip, int port) {
  return coordinator_.SyncPros().GetSlaveNode(ip, port);
}

std::unordered_map<std::string, std::shared_ptr<SlaveNode>> SyncMasterDB::GetAllSlaveNodes() {
  return coordinator_.SyncPros().GetAllSlaveNodes();
}

/* SyncSlaveDB */

// SyncSlaveDB 构造函数，初始化同步从库的信息
SyncSlaveDB::SyncSlaveDB(const std::string& db_name) : SyncDB(db_name) {
  // 设置同步路径，dbsync_path 为数据库同步目录
  std::string dbsync_path = g_pika_conf->db_sync_path() + "/" + db_name;

  // 创建 rsync 客户端对象，负责数据同步
  rsync_cli_.reset(new rsync::RsyncClient(dbsync_path, db_name));

  // 设置最后接收到消息的时间
  m_info_.SetLastRecvTime(pstd::NowMicros());
}

// 设置从库的复制状态
void SyncSlaveDB::SetReplState(const ReplState& repl_state) {
  // 如果状态为 "无连接"，则禁用复制
  if (repl_state == ReplState::kNoConnect) {
    Deactivate();
    return;
  }

  // 使用锁保护共享资源
  std::lock_guard l(db_mu_);
  repl_state_ = repl_state;
}

// 获取当前从库的复制状态
ReplState SyncSlaveDB::State() {
  std::lock_guard l(db_mu_);
  return repl_state_;
}

// 设置从库的最后接收时间
void SyncSlaveDB::SetLastRecvTime(uint64_t time) {
  std::lock_guard l(db_mu_);
  m_info_.SetLastRecvTime(time);
}

// 检查同步超时情况
Status SyncSlaveDB::CheckSyncTimeout(uint64_t now) {
  std::lock_guard l(db_mu_);

  // 如果当前状态不是等待同步或已连接状态，直接返回 OK
  if (repl_state_ != ReplState::kWaitDBSync && repl_state_ != ReplState::kConnected) {
    return Status::OK();
  }

  // 检查是否超时，如果超时，更新从库状态为尝试连接
  if (m_info_.LastRecvTime() + kRecvKeepAliveTimeout < now) {
    repl_state_ = ReplState::kTryConnect;  // 设为尝试连接状态
  }
  return Status::OK();
}

// 获取从库的同步状态信息
Status SyncSlaveDB::GetInfo(std::string* info) {
  // 构建从库状态信息字符串
  std::string tmp_str = "  Role: Slave\r\n";
  tmp_str += "  master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n";
  tmp_str += "  slave status: " + ReplStateMsg[repl_state_] + "\r\n";

  // 将信息添加到传入的字符串中
  info->append(tmp_str);
  return Status::OK();
}

// 激活从库的同步状态，并设置其复制状态
void SyncSlaveDB::Activate(const RmNode& master, const ReplState& repl_state) {
  std::lock_guard l(db_mu_);
  m_info_ = master;                            // 设置主库信息
  repl_state_ = repl_state;                    // 设置复制状态
  m_info_.SetLastRecvTime(pstd::NowMicros());  // 更新最后接收时间
}

// 停用从库，重置其信息和状态
void SyncSlaveDB::Deactivate() {
  std::lock_guard l(db_mu_);
  m_info_ = RmNode();                   // 重置主库信息
  repl_state_ = ReplState::kNoConnect;  // 设置为无连接状态
  rsync_cli_->Stop();                   // 停止同步客户端
}

std::string SyncSlaveDB::ToStringStatus() {
  return "  Master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n" +
         "  SessionId: " + std::to_string(MasterSessionId()) + "\r\n" + "  SyncStatus " + ReplStateMsg[repl_state_] +
         "\r\n";
}

const std::string& SyncSlaveDB::MasterIp() {
  std::lock_guard l(db_mu_);
  return m_info_.Ip();
}

int SyncSlaveDB::MasterPort() {
  std::lock_guard l(db_mu_);
  return m_info_.Port();
}

void SyncSlaveDB::SetMasterSessionId(int32_t session_id) {
  std::lock_guard l(db_mu_);
  m_info_.SetSessionId(session_id);
}

int32_t SyncSlaveDB::MasterSessionId() {
  std::lock_guard l(db_mu_);
  return m_info_.SessionId();
}

void SyncSlaveDB::SetLocalIp(const std::string& local_ip) {
  std::lock_guard l(db_mu_);
  local_ip_ = local_ip;
}

std::string SyncSlaveDB::LocalIp() {
  std::lock_guard l(db_mu_);
  return local_ip_;
}

void SyncSlaveDB::StopRsync() { rsync_cli_->Stop(); }

pstd::Status SyncSlaveDB::ActivateRsync() {
  // 初始化状态为 OK，表示函数正常运行
  Status s = Status::OK();

  // 检查 Rsync 客户端是否处于空闲状态
  // 如果 Rsync 客户端不是空闲状态，直接返回 OK 状态，不重复激活
  if (!rsync_cli_->IsIdle()) {
    return s;
  }

  // 记录日志，表示开始激活 Rsync，同时输出当前重试次数
  LOG(WARNING) << "Slave DB: " << DBName() << " Activating Rsync ... (retry count:" << rsync_init_retry_count_ << ")";

  // 初始化 Rsync 客户端
  if (rsync_cli_->Init()) {
    // 如果初始化成功，重置重试计数
    rsync_init_retry_count_ = 0;

    // 启动 Rsync 客户端，进入同步阶段
    rsync_cli_->Start();
    return s;  // 返回 OK 状态，表示成功激活 Rsync
  } else {
    // 如果初始化失败，增加重试计数
    rsync_init_retry_count_ += 1;

    // 如果重试次数超过最大限制，则设置从节点状态为错误
    if (rsync_init_retry_count_ >= kMaxRsyncInitReTryTimes) {
      SetReplState(ReplState::kError);

      // 记录错误日志，说明 Rsync 初始化失败的原因
      LOG(ERROR)
          << "Full Sync Stage - Rsync Init failed: Slave failed to pull meta info(generated by bgsave task in Master) "
             "from Master after MaxRsyncInitReTryTimes("
          << kMaxRsyncInitReTryTimes
          << " times) is reached. This usually means the Master's bgsave task has costed an unexpected-long time.";
    }

    // 返回错误状态，表示 Rsync 客户端初始化失败
    return Status::Error("rsync client init failed!");
  }
}
/* PikaReplicaManger */

// PikaReplicaManager 构造函数
PikaReplicaManager::PikaReplicaManager() {
  // 初始化一个包含 0.0.0.0 的 IP 地址集合
  std::set<std::string> ips;
  ips.insert("0.0.0.0");

  // 设置从库服务器端口，基于配置文件中的端口加上偏移量
  int port = g_pika_conf->port() + kPortShiftReplServer;

  // 创建 PikaReplClient 和 PikaReplServer 对象，分别用于客户端和服务器通信
  pika_repl_client_ = std::make_unique<PikaReplClient>(3000, 60);         // 参数为超时和重试次数
  pika_repl_server_ = std::make_unique<PikaReplServer>(ips, port, 3000);  // 端口和超时设置

  // 初始化数据库信息
  InitDB();
}

// 启动 PikaReplicaManager，包括启动复制客户端和复制服务器
void PikaReplicaManager::Start() {
  int ret = 0;

  // 启动复制客户端
  ret = pika_repl_client_->Start();
  if (ret != net::kSuccess) {
    LOG(FATAL) << "Start Repl Client Error: " << ret
               << (ret == net::kCreateThreadError ? ": create thread error " : ": other error");
  }

  // 启动复制服务器
  ret = pika_repl_server_->Start();
  if (ret != net::kSuccess) {
    LOG(FATAL) << "Start Repl Server Error: " << ret
               << (ret == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
}

// 停止复制客户端和复制服务器
void PikaReplicaManager::Stop() {
  pika_repl_client_->Stop();  // 停止复制客户端
  pika_repl_server_->Stop();  // 停止复制服务器
}

// 检查主库是否已经完成同步
bool PikaReplicaManager::CheckMasterSyncFinished() {
  // 遍历所有同步的主库数据库
  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;

    // 获取主库数据库的提交日志偏移量
    LogOffset commit = db->ConsensusCommittedIndex();
    BinlogOffset binlog;

    // 获取主库日志生产者的状态
    Status s = db->StableLogger()->Logger()->GetProducerStatus(&binlog.filenum, &binlog.offset);
    if (!s.ok()) {
      return false;  // 如果获取状态失败，返回 false
    }

    // 如果提交偏移量小于当前 binlog 偏移量，表示同步未完成
    if (commit.b_offset < binlog) {
      return false;
    }
  }
  return true;  // 所有主库同步完成，返回 true
}

// 初始化数据库结构，包括主库和从库的同步信息
void PikaReplicaManager::InitDB() {
  // 获取数据库结构配置
  std::vector<DBStruct> db_structs = g_pika_conf->db_structs();

  // 遍历数据库结构配置
  for (const auto& db : db_structs) {
    const std::string& db_name = db.db_name;

    // 初始化同步主库和从库的数据库信息
    sync_master_dbs_[DBInfo(db_name)] = std::make_shared<SyncMasterDB>(db_name);
    sync_slave_dbs_[DBInfo(db_name)] = std::make_shared<SyncSlaveDB>(db_name);
  }
}

void PikaReplicaManager::ProduceWriteQueue(const std::string& ip, int port, std::string db_name,
                                           const std::vector<WriteTask>& tasks) {
  std::lock_guard l(write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  for (auto& task : tasks) {
    write_queues_[index][db_name].push(task);
  }
}
// 函数主要作用：处理和发送写任务队列中的任务
// 功能：从多个数据库的写任务队列中批量获取写任务，打包成 binlog 数据包并发送到对应的从服务器。
// 如果发送失败，则删除该 IP:port 对应的队列。
int PikaReplicaManager::ConsumeWriteQueue() {
  // 用于存储每个目标 IP 和端口的待发送数据的映射
  std::unordered_map<std::string, std::vector<std::vector<WriteTask>>> to_send_map;
  int counter = 0;

  {
    std::lock_guard l(write_queue_mu_);

    // 遍历写队列（按 IP:port 分组）
    for (auto& iter : write_queues_) {
      const std::string& ip_port = iter.first;  // 当前的 IP:port 键
      std::unordered_map<std::string, std::queue<WriteTask>>& p_map = iter.second;

      for (auto& db_queue : p_map) {
        std::queue<WriteTask>& queue = db_queue.second;  // 获取当前数据库的写任务队列

        // 每次最多发送 kBinlogSendPacketNum 个任务
        for (int i = 0; i < kBinlogSendPacketNum; ++i) {
          if (queue.empty()) {
            break;  // 如果队列为空，跳出循环
          }

          // 确定本次批量发送的任务数（最多为 kBinlogSendBatchNum 个任务）
          size_t batch_index = queue.size() > kBinlogSendBatchNum ? kBinlogSendBatchNum : queue.size();
          std::vector<WriteTask> to_send;  // 存储本次要发送的写任务
          size_t batch_size = 0;           // 用于统计当前批次数据的总大小

          // 将队列中的任务打包成一个批次，直到达到批次大小限制或超过最大缓冲区大小
          for (size_t i = 0; i < batch_index; ++i) {
            WriteTask& task = queue.front();                 // 获取队列头部的任务
            batch_size += task.binlog_chip_.binlog_.size();  // 累加该任务的 binlog 大小

            // 确保 SerializeToString 时不会超过最大缓冲区大小（2GB）
            if (batch_size > PIKA_MAX_CONN_RBUF_HB) {
              break;  // 如果批次大小超过限制，跳出循环
            }

            to_send.push_back(task);  // 将任务加入待发送列表
            queue.pop();              // 从队列中移除已处理的任务
            counter++;                // 记录已处理的任务数
          }

          // 如果当前批次有任务需要发送，则添加到发送映射中
          if (!to_send.empty()) {
            to_send_map[ip_port].push_back(std::move(to_send));
          }
        }
      }
    }
  }

  // 用于存储发送失败的 IP:port 键，需要删除其对应的队列
  std::vector<std::string> to_delete;

  // 遍历准备好的发送数据
  for (auto& iter : to_send_map) {
    std::string ip;
    int port = 0;

    // 解析 IP:port 字符串
    if (!pstd::ParseIpPortString(iter.first, ip, port)) {
      LOG(WARNING) << "Parse ip_port error " << iter.first;  // 解析失败时记录警告
      continue;
    }

    // 发送每个批次的数据到对应的从服务器
    for (auto& to_send : iter.second) {
      Status s = pika_repl_server_->SendSlaveBinlogChips(ip, port, to_send);  // 调用发送方法
      if (!s.ok()) {
        // 如果发送失败，记录警告并将该 IP:port 添加到删除列表
        LOG(WARNING) << "send binlog to " << ip << ":" << port << " failed, " << s.ToString();
        to_delete.push_back(iter.first);
        continue;
      }
    }
  }

  // 如果有发送失败的队列，将其从 write_queues_ 中删除
  if (!to_delete.empty()) {
    std::lock_guard l(write_queue_mu_);
    for (auto& del_queue : to_delete) {
      write_queues_.erase(del_queue);  // 从写队列中删除对应的队列
    }
  }

  // 返回处理的任务总数
  return counter;
}

void PikaReplicaManager::DropItemInOneWriteQueue(const std::string& ip, int port, const std::string& db_name) {
  std::lock_guard l(write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  if (write_queues_.find(index) != write_queues_.end()) {
    write_queues_[index].erase(db_name);
  }
}

void PikaReplicaManager::DropItemInWriteQueue(const std::string& ip, int port) {
  std::lock_guard l(write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  write_queues_.erase(index);
}

void PikaReplicaManager::ScheduleReplServerBGTask(net::TaskFunc func, void* arg) {
  pika_repl_server_->Schedule(func, arg);
}

void PikaReplicaManager::ScheduleReplClientBGTask(net::TaskFunc func, void* arg) {
  pika_repl_client_->Schedule(func, arg);
}

void PikaReplicaManager::ScheduleReplClientBGTaskByDBName(net::TaskFunc func, void* arg, const std::string& db_name) {
  pika_repl_client_->ScheduleByDBName(func, arg, db_name);
}

void PikaReplicaManager::ScheduleWriteBinlogTask(const std::string& db,
                                                 const std::shared_ptr<InnerMessage::InnerResponse>& res,
                                                 const std::shared_ptr<net::PbConn>& conn, void* res_private_data) {
  pika_repl_client_->ScheduleWriteBinlogTask(db, res, conn, res_private_data);
}

void PikaReplicaManager::ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const std::string& db_name) {
  pika_repl_client_->ScheduleWriteDBTask(cmd_ptr, db_name);
}

void PikaReplicaManager::ReplServerRemoveClientConn(int fd) { pika_repl_server_->RemoveClientConn(fd); }

void PikaReplicaManager::ReplServerUpdateClientConnMap(const std::string& ip_port, int fd) {
  pika_repl_server_->UpdateClientConnMap(ip_port, fd);
}
// 更新同步 Binlog 的状态
Status PikaReplicaManager::UpdateSyncBinlogStatus(const RmNode& slave, const LogOffset& offset_start,
                                                  const LogOffset& offset_end) {
  // 使用共享锁保护 `sync_master_dbs_` 数据结构，防止写操作冲突
  std::shared_lock l(dbs_rw_);

  // 检查指定的从节点数据库信息是否存在于 `sync_master_dbs_` 中
  if (sync_master_dbs_.find(slave.NodeDBInfo()) == sync_master_dbs_.end()) {
    // 如果不存在，返回 NotFound 状态
    return Status::NotFound(slave.ToString() + " not found");
  }

  // 获取与从节点对应的主数据库对象
  std::shared_ptr<SyncMasterDB> db = sync_master_dbs_[slave.NodeDBInfo()];

  // 更新从节点的日志偏移量信息（offset_start 和 offset_end）
  Status s = db->ConsensusUpdateSlave(slave.Ip(), slave.Port(), offset_start, offset_end);
  if (!s.ok()) {
    // 如果更新失败，返回对应的错误状态
    return s;
  }

  // 尝试将同步的 Binlog 数据写入任务队列中
  s = db->SyncBinlogToWq(slave.Ip(), slave.Port());
  if (!s.ok()) {
    // 如果写入失败，返回对应的错误状态
    return s;
  }

  // 如果所有操作都成功，返回 OK 状态
  return Status::OK();
}
// 检查从库状态函数
// 主要用于判断指定 IP 和端口的从库是否处于未连接状态，并且是否需要重新连接主库
bool PikaReplicaManager::CheckSlaveDBState(const std::string& ip, const int port) {
  std::shared_ptr<SyncSlaveDB> db = nullptr;

  // 遍历所有从库实例
  for (const auto& iter : g_pika_rm->sync_slave_dbs_) {
    db = iter.second;

    // 检查从库状态是否为未连接，且其主库 IP 和端口匹配
    if (db->State() == ReplState::kDBNoConnect && db->MasterIp() == ip &&
        db->MasterPort() + kPortShiftReplServer == port) {
      // 打印日志信息：从库已经配置为不从属于任何主库，不再尝试重新连接
      LOG(INFO) << "DB: " << db->SyncDBInfo().ToString() << " has been dbslaveof no one, then will not try reconnect.";

      // 返回 false 表示无需尝试重新连接
      return false;
    }
  }

  // 如果没有匹配的未连接从库，返回 true
  return true;
}

// 禁用同步从库函数
// 该函数会将指定主库 IP 和端口对应的从库设置为非活动状态
Status PikaReplicaManager::DeactivateSyncSlaveDB(const std::string& ip, int port) {
  // 共享锁保护从库列表，防止并发修改
  std::shared_lock l(dbs_rw_);

  // 遍历所有从库实例
  for (auto& iter : sync_slave_dbs_) {
    std::shared_ptr<SyncSlaveDB> db = iter.second;

    // 找到匹配主库 IP 和端口的从库
    if (db->MasterIp() == ip && db->MasterPort() == port) {
      // 将从库设置为非活动状态
      db->Deactivate();
    }
  }

  // 返回操作成功状态
  return Status::OK();
}

// 处理连接丢失函数
// 当与某主库或从库的连接丢失时，更新相应状态并移除无效的从库节点
Status PikaReplicaManager::LostConnection(const std::string& ip, int port) {
  // 共享锁保护主从库列表，防止并发修改
  std::shared_lock l(dbs_rw_);

  // 遍历所有主库实例，尝试移除丢失连接的从库节点
  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;

    // 移除指定 IP 和端口的从库节点
    Status s = db->RemoveSlaveNode(ip, port);

    // 如果移除操作失败且错误类型不是“未找到”，则打印警告日志
    if (!s.ok() && !s.IsNotFound()) {
      LOG(WARNING) << "Lost Connection failed " << s.ToString();
    }
  }

  // 遍历所有从库实例，找到匹配的主库 IP 和端口，并将其禁用
  for (auto& iter : sync_slave_dbs_) {
    std::shared_ptr<SyncSlaveDB> db = iter.second;

    // 如果匹配主库 IP 和端口，将从库设置为非活动状态
    if (db->MasterIp() == ip && db->MasterPort() == port) {
      db->Deactivate();
    }
  }

  // 返回操作成功状态
  return Status::OK();
}

// 唤醒所有同步的主数据库进行binlog同步
Status PikaReplicaManager::WakeUpBinlogSync() {
  std::shared_lock l(dbs_rw_);  // 获取共享锁，确保读取操作的线程安全
  // 遍历所有同步的主数据库
  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;
    // 唤醒当前主数据库的从库进行binlog同步
    Status s = db->WakeUpSlaveBinlogSync();
    if (!s.ok()) {
      return s;  // 如果唤醒失败，则返回错误状态
    }
  }
  return Status::OK();  // 所有操作成功，返回 OK 状态
}

// 检查同步超时
Status PikaReplicaManager::CheckSyncTimeout(uint64_t now) {
  std::shared_lock l(dbs_rw_);  // 获取共享锁，确保读取操作的线程安全

  // 遍历所有同步的主数据库
  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;
    // 检查主数据库是否超时
    Status s = db->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();  // 如果超时检查失败，记录警告日志
    }
  }

  // 遍历所有同步的从数据库
  for (auto& iter : sync_slave_dbs_) {
    std::shared_ptr<SyncSlaveDB> db = iter.second;
    // 检查从数据库是否超时
    Status s = db->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();  // 如果超时检查失败，记录警告日志
    }
  }

  return Status::OK();  // 返回 OK 状态，表示所有超时检查完成
}

// 检查指定数据库的角色（主库或从库）
Status PikaReplicaManager::CheckDBRole(const std::string& db, int* role) {
  std::shared_lock l(dbs_rw_);  // 获取共享锁，确保读取操作的线程安全
  *role = 0;                    // 初始化角色为 0（表示没有角色）

  DBInfo p_info(db);  // 构造数据库信息对象
  // 如果该数据库在主数据库列表中没有找到
  if (sync_master_dbs_.find(p_info) == sync_master_dbs_.end()) {
    return Status::NotFound(db + " not found");  // 返回数据库未找到的错误
  }
  // 如果该数据库在从数据库列表中没有找到
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {
    return Status::NotFound(db + " not found");  // 返回数据库未找到的错误
  }

  // 如果该数据库是主库（主库没有从节点，或没有从节点且状态为 kNoConnect）
  if (sync_master_dbs_[p_info]->GetNumberOfSlaveNode() != 0 ||
      (sync_master_dbs_[p_info]->GetNumberOfSlaveNode() == 0 && sync_slave_dbs_[p_info]->State() == kNoConnect)) {
    *role |= PIKA_ROLE_MASTER;  // 设置为主库角色
  }

  // 如果该数据库的从库状态不为 kNoConnect，则设置为从库角色
  if (sync_slave_dbs_[p_info]->State() != ReplState::kNoConnect) {
    *role |= PIKA_ROLE_SLAVE;  // 设置为从库角色
  }

  // 如果角色不是主库或从库，则表示该数据库为单机模式
  return Status::OK();  // 返回检查结果
}

// SelectLocalIp 方法用于根据远程 IP 和端口，选择本地的 IP 地址。
// 它通过尝试连接到远程节点来获取本地的 IP 地址，并返回该 IP 地址给调用者。
Status PikaReplicaManager::SelectLocalIp(const std::string& remote_ip, const int remote_port,
                                         std::string* const local_ip) {
  // 创建一个新的 Redis 客户端实例，用于连接远程节点
  std::unique_ptr<net::NetCli> cli(net::NewRedisCli());
  cli->set_connect_timeout(1500);  // 设置连接超时时间为 1500 毫秒

  // 尝试连接到指定的远程 IP 和端口
  if ((cli->Connect(remote_ip, remote_port, "")).ok()) {
    struct sockaddr_in laddr;        // 定义本地地址结构
    socklen_t llen = sizeof(laddr);  // 获取地址结构的大小

    // 获取当前客户端的本地地址
    getsockname(cli->fd(), reinterpret_cast<struct sockaddr*>(&laddr), &llen);

    // 获取本地 IP 地址，并将其转换为字符串格式
    std::string tmp_ip(inet_ntoa(laddr.sin_addr));
    *local_ip = tmp_ip;  // 将获取的本地 IP 地址赋值给输出参数

    // 关闭连接
    cli->Close();
  } else {
    // 如果连接失败，输出警告日志并返回错误状态
    LOG(WARNING) << "Failed to connect remote node(" << remote_ip << ":" << remote_port << ")";
    return Status::Corruption("connect remote node error");
  }

  return Status::OK();  // 返回成功状态
}

// ActivateSyncSlaveDB 方法用于激活同步的从库数据库。
// 它会检查指定的从库数据库的状态，并根据状态进行相应的处理。
Status PikaReplicaManager::ActivateSyncSlaveDB(const RmNode& node, const ReplState& repl_state) {
  std::shared_lock l(dbs_rw_);               // 获取读锁，确保线程安全
  const DBInfo& p_info = node.NodeDBInfo();  // 获取指定节点的数据库信息

  // 检查该从库是否已经存在于同步的从库列表中
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {
    return Status::NotFound("Sync Slave DB " + node.ToString() + " not found");
  }

  // 获取当前同步从库的状态
  ReplState ssp_state = sync_slave_dbs_[p_info]->State();

  // 如果从库状态不为 kNoConnect 或 kDBNoConnect，则表示状态不正常
  if (ssp_state != ReplState::kNoConnect && ssp_state != ReplState::kDBNoConnect) {
    return Status::Corruption("Sync Slave DB in " + ReplStateMsg[ssp_state]);
  }

  std::string local_ip;                                         // 存储本地 IP 地址
  Status s = SelectLocalIp(node.Ip(), node.Port(), &local_ip);  // 获取本地 IP 地址

  if (s.ok()) {
    // 如果成功获取本地 IP，则设置该从库的本地 IP 地址并激活该同步数据库
    sync_slave_dbs_[p_info]->SetLocalIp(local_ip);
    sync_slave_dbs_[p_info]->Activate(node, repl_state);  // 激活从库
  }

  return s;  // 返回获取本地 IP 地址的状态
}

Status PikaReplicaManager::SendMetaSyncRequest() {
  Status s;
  if (time(nullptr) - g_pika_server->GetMetaSyncTimestamp() >= PIKA_META_SYNC_MAX_WAIT_TIME ||
      g_pika_server->IsFirstMetaSync()) {
    s = pika_repl_client_->SendMetaSync();
    if (s.ok()) {
      g_pika_server->UpdateMetaSyncTimestamp();
      g_pika_server->SetFirstMetaSync(false);
    }
  }
  return s;
}
// 发送移除从库节点的请求
Status PikaReplicaManager::SendRemoveSlaveNodeRequest(const std::string& db) {
  pstd::Status s;
  std::shared_lock l(dbs_rw_);  // 获取共享锁，确保读取操作的线程安全

  DBInfo p_info(db);  // 创建数据库信息对象
  // 如果找不到该从库数据库
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {
    return Status::NotFound("Sync Slave DB " + p_info.ToString());  // 返回数据库未找到的错误
  } else {
    std::shared_ptr<SyncSlaveDB> s_db = sync_slave_dbs_[p_info];  // 获取该从库数据库对象
    // 发送移除从库节点的请求
    s = pika_repl_client_->SendRemoveSlaveNode(s_db->MasterIp(), s_db->MasterPort(), db, s_db->LocalIp());
    if (s.ok()) {
      s_db->SetReplState(ReplState::kDBNoConnect);  // 如果请求成功，设置从库状态为不连接
    }
  }

  // 根据请求结果记录日志
  if (s.ok()) {
    LOG(INFO) << "SlaveNode (" << db << ", stop sync success";  // 如果成功，记录成功日志
  } else {
    LOG(WARNING) << "SlaveNode (" << db << ", stop sync faild, " << s.ToString();  // 如果失败，记录失败日志
  }
  return s;  // 返回请求结果状态
}

// 发送尝试同步的请求
Status PikaReplicaManager::SendTrySyncRequest(const std::string& db_name) {
  BinlogOffset boffset;
  // 获取数据库的binlog偏移量
  if (!g_pika_server->GetDBBinlogOffset(db_name, &boffset)) {
    LOG(WARNING) << "DB: " << db_name << ", Get DB binlog offset failed";  // 如果获取失败，记录警告日志
    return Status::Corruption("DB get binlog offset error");               // 返回错误状态
  }

  std::shared_ptr<SyncSlaveDB> slave_db = GetSyncSlaveDBByName(DBInfo(db_name));  // 获取从库数据库对象
  if (!slave_db) {
    LOG(WARNING) << "Slave DB: " << db_name << ", NotFound";  // 如果从库数据库未找到，记录警告日志
    return Status::Corruption("Slave DB not found");          // 返回错误状态
  }

  // 发送尝试同步请求
  Status status = pika_repl_client_->SendTrySync(slave_db->MasterIp(), slave_db->MasterPort(), db_name, boffset,
                                                 slave_db->LocalIp());

  if (status.ok()) {
    slave_db->SetReplState(ReplState::kWaitReply);  // 如果请求成功，设置从库状态为等待回复
  } else {
    slave_db->SetReplState(ReplState::kError);  // 如果请求失败，设置从库状态为错误
    LOG(WARNING) << "SendDBTrySyncRequest failed " << status.ToString();  // 记录失败日志
  }
  return status;  // 返回请求结果状态
}

// 发送数据库同步请求
Status PikaReplicaManager::SendDBSyncRequest(const std::string& db_name) {
  BinlogOffset boffset;
  // 获取数据库的binlog偏移量
  if (!g_pika_server->GetDBBinlogOffset(db_name, &boffset)) {
    LOG(WARNING) << "DB: " << db_name << ", Get DB binlog offset failed";  // 如果获取失败，记录警告日志
    return Status::Corruption("DB get binlog offset error");               // 返回错误状态
  }

  std::shared_ptr<DB> db = g_pika_server->GetDB(db_name);  // 获取数据库对象
  if (!db) {
    LOG(WARNING) << "DB: " << db_name << " NotFound";  // 如果数据库未找到，记录警告日志
    return Status::Corruption("DB not found");         // 返回错误状态
  }
  db->PrepareRsync();  // 准备进行 rsync 同步

  std::shared_ptr<SyncSlaveDB> slave_db = GetSyncSlaveDBByName(DBInfo(db_name));  // 获取从库数据库对象
  if (!slave_db) {
    LOG(WARNING) << "Slave DB: " << db_name << ", NotFound";  // 如果从库数据库未找到，记录警告日志
    return Status::Corruption("Slave DB not found");          // 返回错误状态
  }

  // 发送数据库同步请求
  Status status = pika_repl_client_->SendDBSync(slave_db->MasterIp(), slave_db->MasterPort(), db_name, boffset,
                                                slave_db->LocalIp());

  Status s;
  if (status.ok()) {
    slave_db->SetReplState(ReplState::kWaitReply);  // 如果请求成功，设置从库状态为等待回复
  } else {
    slave_db->SetReplState(ReplState::kError);                  // 如果请求失败，设置从库状态为错误
    LOG(WARNING) << "SendDBSync failed " << status.ToString();  // 记录失败日志
  }
  if (!s.ok()) {
    LOG(WARNING) << s.ToString();  // 如果同步失败，记录警告日志
  }
  return status;  // 返回请求结果状态
}

// 发送 binlog 同步确认请求，通知从库已成功接收到并处理了指定范围的 binlog
Status PikaReplicaManager::SendBinlogSyncAckRequest(const std::string& db, const LogOffset& ack_start,
                                                    const LogOffset& ack_end, bool is_first_send) {
  // 获取指定数据库的同步从库对象
  std::shared_ptr<SyncSlaveDB> slave_db = GetSyncSlaveDBByName(DBInfo(db));
  if (!slave_db) {                                        // 如果找不到对应的从库
    LOG(WARNING) << "Slave DB: " << db << ":, NotFound";  // 打印警告日志
    return Status::Corruption("Slave DB not found");      // 返回数据库未找到的错误状态
  }

  // 发送 binlog 同步确认请求，包含从库的 IP、端口、数据库名称、ack 开始与结束的偏移量
  return pika_repl_client_->SendBinlogSync(slave_db->MasterIp(), slave_db->MasterPort(), db, ack_start, ack_end,
                                           slave_db->LocalIp(), is_first_send);
}

// 关闭指定 IP 和端口的复制客户端连接
Status PikaReplicaManager::CloseReplClientConn(const std::string& ip, int32_t port) {
  // 调用 pika_repl_client 关闭连接
  return pika_repl_client_->Close(ip, port);
}

// 发送从库 binlog 数据块请求，用于发送指定任务到从库
Status PikaReplicaManager::SendSlaveBinlogChipsRequest(const std::string& ip, int port,
                                                       const std::vector<WriteTask>& tasks) {
  // 调用 pika_repl_server 发送数据块到从库
  return pika_repl_server_->SendSlaveBinlogChips(ip, port, tasks);
}

// 获取指定数据库名称的同步主数据库对象
std::shared_ptr<SyncMasterDB> PikaReplicaManager::GetSyncMasterDBByName(const DBInfo& p_info) {
  std::shared_lock l(dbs_rw_);                                    // 共享锁，防止并发访问修改数据库
  if (sync_master_dbs_.find(p_info) == sync_master_dbs_.end()) {  // 如果找不到指定数据库名称的主库
    return nullptr;                                               // 返回空指针
  }
  return sync_master_dbs_[p_info];  // 返回找到的主库对象
}

// 获取指定数据库名称的同步从数据库对象
std::shared_ptr<SyncSlaveDB> PikaReplicaManager::GetSyncSlaveDBByName(const DBInfo& p_info) {
  std::shared_lock l(dbs_rw_);                                  // 共享锁，防止并发访问修改数据库
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {  // 如果找不到指定数据库名称的从库
    return nullptr;                                             // 返回空指针
  }
  return sync_slave_dbs_[p_info];  // 返回找到的从库对象
}

// 执行同步从库状态机，管理从库的同步状态
Status PikaReplicaManager::RunSyncSlaveDBStateMachine() {
  std::shared_lock l(dbs_rw_);
  for (const auto& item : sync_slave_dbs_) {          // 遍历所有同步从数据库
    DBInfo p_info = item.first;                       // 获取数据库信息
    std::shared_ptr<SyncSlaveDB> s_db = item.second;  // 获取从库对象

    // 根据从库当前的同步状态执行相应的操作
    if (s_db->State() == ReplState::kTryConnect) {  // 尝试连接
      SendTrySyncRequest(p_info.db_name_);
    } else if (s_db->State() == ReplState::kTryDBSync) {  // 尝试进行数据库同步
      SendDBSyncRequest(p_info.db_name_);
    } else if (s_db->State() == ReplState::kWaitReply) {  // 等待回应，跳过
      continue;
    } else if (s_db->State() == ReplState::kWaitDBSync) {  // 等待数据库同步完成
      Status s = s_db->ActivateRsync();                    // 激活 rsync 同步
      if (!s.ok()) {
        LOG(WARNING) << "Slave DB: " << s_db->DBName() << " rsync failed! full synchronization will be retried later";
        continue;
      }

      // 获取数据库对象并尝试更新主库的偏移量
      std::shared_ptr<DB> db = g_pika_server->GetDB(p_info.db_name_);
      if (db) {
        if (s_db->IsRsyncExited()) {  // 如果 rsync 同步退出，尝试更新主库的偏移量
          db->TryUpdateMasterOffset();
        }
      } else {  // 如果数据库没有找到，记录警告日志
        LOG(WARNING) << "DB not found, DB Name: " << p_info.db_name_;
      }
    } else if (s_db->State() == ReplState::kConnected || s_db->State() == ReplState::kNoConnect ||
               s_db->State() == ReplState::kDBNoConnect) {  // 如果是已连接、未连接或数据库未连接状态，跳过
      continue;
    }
  }
  return Status::OK();  // 返回成功状态
}

// 查找所有从库的公共主库地址
void PikaReplicaManager::FindCommonMaster(std::string* master) {
  std::shared_lock l(dbs_rw_);  // 共享锁，防止并发访问修改数据库
  std::string common_master_ip;
  int common_master_port = 0;

  for (auto& iter : sync_slave_dbs_) {         // 遍历所有从库
    if (iter.second->State() != kConnected) {  // 如果从库未连接，返回
      return;
    }
    std::string tmp_ip = iter.second->MasterIp();               // 获取当前从库的主库IP
    int tmp_port = iter.second->MasterPort();                   // 获取当前从库的主库端口
    if (common_master_ip.empty() && common_master_port == 0) {  // 如果当前没有设置公共主库IP和端口
      common_master_ip = tmp_ip;                                // 设置公共主库IP
      common_master_port = tmp_port;                            // 设置公共主库端口
    }
    if (tmp_ip != common_master_ip || tmp_port != common_master_port) {  // 如果找到不同的主库IP或端口，返回
      return;
    }
  }

  // 如果找到了公共主库IP和端口，设置返回的主库地址
  if (!common_master_ip.empty() && common_master_port != 0) {
    *master = common_master_ip + ":" + std::to_string(common_master_port);
  }
}

// 获取当前同步主库和从库的状态信息
void PikaReplicaManager::RmStatus(std::string* info) {
  std::shared_lock l(dbs_rw_);   // 共享锁，防止并发访问修改数据库
  std::stringstream tmp_stream;  // 用于构建状态信息的字符串流
  tmp_stream << "Master DB(" << sync_master_dbs_.size() << "):" << "\r\n";  // 输出主库数量
  for (auto& iter : sync_master_dbs_) {                                     // 遍历所有同步主库
    tmp_stream << " DB " << iter.second->SyncDBInfo().ToString() << "\r\n"
               << iter.second->ToStringStatus() << "\r\n";  // 输出每个主库的状态
  }
  tmp_stream << "Slave DB(" << sync_slave_dbs_.size() << "):" << "\r\n";  // 输出从库数量
  for (auto& iter : sync_slave_dbs_) {                                    // 遍历所有同步从库
    tmp_stream << " DB " << iter.second->SyncDBInfo().ToString() << "\r\n"
               << iter.second->ToStringStatus() << "\r\n";  // 输出每个从库的状态
  }
  info->append(tmp_stream.str());  // 将状态信息附加到传入的字符串中
}
