// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_conn.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <sys/time.h>

#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "pika_inner_message.pb.h"
#include "pstd/include/pstd_string.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

PikaReplClientConn::PikaReplClientConn(int fd, const std::string& ip_port, net::Thread* thread,
                                       void* worker_specific_data, net::NetMultiplexer* mpx)
    : net::PbConn(fd, ip_port, thread, mpx) {}

bool PikaReplClientConn::IsDBStructConsistent(const std::vector<DBStruct>& current_dbs,
                                              const std::vector<DBStruct>& expect_dbs) {
  if (current_dbs.size() != expect_dbs.size()) {
    return false;
  }
  for (const auto& db_struct : current_dbs) {
    if (find(expect_dbs.begin(), expect_dbs.end(), db_struct) == expect_dbs.end()) {
      LOG(WARNING) << "DB struct mismatch";
      return false;
    }
  }
  return true;
}

// 处理从客户端接收到的消息，解析并根据消息类型分发到不同的处理方法
int PikaReplClientConn::DealMessage() {
  // 创建一个响应对象，用于解析接收到的消息
  std::shared_ptr<InnerMessage::InnerResponse> response = std::make_shared<InnerMessage::InnerResponse>();

  // 从接收缓冲区构造一个 CodedInputStream，用于解析消息
  ::google::protobuf::io::ArrayInputStream input(rbuf_ + cur_pos_ - header_len_, static_cast<int32_t>(header_len_));
  ::google::protobuf::io::CodedInputStream decoder(&input);

  // 设置消息的最大字节限制，防止过大的消息导致内存溢出
  decoder.SetTotalBytesLimit(g_pika_conf->max_conn_rbuf_size());

  // 解析消息，如果解析失败，记录日志并返回错误
  bool success = response->ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
  if (!success) {
    LOG(WARNING) << "ParseFromArray FAILED! "
                 << " msg_len: " << header_len_;
    g_pika_server->SyncError();  // 调用同步错误处理
    return -1;                   // 返回错误，表示消息解析失败
  }

  // 根据响应类型分发不同的处理逻辑
  switch (response->type()) {
    case InnerMessage::kMetaSync: {  // 如果是 MetaSync 类型的消息
      // 创建任务参数，并将任务提交到后台任务队列，调用 HandleMetaSyncResponse 处理
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleMetaSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kDBSync: {  // 如果是 DBSync 类型的消息
      // 创建任务参数，并将任务提交到后台任务队列，调用 HandleDBSyncResponse 处理
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleDBSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kTrySync: {  // 如果是 TrySync 类型的消息
      const std::string& db_name = response->try_sync().slot().db_name();

      // TrySync 响应必须包含 db_name，确保 db_name 不为空
      assert(!db_name.empty());

      // 创建任务参数，并将任务提交到后台任务队列，调用 HandleTrySyncResponse 处理
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTaskByDBName(&PikaReplClientConn::HandleTrySyncResponse,
                                                  static_cast<void*>(task_arg), db_name);
      break;
    }
    case InnerMessage::kBinlogSync: {  // 如果是 BinlogSync 类型的消息
      // 调用 DispatchBinlogRes 方法来处理 BinlogSync 响应
      DispatchBinlogRes(response);
      break;
    }
    case InnerMessage::kRemoveSlaveNode: {  // 如果是 RemoveSlaveNode 类型的消息
      // 创建任务参数，并将任务提交到后台任务队列，调用 HandleRemoveSlaveNodeResponse 处理
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleRemoveSlaveNodeResponse,
                                          static_cast<void*>(task_arg));
      break;
    }
    default:
      break;  // 对于未知的消息类型，不做任何处理
  }

  return 0;  // 返回 0，表示消息处理成功
}

// 处理元数据同步（MetaSync）请求的响应
void PikaReplClientConn::HandleMetaSyncResponse(void* arg) {
  // 将任务参数封装为智能指针，确保内存自动释放
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;                     // 客户端连接
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;  // MetaSync 响应消息

  // 如果响应的状态是其他（kOther），则继续发送 MetaSync 请求
  if (response->code() == InnerMessage::kOther) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "Meta Sync Failed: " << reply << " will keep sending MetaSync msg";  // 记录警告日志
    return;
  }

  // 如果响应的状态不是 kOk，表示出现错误
  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "Meta Sync Failed: " << reply;  // 记录警告日志
    g_pika_server->SyncError();                     // 通知同步错误
    conn->NotifyClose();                            // 关闭连接
    return;
  }

  // 获取 MetaSync 响应中的元数据结构
  const InnerMessage::InnerResponse_MetaSync meta_sync = response->meta_sync();

  // 解析主库的数据库结构信息
  std::vector<DBStruct> master_db_structs;
  for (int idx = 0; idx < meta_sync.dbs_info_size(); ++idx) {
    const InnerMessage::InnerResponse_MetaSync_DBInfo& db_info = meta_sync.dbs_info(idx);
    master_db_structs.push_back({db_info.db_name(), db_info.db_instance_num()});
  }

  // 获取本地的数据库结构信息
  std::vector<DBStruct> self_db_structs = g_pika_conf->db_structs();

  // 比较本地数据库结构和主库的数据库结构是否一致
  if (!PikaReplClientConn::IsDBStructConsistent(self_db_structs, master_db_structs)) {
    LOG(WARNING) << "Self db structs(number of databases: " << self_db_structs.size()
                 << ") inconsistent with master(number of databases: " << master_db_structs.size()
                 << "), failed to establish master-slave relationship";  // 记录警告日志
    g_pika_server->SyncError();                                          // 通知同步错误
    conn->NotifyClose();                                                 // 关闭连接
    return;
  }

  // 检查获取到的 replication_id 是否为空
  if (meta_sync.replication_id() == "") {
    LOG(WARNING)
        << "Meta Sync Failed: the replicationid obtained from the server is null, keep sending MetaSync msg";  // 记录警告日志
    return;
  }

  // 检查本地和远程的 replication_id 是否一致
  if (g_pika_conf->replication_id() != meta_sync.replication_id() && g_pika_conf->replication_id() != "") {
    LOG(WARNING) << "Meta Sync Failed: replicationid on both sides of the connection are inconsistent";  // 记录警告日志
    g_pika_server->SyncError();  // 通知同步错误
    conn->NotifyClose();         // 关闭连接
    return;
  }

  // 如果本地和远程的 replication_id 不一致，表示这是首次同步
  if (g_pika_conf->replication_id() != meta_sync.replication_id()) {
    LOG(INFO) << "New node is added to the cluster and requires full replication, remote replication id: "
              << meta_sync.replication_id()
              << ", local replication id: " << g_pika_conf->replication_id();  // 记录信息日志
    g_pika_server->force_full_sync_ = true;                                    // 强制全量同步
    g_pika_conf->SetReplicationID(meta_sync.replication_id());                 // 更新本地 replication_id
    g_pika_conf->ConfigRewriteReplicationID();  // 重写配置文件中的 replication_id
  }

  // 启用写入 binlog 配置
  g_pika_conf->SetWriteBinlog("yes");

  // 准备数据库进行尝试同步
  g_pika_server->PrepareDBTrySync();

  // 完成 MetaSync 响应处理
  g_pika_server->FinishMetaSync();
  LOG(INFO) << "Finish to handle meta sync response";  // 记录信息日志，表示 MetaSync 响应处理完成
}

// 处理全量同步（DBSync）请求的响应
void PikaReplClientConn::HandleDBSyncResponse(void* arg) {
  // 将任务参数封装为智能指针，确保内存自动释放
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;                     // 客户端连接
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;  // DBSync 响应消息

  // 提取 DBSync 响应中的具体内容
  const InnerMessage::InnerResponse_DBSync db_sync_response = response->db_sync();
  int32_t session_id = db_sync_response.session_id();               // 获取主库会话 ID
  const InnerMessage::Slot& db_response = db_sync_response.slot();  // 获取响应中的数据库槽信息
  const std::string& db_name = db_response.db_name();               // 获取数据库名称

  // 获取从库（Slave DB）对象
  std::shared_ptr<SyncSlaveDB> slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    // 如果从库对象不存在，记录警告日志并返回
    LOG(WARNING) << "Slave DB: " << db_name << " Not Found";
    return;
  }

  // 如果响应状态码不是 kOk，处理错误逻辑
  if (response->code() != InnerMessage::kOk) {
    // 更新从库状态为错误
    slave_db->SetReplState(ReplState::kError);
    // 记录错误消息（如果存在）
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "DBSync Failed: " << reply;
    return;
  }

  // 设置从库的主库会话 ID
  slave_db->SetMasterSessionId(session_id);

  // 停止从库的 Rsync 服务，准备接收全量同步
  slave_db->StopRsync();

  // 更新从库状态为等待全量同步（WaitDBSync）
  slave_db->SetReplState(ReplState::kWaitDBSync);
  LOG(INFO) << "DB: " << db_name << " Need Wait To Sync";  // 记录日志，等待同步开始

  // 全量同步启动时，增加未完成的全量同步计数（用于内部监控）
  g_pika_conf->AddInternalUsedUnfinishedFullSync(slave_db->DBName());
}

// 处理主从同步（TrySync）请求的响应
void PikaReplClientConn::HandleTrySyncResponse(void* arg) {
  // 将任务参数封装为智能指针，确保内存自动释放
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;                     // 与客户端的连接
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;  // TrySync 响应消息

  // 如果响应中状态码不是 kOk，记录失败日志并返回
  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "TrySync Failed: " << reply;
    return;
  }

  // 获取 TrySync 响应内容和对应的数据库信息
  const InnerMessage::InnerResponse_TrySync& try_sync_response = response->try_sync();
  const InnerMessage::Slot& db_response = try_sync_response.slot();
  std::string db_name = db_response.db_name();

  // 查找对应的主库（Master）对象
  std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!db) {
    LOG(WARNING) << "DB: " << db_name << " Not Found";  // 主库不存在，记录警告日志
    return;
  }

  // 查找对应的从库（Slave）对象
  std::shared_ptr<SyncSlaveDB> slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    LOG(WARNING) << "DB: " << db_name << " Not Found";  // 从库不存在，记录警告日志
    return;
  }

  // 初始化逻辑偏移量
  LogicOffset logic_last_offset;

  // 根据 TrySync 响应中的 reply_code 进行不同处理
  if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
    // 主从同步成功
    BinlogOffset boffset;
    int32_t session_id = try_sync_response.session_id();                 // 获取主库会话 ID
    db->Logger()->GetProducerStatus(&boffset.filenum, &boffset.offset);  // 获取主库的当前 Binlog 文件和偏移量

    // 设置从库的主库会话 ID
    slave_db->SetMasterSessionId(session_id);

    // 构造同步偏移量（包含 Binlog 偏移和逻辑偏移）
    LogOffset offset(boffset, logic_last_offset);

    // 向主库发送同步确认（ACK）请求，表示从库已连接
    g_pika_rm->SendBinlogSyncAckRequest(db_name, offset, offset, true);

    // 更新从库状态为已连接
    slave_db->SetReplState(ReplState::kConnected);

    // 更新从库最近接收时间，避免连接超时
    slave_db->SetLastRecvTime(pstd::NowMicros());

    LOG(INFO) << "DB: " << db_name << " TrySync Ok";  // 记录成功日志

  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
    // 主库同步点已被清理
    slave_db->SetReplState(ReplState::kTryDBSync);            // 更新从库状态为需要全量同步
    LOG(INFO) << "DB: " << db_name << " Need To Try DBSync";  // 记录日志，提示需要全量同步

  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointLarger) {
    // 同步点无效（从库的偏移量比主库的更大）
    slave_db->SetReplState(ReplState::kError);  // 更新从库状态为错误
    LOG(WARNING) << "DB: " << db_name << " TrySync Error, Because the invalid filenum and offset";  // 记录错误日志

  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
    // 通用错误处理
    slave_db->SetReplState(ReplState::kError);              // 更新从库状态为错误
    LOG(WARNING) << "DB: " << db_name << " TrySync Error";  // 记录错误日志
  }
}

// 处理从节点发送的 Binlog 同步响应，将任务分发到对应的从库处理
void PikaReplClientConn::DispatchBinlogRes(const std::shared_ptr<InnerMessage::InnerResponse>& res) {
  // 用于存储每个数据库对应的 Binlog 索引集合
  std::unordered_map<DBInfo, std::vector<int>*, hash_db_info> par_binlog;

  // 遍历响应中的 Binlog 同步项，将它们按数据库分类
  for (int i = 0; i < res->binlog_sync_size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync(i);
    // 提取数据库信息（DB 名称作为 key）
    DBInfo p_info(binlog_res.slot().db_name());
    // 如果当前数据库还未添加到哈希表，则初始化对应的 Binlog 索引列表
    if (par_binlog.find(p_info) == par_binlog.end()) {
      par_binlog[p_info] = new std::vector<int>();
    }
    // 将当前 Binlog 索引加入对应数据库的索引列表
    par_binlog[p_info]->push_back(i);
  }

  std::shared_ptr<SyncSlaveDB> slave_db;

  // 遍历每个数据库及其对应的 Binlog 索引集合
  for (auto& binlog_nums : par_binlog) {
    // 根据数据库名称构造从节点信息
    RmNode node(binlog_nums.first.db_name_);

    // 获取对应的从库对象
    slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(binlog_nums.first.db_name_));
    if (!slave_db) {
      // 如果从库不存在，则记录警告日志并跳过
      LOG(WARNING) << "Slave DB: " << binlog_nums.first.db_name_ << " not exist";
      break;
    }

    // 更新从库最近接收时间为当前时间
    slave_db->SetLastRecvTime(pstd::NowMicros());

    // 调度写 Binlog 的任务到对应从库的后台线程
    g_pika_rm->ScheduleWriteBinlogTask(binlog_nums.first.db_name_, res,
                                       std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()),
                                       reinterpret_cast<void*>(binlog_nums.second));
  }
}

// 处理从节点移除的响应结果
void PikaReplClientConn::HandleRemoveSlaveNodeResponse(void* arg) {
  // 将传入的任务参数封装为智能指针，确保内存自动释放
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  // 检查响应状态码，如果不是 kOk，则表示移除从节点失败
  if (response->code() != InnerMessage::kOk) {
    // 如果响应中包含详细的错误信息，则记录警告日志
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "Remove slave node Failed: " << reply;
    return;
  }

  // 如果响应成功，无需额外处理（可以扩展逻辑）
}
