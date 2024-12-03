// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
// PikaReplServerConn 构造函数：初始化 PbConn 基类并设置连接相关信息
PikaReplServerConn::PikaReplServerConn(int fd, const std::string& ip_port, net::Thread* thread,
                                       void* worker_specific_data, net::NetMultiplexer* mpx)
    : PbConn(fd, ip_port, thread, mpx) {}

// PikaReplServerConn 析构函数：使用默认析构函数
PikaReplServerConn::~PikaReplServerConn() = default;

// 处理 MetaSync 请求
void PikaReplServerConn::HandleMetaSyncRequest(void* arg) {
  // 将请求参数转为 ReplServerTaskArg 类型的智能指针
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));

  // 获取请求和连接对象
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  // 获取 MetaSync 请求中的节点信息和认证信息
  InnerMessage::InnerRequest::MetaSync meta_sync_request = req->meta_sync();
  const InnerMessage::Node& node = meta_sync_request.node();
  std::string masterauth = meta_sync_request.has_auth() ? meta_sync_request.auth() : "";

  // 创建并初始化响应对象
  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::kMetaSync);

  // 如果配置了密码且密码验证失败，返回认证错误
  if (!g_pika_conf->requirepass().empty() && g_pika_conf->requirepass() != masterauth) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Auth with master error, Invalid masterauth");
  } else {
    // 认证通过，记录日志并处理 Slave 节点的添加
    LOG(INFO) << "Receive MetaSync, Slave ip: " << node.ip() << ", Slave port:" << node.port();

    // 获取数据库结构信息并尝试添加 Slave
    std::vector<DBStruct> db_structs = g_pika_conf->db_structs();
    bool success = g_pika_server->TryAddSlave(node.ip(), node.port(), conn->fd(), db_structs);

    // 更新客户端连接映射
    const std::string ip_port = pstd::IpPortString(node.ip(), node.port());
    g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());

    // 如果 Slave 添加失败，返回错误信息；如果成功，进行 Master 切换
    if (!success) {
      response.set_code(InnerMessage::kOther);
      response.set_reply("Slave AlreadyExist");
    } else {
      g_pika_server->BecomeMaster();  // 切换为 Master
      response.set_code(InnerMessage::kOk);

      // 设置 MetaSync 响应中的其他信息
      InnerMessage::InnerResponse_MetaSync* meta_sync = response.mutable_meta_sync();
      if (g_pika_conf->replication_id() == "") {
        // 如果没有设置复制 ID，生成一个新的随机复制 ID
        std::string replication_id = pstd::getRandomHexChars(configReplicationIDSize);
        g_pika_conf->SetReplicationID(replication_id);
        g_pika_conf->ConfigRewriteReplicationID();  // 写入配置文件
      }

      // 填充 MetaSync 响应中的复制信息
      meta_sync->set_classic_mode(g_pika_conf->classic_mode());
      meta_sync->set_run_id(g_pika_conf->run_id());
      meta_sync->set_replication_id(g_pika_conf->replication_id());

      // 添加数据库信息到 MetaSync 响应
      for (const auto& db_struct : db_structs) {
        InnerMessage::InnerResponse_MetaSync_DBInfo* db_info = meta_sync->add_dbs_info();
        db_info->set_db_name(db_struct.db_name);

        // 兼容旧版本的 slot_num 设置
        db_info->set_slot_num(1);                                 // 默认设置 slot_num 为 1
        db_info->set_db_instance_num(db_struct.db_instance_num);  // 设置数据库实例数量
      }
    }
  }

  // 序列化响应消息并发送给客户端
  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    // 序列化或写入失败时记录警告并关闭连接
    LOG(WARNING) << "Process MetaSync request serialization failed";
    conn->NotifyClose();  // 通知关闭连接
    return;
  }

  // 通知客户端写操作已完成
  conn->NotifyWrite();
}
// 处理 TrySync 请求，用于从节点向主节点发起同步请求
void PikaReplServerConn::HandleTrySyncRequest(void* arg) {
  // 将请求参数转为 ReplServerTaskArg 类型的智能指针
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));

  // 获取请求和连接对象
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  // 从请求中提取 TrySync 请求的相关信息
  InnerMessage::InnerRequest::TrySync try_sync_request = req->try_sync();
  const InnerMessage::Slot& db_request = try_sync_request.slot();
  const InnerMessage::BinlogOffset& slave_boffset = try_sync_request.binlog_offset();
  const InnerMessage::Node& node = try_sync_request.node();
  std::string db_name = db_request.db_name();

  // 创建响应对象并初始化
  InnerMessage::InnerResponse response;
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);  // 默认设置为错误响应
  InnerMessage::Slot* db_response = try_sync_response->mutable_slot();
  db_response->set_db_name(db_name);

  // 兼容旧版本的 slot_id 设置
  db_response->set_slot_id(0);  // 默认设置 slot_id 为 0

  bool pre_success = true;                          // 标志位，判断数据库是否成功找到
  response.set_type(InnerMessage::Type::kTrySync);  // 设置响应类型为 TrySync

  // 根据数据库名称获取同步主数据库
  std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!db) {
    // 如果找不到数据库，返回错误
    response.set_code(InnerMessage::kError);
    response.set_reply("DB not found");
    LOG(WARNING) << "DB Name: " << db_name << " Not Found, TrySync Error";
    pre_success = false;  // 标记数据库查找失败
  } else {
    // 找到数据库，记录日志
    LOG(INFO) << "Receive Trysync, Slave ip: " << node.ip() << ", Slave port:" << node.port() << ", DB: " << db_name
              << ", filenum: " << slave_boffset.filenum() << ", pro_offset: " << slave_boffset.offset();
    response.set_code(InnerMessage::kOk);  // 设置响应状态为 OK
  }

  // 如果数据库查找成功，并且同步偏移检查通过，则更新从节点信息
  if (pre_success && TrySyncOffsetCheck(db, try_sync_request, try_sync_response)) {
    TrySyncUpdateSlaveNode(db, try_sync_request, conn, try_sync_response);  // 更新从节点信息
  }

  // 序列化响应并发送给客户端
  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    // 序列化或写入失败时记录警告并关闭连接
    LOG(WARNING) << "Handle Try Sync Failed";
    conn->NotifyClose();  // 通知关闭连接
    return;
  }

  // 通知客户端写操作已完成
  conn->NotifyWrite();
}
// 尝试同步并更新从节点信息
bool PikaReplServerConn::TrySyncUpdateSlaveNode(const std::shared_ptr<SyncMasterDB>& db,
                                                const InnerMessage::InnerRequest::TrySync& try_sync_request,
                                                const std::shared_ptr<net::PbConn>& conn,
                                                InnerMessage::InnerResponse::TrySync* try_sync_response) {
  const InnerMessage::Node& node = try_sync_request.node();

  // 检查该从节点是否已经存在
  if (!db->CheckSlaveNodeExist(node.ip(), node.port())) {
    // 如果从节点不存在，生成新的 Session ID
    int32_t session_id = db->GenSessionId();
    if (session_id == -1) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);  // 错误响应
      LOG(WARNING) << "DB: " << db->DBName() << ", Gen Session id Failed";
      return false;
    }

    try_sync_response->set_session_id(session_id);  // 设置返回的 Session ID

    // 尝试将该从节点添加到数据库
    Status s = db->AddSlaveNode(node.ip(), node.port(), session_id);
    if (!s.ok()) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);  // 错误响应
      LOG(WARNING) << "DB: " << db->DBName() << " TrySync Failed, " << s.ToString();
      return false;
    }

    // 更新客户端连接映射
    const std::string ip_port = pstd::IpPortString(node.ip(), node.port());
    g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());

    // 成功添加从节点，返回成功响应
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
    LOG(INFO) << "DB: " << db->DBName() << " TrySync Success, Session: " << session_id;
  } else {
    // 如果从节点已存在，获取其 Session ID
    int32_t session_id;
    Status s = db->GetSlaveNodeSession(node.ip(), node.port(), &session_id);
    if (!s.ok()) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);  // 错误响应
      LOG(WARNING) << "DB: " << db->DBName() << " Get Session id Failed" << s.ToString();
      return false;
    }

    // 设置 Session ID 并返回成功响应
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
    try_sync_response->set_session_id(session_id);
    LOG(INFO) << "DB: " << db->DBName() << " TrySync Success, Session: " << session_id;
  }

  return true;  // 成功
}

// 检查从节点的 binlog 偏移量是否合法
bool PikaReplServerConn::TrySyncOffsetCheck(const std::shared_ptr<SyncMasterDB>& db,
                                            const InnerMessage::InnerRequest::TrySync& try_sync_request,
                                            InnerMessage::InnerResponse::TrySync* try_sync_response) {
  const InnerMessage::Node& node = try_sync_request.node();
  const InnerMessage::BinlogOffset& slave_boffset = try_sync_request.binlog_offset();  // 从节点的 binlog 偏移量
  std::string db_name = db->DBName();                                                  // 数据库名称

  // 获取当前主节点的 binlog 偏移量
  BinlogOffset boffset;
  Status s = db->Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    // 获取失败，返回错误
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Handle TrySync, DB: " << db_name << " Get binlog offset error, TrySync failed";
    return false;
  }

  // 设置主节点的 binlog 偏移量
  InnerMessage::BinlogOffset* master_db_boffset = try_sync_response->mutable_binlog_offset();
  master_db_boffset->set_filenum(boffset.filenum);
  master_db_boffset->set_offset(boffset.offset);

  // 比较从节点请求的偏移量和主节点的偏移量
  if (boffset.filenum < slave_boffset.filenum() ||
      (boffset.filenum == slave_boffset.filenum() && boffset.offset < slave_boffset.offset())) {
    // 从节点的偏移量比主节点大，返回同步点更大错误
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointLarger);
    LOG(WARNING) << "Slave offset is larger than mine, Slave ip: " << node.ip() << ", Slave port: " << node.port()
                 << ", DB: " << db_name << ", slave filenum: " << slave_boffset.filenum()
                 << ", slave pro_offset_: " << slave_boffset.offset() << ", local filenum: " << boffset.filenum
                 << ", local pro_offset_: " << boffset.offset;
    return false;
  }

  // 检查从节点请求的 binlog 是否已经被清除（即需要完整同步）
  std::string confile = NewFileName(db->Logger()->filename(), slave_boffset.filenum());
  if (!pstd::FileExists(confile)) {
    LOG(INFO) << "DB: " << db_name << " binlog has been purged, may need full sync";
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
    return false;
  }

  // 创建 binlog 读取器并定位到从节点请求的偏移量
  PikaBinlogReader reader;
  reader.Seek(db->Logger(), slave_boffset.filenum(), slave_boffset.offset());
  BinlogOffset seeked_offset;
  reader.GetReaderStatus(&(seeked_offset.filenum), &(seeked_offset.offset));

  // 检查从节点的偏移量是否为当前日志的起始点
  if (seeked_offset.filenum != slave_boffset.filenum() || seeked_offset.offset != slave_boffset.offset()) {
    // 偏移量不匹配，返回错误
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Slave offset is not a start point of cur log, Slave ip: " << node.ip()
                 << ", Slave port: " << node.port() << ", DB: " << db_name
                 << " closest start point, filenum: " << seeked_offset.filenum << ", offset: " << seeked_offset.offset;
    return false;
  }

  return true;  // 偏移量有效，返回成功
}
// 处理数据库同步请求
void PikaReplServerConn::HandleDBSyncRequest(void* arg) {
  // 创建任务参数并从中提取请求和连接对象
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  // 从请求中提取 DBSync 信息
  InnerMessage::InnerRequest::DBSync db_sync_request = req->db_sync();
  const InnerMessage::Slot& db_request = db_sync_request.slot();
  const InnerMessage::Node& node = db_sync_request.node();
  const InnerMessage::BinlogOffset& slave_boffset = db_sync_request.binlog_offset();
  std::string db_name = db_request.db_name();

  // 设置响应初始状态
  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::kOk);            // 默认为成功
  response.set_type(InnerMessage::Type::kDBSync);  // 响应类型为 DBSync
  InnerMessage::InnerResponse::DBSync* db_sync_response = response.mutable_db_sync();
  InnerMessage::Slot* db_response = db_sync_response->mutable_slot();
  db_response->set_db_name(db_name);

  // 为了兼容旧版本，设置 slot_id 为 0（slot_id 并未使用）
  db_response->set_slot_id(0);

  // 打印日志
  LOG(INFO) << "Handle DBSync Request";
  bool prior_success = true;

  // 获取主数据库实例
  std::shared_ptr<SyncMasterDB> master_db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!master_db) {
    // 如果没有找到主数据库，设置失败状态并返回
    LOG(WARNING) << "Sync Master DB: " << db_name << ", NotFound";
    prior_success = false;
    response.set_code(InnerMessage::kError);
  }

  if (prior_success) {
    // 如果数据库存在，检查从节点是否已经存在
    if (!master_db->CheckSlaveNodeExist(node.ip(), node.port())) {
      // 如果从节点不存在，生成新的 session id
      int32_t session_id = master_db->GenSessionId();
      db_sync_response->set_session_id(session_id);

      if (session_id == -1) {
        // 生成 session id 失败，设置错误状态并返回
        response.set_code(InnerMessage::kError);
        LOG(WARNING) << "DB: " << db_name << ", Gen Session id Failed";
      } else {
        // 添加从节点并更新连接映射
        Status s = master_db->AddSlaveNode(node.ip(), node.port(), session_id);
        if (s.ok()) {
          const std::string ip_port = pstd::IpPortString(node.ip(), node.port());
          g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());
          LOG(INFO) << "DB: " << db_name << " Handle DBSync Request Success, Session: " << session_id;
        } else {
          // 添加从节点失败，设置错误状态并返回
          response.set_code(InnerMessage::kError);
          LOG(WARNING) << "DB: " << db_name << " Handle DBSync Request Failed, " << s.ToString();
        }
      }
    } else {
      // 如果从节点已存在，获取现有的 session id
      int32_t session_id = 0;
      Status s = master_db->GetSlaveNodeSession(node.ip(), node.port(), &session_id);
      if (!s.ok()) {
        // 获取 session id 失败，返回错误
        response.set_code(InnerMessage::kError);
        db_sync_response->set_session_id(-1);
        LOG(WARNING) << "DB: " << db_name << ", Get Session id Failed" << s.ToString();
      } else {
        // 返回获取到的 session id
        db_sync_response->set_session_id(session_id);
        LOG(INFO) << "DB: " << db_name << " Handle DBSync Request Success, Session: " << session_id;
      }
    }
  }

  // 将从节点的状态设置为 kSlaveDbSync，以确保 binlog 被保留
  // 详见 SyncMasterSlot::BinlogCloudPurge
  master_db->ActivateSlaveDbSync(node.ip(), node.port());

  // 尝试开始数据库同步
  g_pika_server->TryDBSync(node.ip(), node.port() + kPortShiftRSync, db_name,
                           static_cast<int32_t>(slave_boffset.filenum()));

  // 序列化响应并发送给客户端
  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    // 如果序列化失败或发送失败，关闭连接并返回
    LOG(WARNING) << "Handle DBSync Failed";
    conn->NotifyClose();
    return;
  }

  // 通知连接写入成功
  conn->NotifyWrite();
}
// 处理 Binlog 同步请求
void PikaReplServerConn::HandleBinlogSyncRequest(void* arg) {
  // 创建任务参数并从中提取请求和连接对象
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  // 如果请求中没有 binlog_sync 信息，则报错并返回
  if (!req->has_binlog_sync()) {
    LOG(WARNING) << "Pb parse error";  // protobuf 解析错误
    return;
  }

  // 从请求中提取 binlog_sync 信息
  const InnerMessage::InnerRequest::BinlogSync& binlog_req = req->binlog_sync();
  const InnerMessage::Node& node = binlog_req.node();  // 从节点信息
  const std::string& db_name = binlog_req.db_name();   // 数据库名称

  bool is_first_send = binlog_req.first_send();                                      // 是否是第一次发送
  int32_t session_id = binlog_req.session_id();                                      // 会话 ID
  const InnerMessage::BinlogOffset& ack_range_start = binlog_req.ack_range_start();  // binlog 起始偏移量
  const InnerMessage::BinlogOffset& ack_range_end = binlog_req.ack_range_end();      // binlog 结束偏移量

  // 将 binlog 偏移量转化为更高层次的格式
  BinlogOffset b_range_start(ack_range_start.filenum(), ack_range_start.offset());
  BinlogOffset b_range_end(ack_range_end.filenum(), ack_range_end.offset());
  LogicOffset l_range_start(ack_range_start.term(), ack_range_start.index());
  LogicOffset l_range_end(ack_range_end.term(), ack_range_end.index());
  LogOffset range_start(b_range_start, l_range_start);  // 完整的起始偏移量
  LogOffset range_end(b_range_end, l_range_end);        // 完整的结束偏移量

  // 获取主数据库实例
  std::shared_ptr<SyncMasterDB> master_db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!master_db) {
    // 如果主数据库未找到，记录警告并返回
    LOG(WARNING) << "Sync Master DB: " << db_name << ", NotFound";
    return;
  }

  // 检查从节点的 session_id 是否有效
  if (!master_db->CheckSessionId(node.ip(), node.port(), db_name, session_id)) {
    LOG(WARNING) << "Check Session failed " << node.ip() << ":" << node.port() << ", " << db_name;
    return;
  }

  // 设置从节点的 ACK 信息
  RmNode slave_node = RmNode(node.ip(), node.port(), db_name);

  // 更新主数据库接收时间
  Status s = master_db->SetLastRecvTime(node.ip(), node.port(), pstd::NowMicros());
  if (!s.ok()) {
    // 如果更新接收时间失败，记录警告并关闭连接
    LOG(WARNING) << "SetMasterLastRecvTime failed " << node.ip() << ":" << node.port() << ", " << db_name << " "
                 << s.ToString();
    conn->NotifyClose();
    return;
  }

  // 如果是第一次发送 Binlog 同步请求，检查参数并激活 Binlog 同步
  if (is_first_send) {
    if (range_start.b_offset != range_end.b_offset) {
      // 如果起始和结束偏移量不一致，参数无效
      LOG(WARNING) << "first binlogsync request pb argument invalid";
      conn->NotifyClose();
      return;
    }

    // 激活从节点的 Binlog 同步
    Status s = master_db->ActivateSlaveBinlogSync(node.ip(), node.port(), range_start);
    if (!s.ok()) {
      // 如果激活同步失败，记录警告并关闭连接
      LOG(WARNING) << "Activate Binlog Sync failed " << slave_node.ToString() << " " << s.ToString();
      conn->NotifyClose();
      return;
    }
    return;
  }

  // 如果不是第一次发送，并且起始和结束偏移量为 0，则不处理该请求（相当于 ping）
  if (range_start.b_offset == BinlogOffset() && range_end.b_offset == BinlogOffset()) {
    return;
  }

  // 更新 binlog 同步状态
  s = g_pika_rm->UpdateSyncBinlogStatus(slave_node, range_start, range_end);
  if (!s.ok()) {
    // 如果更新 binlog 同步状态失败，记录警告并关闭连接
    LOG(WARNING) << "Update binlog ack failed " << db_name << " " << s.ToString();
    conn->NotifyClose();
    return;
  }

  // 发出信号，通知辅助线程
  g_pika_server->SignalAuxiliary();
}
// 处理移除从节点请求
void PikaReplServerConn::HandleRemoveSlaveNodeRequest(void* arg) {
  // 创建任务参数并从中提取请求和连接对象
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  // 如果请求中没有有效的从节点信息，返回错误
  if (req->remove_slave_node_size() == 0) {
    LOG(WARNING) << "Pb parse error";  // protobuf 解析错误
    conn->NotifyClose();               // 关闭连接
    return;
  }

  // 从请求中提取移除从节点的相关信息
  const InnerMessage::InnerRequest::RemoveSlaveNode& remove_slave_node_req = req->remove_slave_node(0);
  const InnerMessage::Node& node = remove_slave_node_req.node();  // 从节点信息
  const InnerMessage::Slot& slot = remove_slave_node_req.slot();  // 数据库槽位信息

  std::string db_name = slot.db_name();  // 获取数据库名称

  // 获取主数据库实例
  std::shared_ptr<SyncMasterDB> master_db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!master_db) {
    // 如果找不到主数据库，记录警告并返回
    LOG(WARNING) << "Sync Master DB: " << db_name << ", NotFound";
  }

  // 移除从节点
  Status s = master_db->RemoveSlaveNode(node.ip(), node.port());

  // 创建响应消息
  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::kOk);                     // 设置响应码为成功
  response.set_type(InnerMessage::Type::kRemoveSlaveNode);  // 设置响应类型为移除从节点

  // 设置返回的从节点信息
  InnerMessage::InnerResponse::RemoveSlaveNode* remove_slave_node_response = response.add_remove_slave_node();
  InnerMessage::Slot* db_response = remove_slave_node_response->mutable_slot();
  db_response->set_db_name(db_name);  // 设置数据库名称
  /*
   * slot_id 设置为 0，这是为了兼容旧版本，
   * 因为 slot_id 在新的实现中未被使用
   */
  db_response->set_slot_id(0);
  InnerMessage::Node* node_response = remove_slave_node_response->mutable_node();
  node_response->set_ip(g_pika_server->host());    // 设置当前服务器 IP
  node_response->set_port(g_pika_server->port());  // 设置当前服务器端口

  // 序列化响应并发送给客户端
  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Remove Slave Node Failed";  // 发送响应失败，记录警告
    conn->NotifyClose();                         // 关闭连接
    return;
  }
  conn->NotifyWrite();  // 通知写操作成功
}

// 处理接收到的消息并根据消息类型调度不同的请求处理函数
int PikaReplServerConn::DealMessage() {
  // 创建一个 InnerRequest 请求对象，并从接收到的数据中解析
  std::shared_ptr<InnerMessage::InnerRequest> req = std::make_shared<InnerMessage::InnerRequest>();
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, static_cast<int32_t>(header_len_));

  // 如果解析失败，记录警告并返回错误
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }

  // 根据请求类型分配相应的任务处理函数
  switch (req->type()) {
    case InnerMessage::kMetaSync: {
      // 元数据同步请求，调度相应处理函数
      auto task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleMetaSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kTrySync: {
      // 尝试同步请求，调度相应处理函数
      auto task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleTrySyncRequest, task_arg);
      break;
    }
    case InnerMessage::kDBSync: {
      // 数据库同步请求，调度相应处理函数
      auto task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleDBSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kBinlogSync: {
      // Binlog 同步请求，调度相应处理函数
      auto task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleBinlogSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kRemoveSlaveNode: {
      // 移除从节点请求，调度相应处理函数
      auto task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleRemoveSlaveNodeRequest, task_arg);
      break;
    }
    default:
      break;
  }

  return 0;  // 返回 0 表示成功处理请求
}
