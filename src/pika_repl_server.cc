// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server.h"

#include <glog/logging.h>

#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;

// 全局 PikaServer 和 PikaReplicaManager 实例
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

// PikaReplServer 构造函数，初始化服务器线程池和复制线程
PikaReplServer::PikaReplServer(const std::set<std::string>& ips, int port, int cron_interval) {
  // 创建一个线程池，指定线程数和任务队列大小
  server_tp_ = std::make_unique<net::ThreadPool>(PIKA_REPL_SERVER_TP_SIZE, 100000, "PikaReplServer");

  // 创建 PikaReplServerThread，负责处理复制操作
  pika_repl_server_thread_ = std::make_unique<PikaReplServerThread>(ips, port, cron_interval);
  // 设置线程名称
  pika_repl_server_thread_->set_thread_name("PikaReplServer");
}

// PikaReplServer 析构函数，输出日志表示退出
PikaReplServer::~PikaReplServer() { LOG(INFO) << "PikaReplServer exit!!!"; }

// 启动 PikaReplServer 线程和线程池
int PikaReplServer::Start() {
  // 设置线程名称
  pika_repl_server_thread_->set_thread_name("PikaReplServer");

  // 启动 PikaReplServerThread 线程
  int res = pika_repl_server_thread_->StartThread();
  if (res != net::kSuccess) {
    // 启动失败，记录日志并给出详细错误信息
    LOG(FATAL) << "Start Pika Repl Server Thread Error: " << res
               << (res == net::kBindError
                       ? ": bind port " + std::to_string(pika_repl_server_thread_->ListenPort()) + " conflict"
                       : ": create thread error ")
               << ", Listen on this port to handle the request sent by the Slave";
  }

  // 启动线程池
  res = server_tp_->start_thread_pool();
  if (res != net::kSuccess) {
    // 启动线程池失败，记录日志并给出详细错误信息
    LOG(FATAL) << "Start ThreadPool Error: " << res
               << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
  return res;
}

// 停止 PikaReplServer 线程和线程池
int PikaReplServer::Stop() {
  // 停止线程池
  server_tp_->stop_thread_pool();

  // 停止 PikaReplServerThread 线程，并进行清理
  pika_repl_server_thread_->StopThread();
  pika_repl_server_thread_->Cleanup();
  return 0;
}

// 向指定的从服务器发送 binlog 数据块（可能分块发送）
pstd::Status PikaReplServer::SendSlaveBinlogChips(const std::string& ip, int port,
                                                  const std::vector<WriteTask>& tasks) {
  // 创建响应对象
  InnerMessage::InnerResponse response;
  // 根据任务构建 BinlogSync 响应消息
  BuildBinlogSyncResp(tasks, &response);

  // 序列化响应为字符串
  std::string binlog_chip_pb;
  if (!response.SerializeToString(&binlog_chip_pb)) {
    return Status::Corruption("Serialized Failed");  // 序列化失败，返回错误
  }

  // 如果序列化后的数据块超出最大缓冲区大小，则进行分块发送
  if (binlog_chip_pb.size() > static_cast<size_t>(g_pika_conf->max_conn_rbuf_size())) {
    // 遍历任务，将每个任务单独发送
    for (const auto& task : tasks) {
      InnerMessage::InnerResponse response;
      std::vector<WriteTask> tmp_tasks;
      tmp_tasks.push_back(task);

      // 构建响应并序列化
      BuildBinlogSyncResp(tmp_tasks, &response);
      if (!response.SerializeToString(&binlog_chip_pb)) {
        return Status::Corruption("Serialized Failed");  // 序列化失败，返回错误
      }

      // 发送序列化后的数据
      pstd::Status s = Write(ip, port, binlog_chip_pb);
      if (!s.ok()) {
        return s;  // 发送失败，返回错误
      }
    }
    return pstd::Status::OK();  // 分块发送成功
  }

  // 如果数据块未超出大小限制，直接发送
  return Write(ip, port, binlog_chip_pb);
}

// 将 LogOffset 转换为 BinlogOffset，并设置到 boffset 中
void PikaReplServer::BuildBinlogOffset(const LogOffset& offset, InnerMessage::BinlogOffset* boffset) {
  // 设置 BinlogOffset 的文件号和偏移量
  boffset->set_filenum(offset.b_offset.filenum);
  boffset->set_offset(offset.b_offset.offset);

  // 设置 BinlogOffset 的 term 和 index（逻辑偏移量）
  boffset->set_term(offset.l_offset.term);
  boffset->set_index(offset.l_offset.index);
}

// 构建一个 Binlog 同步响应消息，并填充相关信息
void PikaReplServer::BuildBinlogSyncResp(const std::vector<WriteTask>& tasks, InnerMessage::InnerResponse* response) {
  // 设置响应的状态码为 OK，类型为 BinlogSync
  response->set_code(InnerMessage::kOk);
  response->set_type(InnerMessage::Type::kBinlogSync);

  // 遍历每个写任务，填充 BinlogSync 相关信息
  for (const auto& task : tasks) {
    // 添加一个新的 BinlogSync 到响应中
    InnerMessage::InnerResponse::BinlogSync* binlog_sync = response->add_binlog_sync();

    // 设置会话 ID
    binlog_sync->set_session_id(task.rm_node_.SessionId());

    // 获取 Slot 信息并设置数据库名称
    InnerMessage::Slot* db = binlog_sync->mutable_slot();
    db->set_db_name(task.rm_node_.DBName());

    /*
     * 由于 slot 字段是使用 protobuf 写入的，
     * 但为了兼容旧版本，slot_id 设置为默认值 0，虽然 slot_id 不再使用
     */
    db->set_slot_id(0);

    // 构建并设置 BinlogOffset
    InnerMessage::BinlogOffset* boffset = binlog_sync->mutable_binlog_offset();
    BuildBinlogOffset(task.binlog_chip_.offset_, boffset);

    // 设置 Binlog 内容
    binlog_sync->set_binlog(task.binlog_chip_.binlog_);
  }
}

// 向指定 IP 和端口写入消息
pstd::Status PikaReplServer::Write(const std::string& ip, const int port, const std::string& msg) {
  // 使用共享锁保护 client_conn_rwlock_，防止并发访问 client_conn_map_
  std::shared_lock l(client_conn_rwlock_);

  // 将 IP 和端口组合成字符串
  const std::string ip_port = pstd::IpPortString(ip, port);

  // 检查客户端连接是否存在
  if (client_conn_map_.find(ip_port) == client_conn_map_.end()) {
    return Status::NotFound("The " + ip_port + " fd cannot be found");
  }

  // 获取客户端连接对应的文件描述符
  int fd = client_conn_map_[ip_port];

  // 从连接池获取 PbConn 类型的连接对象
  std::shared_ptr<net::PbConn> conn = std::dynamic_pointer_cast<net::PbConn>(pika_repl_server_thread_->get_conn(fd));
  if (!conn) {
    return Status::NotFound("The" + ip_port + " conn cannot be found");
  }

  // 如果写入响应失败，返回错误状态并通知关闭连接
  if (conn->WriteResp(msg)) {
    conn->NotifyClose();
    return Status::Corruption("The" + ip_port + " conn, Write Resp Failed");
  }

  // 如果写入成功，通知写操作完成
  conn->NotifyWrite();
  return Status::OK();
}

// 调度一个任务函数到指定的服务器线程池中
void PikaReplServer::Schedule(net::TaskFunc func, void* arg) { server_tp_->Schedule(func, arg); }

// 更新客户端连接映射，记录 IP:端口 和 文件描述符的映射关系
void PikaReplServer::UpdateClientConnMap(const std::string& ip_port, int fd) {
  // 使用锁保护对 client_conn_map_ 的修改
  std::lock_guard l(client_conn_rwlock_);
  client_conn_map_[ip_port] = fd;
}

// 移除指定文件描述符对应的客户端连接
void PikaReplServer::RemoveClientConn(int fd) {
  // 使用锁保护对 client_conn_map_ 的修改
  std::lock_guard l(client_conn_rwlock_);

  // 遍历 client_conn_map_，查找文件描述符并删除对应项
  auto iter = client_conn_map_.begin();
  while (iter != client_conn_map_.end()) {
    if (iter->second == fd) {
      iter = client_conn_map_.erase(iter);  // 删除映射项
      break;
    }
    iter++;
  }
}

void PikaReplServer::KillAllConns() { return pika_repl_server_thread_->KillAllConns(); }
