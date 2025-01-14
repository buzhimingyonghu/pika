// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <filesystem>

#include <glog/logging.h>
#include <google/protobuf/map.h>

#include "include/pika_server.h"
#include "include/rsync_server.h"
#include "pstd/include/pstd_defer.h"
#include "pstd_hash.h"

extern PikaServer* g_pika_server;
namespace rsync {

using namespace net;
using namespace pstd;
using namespace RsyncService;

// 向客户端发送 Rsync 响应
void RsyncWriteResp(RsyncService::RsyncResponse& response, std::shared_ptr<net::PbConn> conn) {
  std::string reply_str;

  // 将响应序列化为字符串，如果失败或者写入失败，记录警告日志并关闭连接
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Process FileRsync request serialization failed";
    conn->NotifyClose();
    return;
  }

  // 通知连接写入完成
  conn->NotifyWrite();
}

// RsyncServer 构造函数，初始化线程池和 Rsync 服务线程
RsyncServer::RsyncServer(const std::set<std::string>& ips, const int port) {
  // 创建工作线程池，最多允许 100000 个任务，线程池包含 2 个线程
  work_thread_ = std::make_unique<net::ThreadPool>(2, 100000, "RsyncServerWork");

  // 创建 Rsync 服务线程，指定监听 IP、端口号及心跳间隔（1 秒）
  rsync_server_thread_ = std::make_unique<RsyncServerThread>(ips, port, 1 * 1000, this);
}

// RsyncServer 析构函数
RsyncServer::~RsyncServer() {
  // TODO: 添加销毁逻辑（如果有）
  LOG(INFO) << "Rsync server destroyed";
}

// 将任务调度到工作线程池执行
void RsyncServer::Schedule(net::TaskFunc func, void* arg) { work_thread_->Schedule(func, arg); }

// 启动 Rsync 服务
int RsyncServer::Start() {
  LOG(INFO) << "start RsyncServer ...";

  // 启动 Rsync 服务线程
  rsync_server_thread_->set_thread_name("RsyncServerThread");
  int res = rsync_server_thread_->StartThread();
  if (res != net::kSuccess) {
    LOG(FATAL) << "Start rsync Server Thread Error. ret_code: " << res
               << " message: " << (res == net::kBindError ? ": bind port conflict" : ": other error");
  }

  // 启动工作线程池
  res = work_thread_->start_thread_pool();
  if (res != net::kSuccess) {
    LOG(FATAL) << "Start rsync Server ThreadPool Error, ret_code: " << res
               << " message: " << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
  }

  LOG(INFO) << "RsyncServer started ...";
  return res;
}

// 停止 Rsync 服务
int RsyncServer::Stop() {
  LOG(INFO) << "stop RsyncServer ...";

  // 停止线程池和服务线程
  work_thread_->stop_thread_pool();
  rsync_server_thread_->StopThread();
  return 0;
}

// RsyncServerConn 构造函数，用于初始化连接
RsyncServerConn::RsyncServerConn(int connfd, const std::string& ip_port, Thread* thread, void* worker_specific_data,
                                 NetMultiplexer* mpx)
    : PbConn(connfd, ip_port, thread, mpx), data_(worker_specific_data) {
  // 初始化多个 RsyncReader 实例，用于并行处理
  readers_.resize(kMaxRsyncParallelNum);
  for (int i = 0; i < kMaxRsyncParallelNum; i++) {
    readers_[i].reset(new RsyncReader());
  }
}

// RsyncServerConn 析构函数，释放资源
RsyncServerConn::~RsyncServerConn() {
  std::lock_guard<std::mutex> guard(mu_);
  for (int i = 0; i < readers_.size(); i++) {
    readers_[i].reset();  // 清除 RsyncReader 的指针
  }
}

// 处理消息的主要逻辑
int RsyncServerConn::DealMessage() {
  // 创建 RsyncRequest 实例，用于解析客户端发来的请求
  std::shared_ptr<RsyncService::RsyncRequest> req = std::make_shared<RsyncService::RsyncRequest>();

  // 从读取缓冲区解析请求，如果失败，记录警告日志并返回错误码
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika rsync server connection pb parse error.";
    return -1;
  }

  // 根据请求类型执行不同的处理逻辑
  switch (req->type()) {
    case RsyncService::kRsyncMeta: {  // 处理元数据同步请求
      auto task_arg = new RsyncServerTaskArg(req, std::dynamic_pointer_cast<RsyncServerConn>(shared_from_this()));
      ((RsyncServer*)(data_))->Schedule(&RsyncServerConn::HandleMetaRsyncRequest, task_arg);
      break;
    }
    case RsyncService::kRsyncFile: {  // 处理文件同步请求
      auto task_arg = new RsyncServerTaskArg(req, std::dynamic_pointer_cast<RsyncServerConn>(shared_from_this()));
      ((RsyncServer*)(data_))->Schedule(&RsyncServerConn::HandleFileRsyncRequest, task_arg);
      break;
    }
    default: {  // 非法的 Rsync 请求类型
      LOG(WARNING) << "Invalid RsyncRequest type";
    }
  }
  return 0;  // 成功返回
}
// 处理 Rsync 元数据同步请求
void RsyncServerConn::HandleMetaRsyncRequest(void* arg) {
  // 使用智能指针管理任务参数，避免内存泄漏
  std::unique_ptr<RsyncServerTaskArg> task_arg(static_cast<RsyncServerTaskArg*>(arg));

  // 获取请求和连接对象
  const std::shared_ptr<RsyncService::RsyncRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  // 提取数据库名称并获取对应的 DB 实例
  std::string db_name = req->db_name();
  std::shared_ptr<DB> db = g_pika_server->GetDB(db_name);

  // 初始化响应对象
  RsyncService::RsyncResponse response;
  response.set_reader_index(req->reader_index());
  response.set_code(RsyncService::kOk);
  response.set_type(RsyncService::kRsyncMeta);
  response.set_db_name(db_name);

  // 为兼容旧版本设置默认 slot_id（实际未使用）
  response.set_slot_id(0);

  // 如果数据库未找到或正在进行后台保存操作，返回错误响应
  std::string snapshot_uuid;
  if (!db || db->IsBgSaving()) {
    LOG(WARNING) << "waiting bgsave done...";
    response.set_snapshot_uuid(snapshot_uuid);
    response.set_code(RsyncService::kErr);
    RsyncWriteResp(response, conn);
    return;
  }

  // 获取数据库的快照元信息（文件列表和快照 UUID）
  std::vector<std::string> filenames;
  g_pika_server->GetDumpMeta(db_name, &filenames, &snapshot_uuid);
  response.set_snapshot_uuid(snapshot_uuid);

  // 日志记录快照信息
  LOG(INFO) << "Rsync Meta request, snapshot_uuid: " << snapshot_uuid << " files count: " << filenames.size()
            << " file list: ";
  std::for_each(filenames.begin(), filenames.end(), [](auto& file) { LOG(INFO) << "rsync snapshot file: " << file; });

  // 将文件列表添加到响应中
  RsyncService::MetaResponse* meta_resp = response.mutable_meta_resp();
  for (const auto& filename : filenames) {
    meta_resp->add_filenames(filename);
  }

  // 发送响应
  RsyncWriteResp(response, conn);
}

// 处理 Rsync 文件同步请求
void RsyncServerConn::HandleFileRsyncRequest(void* arg) {
  // 使用智能指针管理任务参数，避免内存泄漏
  std::unique_ptr<RsyncServerTaskArg> task_arg(static_cast<RsyncServerTaskArg*>(arg));

  // 获取请求和连接对象
  const std::shared_ptr<RsyncService::RsyncRequest> req = task_arg->req;
  std::shared_ptr<RsyncServerConn> conn = task_arg->conn;

  // 提取请求参数
  std::string db_name = req->db_name();
  std::string filename = req->file_req().filename();
  size_t offset = req->file_req().offset();
  size_t count = req->file_req().count();

  // 初始化响应对象
  RsyncService::RsyncResponse response;
  response.set_reader_index(req->reader_index());
  response.set_code(RsyncService::kOk);
  response.set_type(RsyncService::kRsyncFile);
  response.set_db_name(db_name);

  // 为兼容旧版本设置默认 slot_id（实际未使用）
  response.set_slot_id(0);

  // 获取快照 UUID
  std::string snapshot_uuid;
  Status s = g_pika_server->GetDumpUUID(db_name, &snapshot_uuid);
  response.set_snapshot_uuid(snapshot_uuid);
  if (!s.ok()) {
    LOG(WARNING) << "rsyncserver get snapshotUUID failed";
    response.set_code(RsyncService::kErr);
    RsyncWriteResp(response, conn);
    return;
  }

  // 获取数据库对象
  std::shared_ptr<DB> db = g_pika_server->GetDB(db_name);
  if (!db) {
    LOG(WARNING) << "cannot find db for db_name: " << db_name;
    response.set_code(RsyncService::kErr);
    RsyncWriteResp(response, conn);
    return;
  }

  // 拼接文件路径
  const std::string filepath = db->bgsave_info().path + "/" + filename;

  // 读取文件数据
  char* buffer = new char[req->file_req().count() + 1];
  size_t bytes_read = 0;
  std::string checksum = "";
  bool is_eof = false;

  // 使用 RsyncReader 读取文件内容
  std::shared_ptr<RsyncReader> reader = conn->readers_[req->reader_index()];
  s = reader->Read(filepath, offset, count, buffer, &bytes_read, &checksum, &is_eof);
  if (!s.ok()) {
    response.set_code(RsyncService::kErr);
    RsyncWriteResp(response, conn);
    delete[] buffer;
    return;
  }

  // 设置文件同步响应数据
  RsyncService::FileResponse* file_resp = response.mutable_file_resp();
  file_resp->set_data(buffer, bytes_read);
  file_resp->set_eof(is_eof);
  file_resp->set_checksum(checksum);
  file_resp->set_filename(filename);
  file_resp->set_count(bytes_read);
  file_resp->set_offset(offset);

  // 发送响应
  RsyncWriteResp(response, conn);

  // 释放缓冲区内存
  delete[] buffer;
}

// Rsync 服务线程的构造函数
RsyncServerThread::RsyncServerThread(const std::set<std::string>& ips, int port, int cron_interval, RsyncServer* arg)
    : HolyThread(ips, port, &conn_factory_, cron_interval, &handle_, true), conn_factory_(arg) {}

// Rsync 服务线程的析构函数
RsyncServerThread::~RsyncServerThread() { LOG(WARNING) << "RsyncServerThread destroyed"; }

// 处理连接关闭事件
void RsyncServerThread::RsyncServerHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  LOG(WARNING) << "ip_port: " << ip_port << " connection closed";
}

// 处理连接超时事件
void RsyncServerThread::RsyncServerHandle::FdTimeoutHandle(int fd, const std::string& ip_port) const {
  LOG(WARNING) << "ip_port: " << ip_port << " connection timeout";
}

// 处理新连接事件
bool RsyncServerThread::RsyncServerHandle::AccessHandle(int fd, std::string& ip_port) const {
  LOG(WARNING) << "fd: " << fd << " ip_port: " << ip_port << " connection accepted";
  return true;  // 返回 true 表示接受连接
}

// 定时任务的处理函数（此处未实现具体逻辑）
void RsyncServerThread::RsyncServerHandle::CronHandle() const {}
}