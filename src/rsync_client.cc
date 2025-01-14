// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <stdio.h>
#include <fstream>

#include "include/pika_server.h"
#include "include/rsync_client.h"
#include "pstd/include/pstd_defer.h"
#include "rocksdb/env.h"

using namespace net;
using namespace pstd;
using namespace RsyncService;

extern PikaServer* g_pika_server;

const int kFlushIntervalUs = 10 * 1000 * 1000;
const int kBytesPerRequest = 4 << 20;
const int kThrottleCheckCycle = 10;

namespace rsync {
RsyncClient::RsyncClient(const std::string& dir, const std::string& db_name)
    : snapshot_uuid_(""),
      dir_(dir),
      db_name_(db_name),
      state_(IDLE),
      max_retries_(10),
      master_ip_(""),
      master_port_(0),
      parallel_num_(g_pika_conf->max_rsync_parallel_num()) {
  wo_mgr_.reset(new WaitObjectManager());
  client_thread_ = std::make_unique<RsyncClientThread>(3000, 60, wo_mgr_.get());
  client_thread_->set_thread_name("RsyncClientThread");
  work_threads_.resize(GetParallelNum());
  finished_work_cnt_.store(0);
}

void RsyncClient::Copy(const std::set<std::string>& file_set, int index) {
  // 初始化状态为 OK，表示拷贝操作开始
  Status s = Status::OK();

  // 遍历需要拷贝的文件集合
  for (const auto& file : file_set) {
    // 持续尝试拷贝，直到成功或状态不再是 RUNNING
    while (state_.load() == RUNNING) {
      // 日志记录当前正在拷贝的文件名
      LOG(INFO) << "copy remote file, filename: " << file;

      // 调用 CopyRemoteFile 函数拷贝远程文件
      s = CopyRemoteFile(file, index);

      // 如果拷贝失败，记录警告并继续尝试
      if (!s.ok()) {
        LOG(WARNING) << "copy remote file failed, msg: " << s.ToString();
        continue;
      }

      // 如果拷贝成功，退出当前循环
      break;
    }

    // 如果状态不再是 RUNNING，则中断循环
    if (state_.load() != RUNNING) {
      break;
    }
  }

  // 如果未触发错误停止标志，说明拷贝操作完成
  if (!error_stopped_.load()) {
    LOG(INFO) << "work_thread index: " << index << " copy remote files done";
  }

  // 增加已完成工作的线程计数
  finished_work_cnt_.fetch_add(1);

  // 通知等待中的线程拷贝任务完成
  cond_.notify_all();
}
bool RsyncClient::Init() {
  // 初始化时检查当前状态是否为 IDLE，只有 IDLE 状态才允许初始化
  if (state_ != IDLE) {
    LOG(WARNING) << "State should be IDLE when Init";
    return false;
  }

  // 从全局对象中获取主机 IP 和端口，并初始化远程同步端口
  master_ip_ = g_pika_server->master_ip();
  master_port_ = g_pika_server->master_port() + kPortShiftRsync2;

  // 清空文件集合，确保状态干净
  file_set_.clear();

  // 启动客户端线程
  client_thread_->StartThread();

  // 调用 ComparisonUpdate 函数检查本地和远程数据是否一致
  bool ret = ComparisonUpdate();
  if (!ret) {
    // 如果 ComparisonUpdate 失败，停止客户端线程并返回 false
    LOG(WARNING) << "RsyncClient recover failed";
    client_thread_->StopThread();
    state_.store(IDLE);
    return false;
  }

  // 初始化完成的工作线程计数为 0
  finished_work_cnt_.store(0);

  // 日志记录初始化成功
  LOG(INFO) << "RsyncClient recover success";
  return true;
}
void* RsyncClient::ThreadMain() {
  // 如果文件集合为空，表示没有需要同步的远程文件
  if (file_set_.empty()) {
    LOG(INFO) << "No remote files need copy, RsyncClient exit and going to delete dir:" << dir_;
    DeleteDirIfExist(dir_);          // 删除指定的目录（如果存在）
    state_.store(STOP);              // 更新状态为停止
    all_worker_exited_.store(true);  // 标记所有线程已退出
    return nullptr;                  // 返回空指针，结束线程
  }

  Status s = Status::OK();  // 初始化状态为 OK
  LOG(INFO) << "RsyncClient begin to copy remote files";

  // 按并行线程数对文件集合进行划分，将文件分配到多个线程中
  std::vector<std::set<std::string>> file_vec(GetParallelNum());
  int index = 0;
  for (const auto& file : file_set_) {
    file_vec[index++ % GetParallelNum()].insert(file);
  }

  // 标记线程工作状态为非空闲
  all_worker_exited_.store(false);

  // 创建并启动多个线程，并为每个线程分配文件集合
  for (int i = 0; i < GetParallelNum(); i++) {
    work_threads_[i] = std::move(std::thread(&RsyncClient::Copy, this, file_vec[i], i));
  }

  // 打开本地元数据文件，用于记录文件同步状态
  std::string meta_file_path = GetLocalMetaFilePath();
  std::ofstream outfile;
  outfile.open(meta_file_path, std::ios_base::app);  // 以追加模式打开文件
  if (!outfile.is_open()) {
    // 如果打开失败，记录错误日志并设置错误状态
    LOG(ERROR) << "unable to open meta file " << meta_file_path << ", error:" << strerror(errno);
    error_stopped_.store(true);
    state_.store(STOP);
  }
  // 保证文件关闭（即使异常退出）
  DEFER { outfile.close(); };

  std::string meta_rep;                     // 用于存储元数据的字符串
  uint64_t start_time = pstd::NowMicros();  // 获取当前时间（微秒）

  // 主线程负责定时将元数据写入文件，并检查所有工作线程状态
  while (state_.load() == RUNNING) {
    uint64_t elapse = pstd::NowMicros() - start_time;  // 计算时间差
    if (elapse < kFlushIntervalUs) {
      int wait_for_us = kFlushIntervalUs - elapse;  // 剩余等待时间
      std::unique_lock<std::mutex> lock(mu_);
      cond_.wait_for(lock, std::chrono::microseconds(wait_for_us));  // 等待条件变量，节省 CPU 资源
    }

    // 检查状态是否仍为运行，如果不是则跳出循环
    if (state_.load() != RUNNING) {
      break;
    }

    start_time = pstd::NowMicros();  // 更新起始时间

    // 交换并清空元数据表，收集文件的同步状态
    std::map<std::string, std::string> files_map;
    {
      std::lock_guard<std::mutex> guard(mu_);
      files_map.swap(meta_table_);
    }
    // 将文件状态写入元数据字符串
    for (const auto& file : files_map) {
      meta_rep.append(file.first + ":" + file.second);
      meta_rep.append("\n");
    }
    // 写入文件并清空缓冲区
    outfile << meta_rep;
    outfile.flush();
    meta_rep.clear();

    // 如果所有线程的工作都完成，退出循环
    if (finished_work_cnt_.load() == GetParallelNum()) {
      break;
    }
  }

  // 等待所有线程完成工作
  for (int i = 0; i < GetParallelNum(); i++) {
    work_threads_[i].join();
  }

  // 重置完成工作线程计数，更新状态为停止
  finished_work_cnt_.store(0);
  state_.store(STOP);

  // 根据错误状态输出日志并进行清理
  if (!error_stopped_.load()) {
    LOG(INFO) << "RsyncClient copy remote files done";  // 同步成功
  } else {
    if (DeleteDirIfExist(dir_)) {
      // 同步失败但成功删除目录
      LOG(ERROR) << "RsyncClient stopped with errors, deleted:" << dir_;
    } else {
      // 同步失败且目录删除失败
      LOG(ERROR) << "RsyncClient stopped with errors, but failed to delete " << dir_ << " when cleaning";
    }
  }

  // 标记所有线程已退出
  all_worker_exited_.store(true);
  return nullptr;
}
Status RsyncClient::CopyRemoteFile(const std::string& filename, int index) {
  // 确定本地文件路径
  const std::string filepath = dir_ + "/" + filename;

  // 创建文件写入器，用于将远程传输的数据写入本地文件
  std::unique_ptr<RsyncWriter> writer(new RsyncWriter(filepath));
  Status s = Status::OK();
  size_t offset = 0;  // 文件读取的偏移量，初始为 0
  int retries = 0;    // 重试次数，初始为 0

  // 定义清理逻辑，确保函数退出时关闭写入器，必要时删除失败的文件
  DEFER {
    if (writer) {
      writer->Close();  // 关闭文件写入器
      writer.reset();
    }
    if (!s.ok()) {
      DeleteFile(filepath);  // 删除不完整的文件
    }
  };

  // 主循环：请求远程数据，直到文件传输完成或达到最大重试次数
  while (retries < max_retries_) {
    if (state_.load() != RUNNING) {
      break;  // 如果客户端状态不是 RUNNING，退出传输
    }

    // 获取当前可以传输的数据量，受限于流量控制器 Throttle
    size_t copy_file_begin_time = pstd::NowMicros();
    size_t count = Throttle::GetInstance().ThrottledByThroughput(kBytesPerRequest);
    if (count == 0) {
      // 如果当前流量控制为 0，等待一段时间后重试
      std::this_thread::sleep_for(std::chrono::milliseconds(1000 / kThrottleCheckCycle));
      continue;
    }

    // 构造 RsyncRequest 请求，包含文件信息和读取范围
    RsyncRequest request;
    request.set_reader_index(index);  // 指定请求对应的 reader
    request.set_type(kRsyncFile);     // 设置请求类型为文件传输
    request.set_db_name(db_name_);    // 设置数据库名称

    // 设置 slot_id 为 0，用于兼容旧版本，但当前并未使用
    request.set_slot_id(0);

    // 填充文件请求具体信息，包括文件名、偏移量和读取数量
    FileRequest* file_req = request.mutable_file_req();
    file_req->set_filename(filename);
    file_req->set_offset(offset);
    file_req->set_count(count);

    // 序列化请求并发送到远程服务器
    std::string to_send;
    request.SerializeToString(&to_send);

    // 等待对象，用于同步响应
    WaitObject* wo = wo_mgr_->UpdateWaitObject(index, filename, kRsyncFile, offset);
    s = client_thread_->Write(master_ip_, master_port_, to_send);
    if (!s.ok()) {
      LOG(WARNING) << "send rsync request failed";  // 记录发送失败日志
      continue;
    }

    // 等待远程服务器的响应
    std::shared_ptr<RsyncResponse> resp = nullptr;
    s = wo->Wait(resp);
    if (s.IsTimeout() || resp == nullptr) {
      LOG(WARNING) << s.ToString();  // 超时或响应为空时记录日志
      retries++;
      continue;
    }

    // 检查响应代码是否为 kOk
    if (resp->code() != RsyncService::kOk) {
      return Status::IOError("kRsyncFile request failed, master response error code");
    }

    // 提取响应中的文件数据
    size_t ret_count = resp->file_resp().count();  // 本次传输的字节数
    size_t elaspe_time_us = pstd::NowMicros() - copy_file_begin_time;

    // 更新流量控制器状态，释放未使用的带宽
    Throttle::GetInstance().ReturnUnusedThroughput(count, ret_count, elaspe_time_us);

    // 检查快照 UUID 是否一致
    if (resp->snapshot_uuid() != snapshot_uuid_) {
      LOG(WARNING) << "receive newer dump, reset state to STOP, local_snapshot_uuid:" << snapshot_uuid_
                   << ", remote snapshot uuid: " << resp->snapshot_uuid();
      state_.store(STOP);          // 停止传输
      error_stopped_.store(true);  // 设置错误标志
      return s;
    }

    // 将数据写入本地文件
    s = writer->Write((uint64_t)offset, ret_count, resp->file_resp().data().c_str());
    if (!s.ok()) {
      LOG(WARNING) << "rsync client write file error";  // 写入失败，记录日志
      break;
    }

    // 更新偏移量
    offset += resp->file_resp().count();

    // 如果文件传输完成 (EOF 标志)，执行文件同步并更新元数据表
    if (resp->file_resp().eof()) {
      s = writer->Fsync();  // 将文件写入同步到磁盘
      if (!s.ok()) {
        return s;  // 同步失败返回错误
      }
      mu_.lock();
      meta_table_[filename] = "";  // 更新元数据表
      mu_.unlock();
      break;  // 退出循环，传输完成
    }

    // 本次成功后重置重试次数
    retries = 0;
  }

  return s;  // 返回传输状态
}

Status RsyncClient::Start() {
  StartThread();
  return Status::OK();
}

Status RsyncClient::Stop() {
  if (state_ == IDLE) {
    return Status::OK();
  }
  LOG(WARNING) << "RsyncClient stop ...";
  state_ = STOP;
  cond_.notify_all();
  StopThread();
  client_thread_->StopThread();
  JoinThread();
  client_thread_->JoinThread();
  state_ = IDLE;
  return Status::OK();
}

bool RsyncClient::ComparisonUpdate() {
  // 定义变量，用于存储本地和远程的快照 UUID、文件集合和文件映射
  std::string local_snapshot_uuid;                    // 本地快照 UUID
  std::string remote_snapshot_uuid;                   // 远程快照 UUID
  std::set<std::string> local_file_set;               // 本地文件集合
  std::set<std::string> remote_file_set;              // 远程文件集合
  std::map<std::string, std::string> local_file_map;  // 本地文件名到校验和的映射

  // 拉取远程元数据
  Status s = PullRemoteMeta(&remote_snapshot_uuid, &remote_file_set);
  if (!s.ok()) {
    LOG(WARNING) << "copy remote meta failed! error:" << s.ToString();
    return false;  // 如果拉取失败，返回 false
  }

  // 加载本地元数据
  s = LoadLocalMeta(&local_snapshot_uuid, &local_file_map);
  if (!s.ok()) {
    LOG(WARNING) << "load local meta failed";
    return false;  // 如果加载失败，返回 false
  }

  // 将本地文件映射的键（文件名）插入到本地文件集合
  for (auto const& file : local_file_map) {
    local_file_set.insert(file.first);
  }

  std::set<std::string> expired_files;  // 过期文件集合

  // 如果本地和远程的快照 UUID 不一致
  if (remote_snapshot_uuid != local_snapshot_uuid) {
    // 更新当前快照 UUID 和文件集合为远程数据
    snapshot_uuid_ = remote_snapshot_uuid;
    file_set_ = remote_file_set;
    expired_files = local_file_set;  // 所有本地文件被标记为过期文件
  } else {
    // 如果快照 UUID 一致，计算新增文件和过期文件

    // 计算远程比本地新增的文件
    std::set<std::string> newly_files;
    set_difference(remote_file_set.begin(), remote_file_set.end(), local_file_set.begin(), local_file_set.end(),
                   inserter(newly_files, newly_files.begin()));

    // 计算本地比远程多的（即过期的）文件
    set_difference(local_file_set.begin(), local_file_set.end(), remote_file_set.begin(), remote_file_set.end(),
                   inserter(expired_files, expired_files.begin()));

    // 将新增的文件添加到当前文件集合
    file_set_.insert(newly_files.begin(), newly_files.end());
  }

  // 清理过期文件
  s = CleanUpExpiredFiles(local_snapshot_uuid != remote_snapshot_uuid, expired_files);
  if (!s.ok()) {
    LOG(WARNING) << "clean up expired files failed";
    return false;  // 如果清理失败，返回 false
  }

  // 更新本地元数据文件
  s = UpdateLocalMeta(snapshot_uuid_, expired_files, &local_file_map);
  if (!s.ok()) {
    LOG(WARNING) << "update local meta failed";
    return false;  // 如果更新失败，返回 false
  }

  // 更新状态
  state_.store(RUNNING);        // 设置为运行状态
  error_stopped_.store(false);  // 清除错误停止标志

  // 打印日志，记录同步操作的详细信息
  LOG(INFO) << "copy meta data done, db name: " << db_name_ << " snapshot_uuid: " << snapshot_uuid_
            << " file count: " << file_set_.size() << " expired file count: " << expired_files.size()
            << " local file count: " << local_file_set.size() << " remote file count: " << remote_file_set.size()
            << " remote snapshot_uuid: " << remote_snapshot_uuid << " local snapshot_uuid: " << local_snapshot_uuid
            << " file_set_: " << file_set_.size();

  // 输出当前文件集合
  for_each(file_set_.begin(), file_set_.end(), [](auto& file) { LOG(WARNING) << "file_set: " << file; });

  return true;  // 返回 true 表示更新成功
}
Status RsyncClient::PullRemoteMeta(std::string* snapshot_uuid, std::set<std::string>* file_set) {
  Status s;
  int retries = 0;

  // 构造 RsyncRequest 请求对象，用于拉取远程元数据
  RsyncRequest request;
  request.set_reader_index(0);    // 设置 reader 索引
  request.set_db_name(db_name_);  // 数据库名称
  /*
   * 设置 slot_id 为默认值 0，用于兼容旧版本，但当前并未使用
   */
  request.set_slot_id(0);
  request.set_type(kRsyncMeta);  // 设置请求类型为元数据拉取

  // 序列化请求
  std::string to_send;
  request.SerializeToString(&to_send);

  // 循环发送请求，直到成功或达到最大重试次数
  while (retries < max_retries_) {
    // 创建等待对象，用于等待远程服务器响应
    WaitObject* wo = wo_mgr_->UpdateWaitObject(0, "", kRsyncMeta, kInvalidOffset);

    // 发送请求到远程服务器
    s = client_thread_->Write(master_ip_, master_port_, to_send);
    if (!s.ok()) {
      retries++;  // 发送失败，增加重试次数
    }

    // 等待响应
    std::shared_ptr<RsyncResponse> resp;
    s = wo->Wait(resp);

    // 超时处理
    if (s.IsTimeout()) {
      LOG(WARNING) << "rsync PullRemoteMeta request timeout, retry times: " << retries;
      retries++;
      continue;
    }

    // 如果响应为空或响应代码不是 kOk，则记录错误并重试
    if (resp.get() == nullptr || resp->code() != RsyncService::kOk) {
      s = Status::IOError("kRsyncMeta request failed! db is not exist or doing bgsave");
      LOG(WARNING) << s.ToString() << ", retries:" << retries;
      sleep(1);
      retries++;
      continue;
    }

    // 打印日志：成功接收到远程元数据
    LOG(INFO) << "receive rsync meta infos, snapshot_uuid: " << resp->snapshot_uuid()
              << " files count: " << resp->meta_resp().filenames_size();

    // 将接收到的文件名插入到 file_set 集合
    for (std::string item : resp->meta_resp().filenames()) {
      file_set->insert(item);
    }

    // 更新 snapshot_uuid
    *snapshot_uuid = resp->snapshot_uuid();
    s = Status::OK();
    break;
  }

  return s;
}
Status RsyncClient::LoadLocalMeta(std::string* snapshot_uuid, std::map<std::string, std::string>* file_map) {
  // 获取本地元数据文件的路径
  std::string meta_file_path = GetLocalMetaFilePath();

  // 如果元数据文件不存在，直接返回 OK
  if (!FileExists(meta_file_path)) {
    LOG(WARNING) << kDumpMetaFileName << " not exist";
    return Status::OK();
  }

  FILE* fp;              // 文件指针
  char* line = nullptr;  // 行缓冲区
  size_t len = 0;        // 行缓冲区长度
  size_t read = 0;       // 读取的字符数
  int32_t line_num = 0;  // 当前行号

  std::atomic_int8_t retry_times = 5;  // 文件打开重试次数

  // 尝试打开元数据文件，最多重试 5 次
  while (retry_times > 0) {
    retry_times--;
    fp = fopen(meta_file_path.c_str(), "r");
    if (fp == nullptr) {
      LOG(WARNING) << "open meta file failed, meta_path: " << dir_;
    } else {
      break;
    }
  }

  // 如果文件仍无法打开，返回错误
  if (fp == nullptr) {
    LOG(WARNING) << "open meta file failed, meta_path: " << meta_file_path << ", retry times: " << retry_times;
    return Status::IOError("open meta file failed, dir: ", meta_file_path);
  }

  // 逐行读取文件内容
  while ((read = getline(&line, &len, fp)) != -1) {
    std::string str(line);  // 当前行内容
    std::string::size_type pos;

    // 删除行中的 "\r" 和 "\n" 字符
    while ((pos = str.find("\r")) != std::string::npos) {
      str.erase(pos, 1);
    }
    while ((pos = str.find("\n")) != std::string::npos) {
      str.erase(pos, 1);
    }

    // 如果行为空，跳过
    if (str.empty()) {
      continue;
    }

    // 第一行：提取 snapshot_uuid
    if (line_num == 0) {
      *snapshot_uuid = str.erase(0, kUuidPrefix.size());
    } else {
      // 其他行：解析文件名和校验和，并插入到 file_map
      if ((pos = str.find(":")) != std::string::npos) {
        std::string filename = str.substr(0, pos);               // 文件名
        std::string checksum = str.substr(pos + 1, str.size());  // 校验和
        (*file_map)[filename] = checksum;
      }
    }

    line_num++;
  }

  fclose(fp);  // 关闭文件
  return Status::OK();
}

Status RsyncClient::CleanUpExpiredFiles(bool need_reset_path, const std::set<std::string>& files) {
  if (need_reset_path) {
    std::string db_path = dir_ + (dir_.back() == '/' ? "" : "/");
    pstd::DeleteDirIfExist(db_path);
    int db_instance_num = g_pika_conf->db_instance_num();
    for (int idx = 0; idx < db_instance_num; idx++) {
      pstd::CreatePath(db_path + std::to_string(idx));
    }
    return Status::OK();
  }

  std::string db_path = dir_ + (dir_.back() == '/' ? "" : "/");
  for (const auto& file : files) {
    bool b = pstd::DeleteDirIfExist(db_path + file);
    if (!b) {
      LOG(WARNING) << "delete file failed, file: " << file;
      return Status::IOError("delete file failed");
    }
  }
  return Status::OK();
}

Status RsyncClient::UpdateLocalMeta(const std::string& snapshot_uuid, const std::set<std::string>& expired_files,
                                    std::map<std::string, std::string>* localFileMap) {
  if (localFileMap->empty()) {
    return Status::OK();
  }

  for (const auto& item : expired_files) {
    localFileMap->erase(item);
  }

  std::string meta_file_path = GetLocalMetaFilePath();
  pstd::DeleteFile(meta_file_path);

  std::unique_ptr<WritableFile> file;
  pstd::Status s = pstd::NewWritableFile(meta_file_path, file);
  if (!s.ok()) {
    LOG(WARNING) << "create meta file failed, meta_file_path: " << meta_file_path;
    return s;
  }
  file->Append(kUuidPrefix + snapshot_uuid + "\n");

  for (const auto& item : *localFileMap) {
    std::string line = item.first + ":" + item.second + "\n";
    file->Append(line);
  }
  s = file->Close();
  if (!s.ok()) {
    LOG(WARNING) << "flush meta file failed, meta_file_path: " << meta_file_path;
    return s;
  }
  return Status::OK();
}

std::string RsyncClient::GetLocalMetaFilePath() {
  std::string db_path = dir_ + (dir_.back() == '/' ? "" : "/");
  return db_path + kDumpMetaFileName;
}

int RsyncClient::GetParallelNum() { return parallel_num_; }

}  // end namespace rsync
