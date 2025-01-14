// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <utility>

#include "include/pika_db.h"

#include "include/pika_cmd_table_manager.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "mutex_impl.h"

using pstd::Status;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

std::string DBPath(const std::string& path, const std::string& db_name) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/", db_name.data());
  return path + buf;
}

std::string DbSyncPath(const std::string& sync_path, const std::string& db_name) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s/", db_name.data());
  return sync_path + buf;
}

DB::DB(std::string db_name, const std::string& db_path, const std::string& log_path)
    : db_name_(db_name), bgsave_engine_(nullptr) {
  db_path_ = DBPath(db_path, db_name_);
  bgsave_sub_path_ = db_name;
  dbsync_path_ = DbSyncPath(g_pika_conf->db_sync_path(), db_name);
  log_path_ = DBPath(log_path, "log_" + db_name_);
  storage_ = std::make_shared<storage::Storage>(g_pika_conf->db_instance_num(), g_pika_conf->default_slot_num(),
                                                g_pika_conf->classic_mode());
  rocksdb::Status s = storage_->Open(g_pika_server->storage_options(), db_path_);
  pstd::CreatePath(db_path_);
  pstd::CreatePath(log_path_);
  lock_mgr_ = std::make_shared<pstd::lock::LockMgr>(1000, 0, std::make_shared<pstd::lock::MutexFactoryImpl>());
  binlog_io_error_.store(false);
  opened_ = s.ok();
  assert(storage_);
  assert(s.ok());
  LOG(INFO) << db_name_ << " DB Success";
}

DB::~DB() { StopKeyScan(); }

bool DB::WashData() {
  rocksdb::ReadOptions read_options;
  rocksdb::Status s;
  auto suffix_len = storage::ParsedBaseDataValue::GetkBaseDataValueSuffixLength();
  for (int i = 0; i < g_pika_conf->db_instance_num(); i++) {
    rocksdb::WriteBatch batch;
    auto handle = storage_->GetHashCFHandles(i)[1];
    auto db = storage_->GetDBByIndex(i);
    auto it(db->NewIterator(read_options, handle));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::string key = it->key().ToString();
      std::string value = it->value().ToString();
      if (value.size() < suffix_len) {
        // need to wash
        storage::BaseDataValue internal_value(value);
        batch.Put(handle, key, internal_value.Encode());
      }
    }
    delete it;
    s = db->Write(storage_->GetDefaultWriteOptions(i), &batch);
    if (!s.ok()) {
      return false;
    }
  }
  return true;
}

std::string DB::GetDBName() { return db_name_; }

void DB::BgSaveDB() {
  std::shared_lock l(dbs_rw_);
  std::lock_guard ml(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return;
  }
  bgsave_info_.bgsaving = true;
  auto bg_task_arg = new BgTaskArg();
  bg_task_arg->db = shared_from_this();
  g_pika_server->BGSaveTaskSchedule(&DoBgSave, static_cast<void*>(bg_task_arg));
}

void DB::SetBinlogIoError() { return binlog_io_error_.store(true); }
void DB::SetBinlogIoErrorrelieve() { return binlog_io_error_.store(false); }
bool DB::IsBinlogIoError() { return binlog_io_error_.load(); }
std::shared_ptr<pstd::lock::LockMgr> DB::LockMgr() { return lock_mgr_; }
std::shared_ptr<PikaCache> DB::cache() const { return cache_; }
std::shared_ptr<storage::Storage> DB::storage() const { return storage_; }

void DB::KeyScan() {
  std::lock_guard ml(key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return;
  }

  key_scan_info_.duration = -2;  // duration -2 mean the task in waiting status,
                                 // has not been scheduled for exec
  auto bg_task_arg = new BgTaskArg();
  bg_task_arg->db = shared_from_this();
  g_pika_server->KeyScanTaskSchedule(&DoKeyScan, reinterpret_cast<void*>(bg_task_arg));
}

bool DB::IsKeyScaning() {
  std::lock_guard ml(key_scan_protector_);
  return key_scan_info_.key_scaning_;
}

void DB::RunKeyScan() {
  Status s;
  std::vector<storage::KeyInfo> new_key_infos;

  InitKeyScan();
  std::shared_lock l(dbs_rw_);
  s = GetKeyNum(&new_key_infos);
  key_scan_info_.duration = static_cast<int32_t>(time(nullptr) - key_scan_info_.start_time);

  std::lock_guard lm(key_scan_protector_);
  if (s.ok()) {
    key_scan_info_.key_infos = new_key_infos;
  }
  key_scan_info_.key_scaning_ = false;
}

Status DB::GetKeyNum(std::vector<storage::KeyInfo>* key_info) {
  std::lock_guard l(key_info_protector_);
  if (key_scan_info_.key_scaning_) {
    *key_info = key_scan_info_.key_infos;
    return Status::OK();
  }
  InitKeyScan();
  key_scan_info_.key_scaning_ = true;
  key_scan_info_.duration = -2;  // duration -2 mean the task in waiting status,
                                 // has not been scheduled for exec
  rocksdb::Status s = storage_->GetKeyNum(key_info);
  key_scan_info_.key_scaning_ = false;
  if (!s.ok()) {
    return Status::Corruption(s.ToString());
  }
  key_scan_info_.key_infos = *key_info;
  key_scan_info_.duration = static_cast<int32_t>(time(nullptr) - key_scan_info_.start_time);
  return Status::OK();
}

void DB::StopKeyScan() {
  std::shared_lock rwl(dbs_rw_);
  std::lock_guard ml(key_scan_protector_);

  if (!key_scan_info_.key_scaning_) {
    return;
  }
  storage_->StopScanKeyNum();
  key_scan_info_.key_scaning_ = false;
}

void DB::ScanDatabase(const storage::DataType& type) {
  std::shared_lock l(dbs_rw_);
  storage_->ScanDatabase(type);
}

KeyScanInfo DB::GetKeyScanInfo() {
  std::lock_guard lm(key_scan_protector_);
  return key_scan_info_;
}

void DB::Compact(const storage::DataType& type) {
  std::lock_guard rwl(dbs_rw_);
  if (!opened_) {
    return;
  }
  storage_->Compact(type);
}

void DB::CompactRange(const storage::DataType& type, const std::string& start, const std::string& end) {
  std::lock_guard rwl(dbs_rw_);
  if (!opened_) {
    return;
  }
  storage_->CompactRange(type, start, end);
}

void DB::LongestNotCompactionSstCompact(const storage::DataType& type) {
  std::lock_guard rwl(dbs_rw_);
  if (!opened_) {
    return;
  }
  storage_->LongestNotCompactionSstCompact(type);
}

void DB::DoKeyScan(void* arg) {
  std::unique_ptr<BgTaskArg> bg_task_arg(static_cast<BgTaskArg*>(arg));
  bg_task_arg->db->RunKeyScan();
}

void DB::InitKeyScan() {
  key_scan_info_.start_time = time(nullptr);
  char s_time[32];
  size_t len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
  key_scan_info_.s_start_time.assign(s_time, len);
  key_scan_info_.duration = -1;  // duration -1 mean the task in processing
}

void DB::SetCompactRangeOptions(const bool is_canceled) {
  if (!opened_) {
    return;
  }
  storage_->SetCompactRangeOptions(is_canceled);
}

DisplayCacheInfo DB::GetCacheInfo() {
  std::lock_guard l(cache_info_rwlock_);
  return cache_info_;
}

bool DB::FlushDBWithoutLock() {
  std::lock_guard l(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << db_name_ << " Delete old db...";
  storage_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  std::string delete_suffix("_deleting_");
  delete_suffix.append(std::to_string(NowMicros()));
  delete_suffix.append("/");
  dbpath.append(delete_suffix);
  auto rename_success = pstd::RenameFile(db_path_, dbpath);
  storage_ = std::make_shared<storage::Storage>(g_pika_conf->db_instance_num(), g_pika_conf->default_slot_num(),
                                                g_pika_conf->classic_mode());
  rocksdb::Status s = storage_->Open(g_pika_server->storage_options(), db_path_);
  assert(storage_);
  assert(s.ok());
  if (rename_success == -1) {
    // the storage_->Open actually opened old RocksDB instance, so flushdb failed
    LOG(WARNING) << db_name_ << " FlushDB failed due to rename old db_path_ failed";
    return false;
  }
  LOG(INFO) << db_name_ << " Open new db success";

  g_pika_server->PurgeDir(dbpath);
  return true;
}

void DB::DoBgSave(void* arg) {
  std::unique_ptr<BgTaskArg> bg_task_arg(static_cast<BgTaskArg*>(arg));

  // Do BgSave
  bool success = bg_task_arg->db->RunBgsaveEngine();

  // Some output
  BgSaveInfo info = bg_task_arg->db->bgsave_info();
  std::stringstream info_content;
  std::ofstream out;
  out.open(info.path + "/" + kBgsaveInfoFile, std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    info_content << (time(nullptr) - info.start_time) << "s\n"
                 << g_pika_server->host() << "\n"
                 << g_pika_server->port() << "\n"
                 << info.offset.b_offset.filenum << "\n"
                 << info.offset.b_offset.offset << "\n";
    bg_task_arg->db->snapshot_uuid_ = md5(info_content.str());
    out << info_content.rdbuf();
    out.close();
  }
  if (!success) {
    std::string fail_path = info.path + "_FAILED";
    pstd::RenameFile(info.path, fail_path);
  }
  bg_task_arg->db->FinishBgsave();
}

bool DB::RunBgsaveEngine() {
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return false;
  }
  LOG(INFO) << db_name_ << " after prepare bgsave";

  BgSaveInfo info = bgsave_info();
  LOG(INFO) << db_name_ << " bgsave_info: path=" << info.path << ",  filenum=" << info.offset.b_offset.filenum
            << ", offset=" << info.offset.b_offset.offset;

  // Backup to tmp dir
  rocksdb::Status s = bgsave_engine_->CreateNewBackup(info.path);

  if (!s.ok()) {
    LOG(WARNING) << db_name_ << " create new backup failed :" << s.ToString();
    return false;
  }
  LOG(INFO) << db_name_ << " create new backup finished.";

  return true;
}

BgSaveInfo DB::bgsave_info() {
  std::lock_guard l(bgsave_protector_);
  return bgsave_info_;
}

void DB::FinishBgsave() {
  std::lock_guard l(bgsave_protector_);
  bgsave_info_.bgsaving = false;
  g_pika_server->UpdateLastSave(time(nullptr));
}

// Prepare engine, need bgsave_protector protect
bool DB::InitBgsaveEnv() {
  std::lock_guard l(bgsave_protector_);
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(nullptr);
  char s_time[32];
  int len = static_cast<int32_t>(strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time)));
  bgsave_info_.s_start_time.assign(s_time, len);
  std::string time_sub_path = g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
  bgsave_info_.path = g_pika_conf->bgsave_path() + time_sub_path + "/" + bgsave_sub_path_;
  if (!pstd::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(WARNING) << db_name_ << " remove exist bgsave dir failed";
    return false;
  }
  pstd::CreatePath(bgsave_info_.path, 0755);
  // Prepare for failed dir
  if (!pstd::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(WARNING) << db_name_ << " remove exist fail bgsave dir failed :";
    return false;
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool DB::InitBgsaveEngine() {
  bgsave_engine_.reset();
  rocksdb::Status s = storage::BackupEngine::Open(storage().get(), bgsave_engine_, g_pika_conf->db_instance_num());
  if (!s.ok()) {
    LOG(WARNING) << db_name_ << " open backup engine failed " << s.ToString();
    return false;
  }

  std::shared_ptr<SyncMasterDB> db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name_));
  if (!db) {
    LOG(WARNING) << db_name_ << " not found";
    return false;
  }

  {
    std::lock_guard lock(dbs_rw_);
    LogOffset bgsave_offset;
    // term, index are 0
    db->Logger()->GetProducerStatus(&(bgsave_offset.b_offset.filenum), &(bgsave_offset.b_offset.offset));
    {
      std::lock_guard l(bgsave_protector_);
      bgsave_info_.offset = bgsave_offset;
    }
    s = bgsave_engine_->SetBackupContent();
    if (!s.ok()) {
      LOG(WARNING) << db_name_ << " set backup content failed " << s.ToString();
      return false;
    }
  }
  return true;
}

void DB::Init() {
  cache_ = std::make_shared<PikaCache>(g_pika_conf->zset_cache_start_direction(),
                                       g_pika_conf->zset_cache_field_num_per_key());
  // Create cache
  cache::CacheConfig cache_cfg;
  g_pika_server->CacheConfigInit(cache_cfg);
  cache_->Init(g_pika_conf->GetCacheNum(), &cache_cfg);
}

void DB::GetBgSaveMetaData(std::vector<std::string>* fileNames, std::string* snapshot_uuid) {
  const std::string dbPath = bgsave_info().path;

  int db_instance_num = g_pika_conf->db_instance_num();
  for (int index = 0; index < db_instance_num; index++) {
    std::string instPath = dbPath + ((dbPath.back() != '/') ? "/" : "") + std::to_string(index);
    if (!pstd::FileExists(instPath)) {
      continue;
    }

    std::vector<std::string> tmpFileNames;
    int ret = pstd::GetChildren(instPath, tmpFileNames);
    if (ret) {
      LOG(WARNING) << dbPath << " read dump meta files failed, path " << instPath;
      return;
    }

    for (const std::string fileName : tmpFileNames) {
      fileNames->push_back(std::to_string(index) + "/" + fileName);
    }
  }
  fileNames->push_back(kBgsaveInfoFile);
  pstd::Status s = GetBgSaveUUID(snapshot_uuid);
  if (!s.ok()) {
    LOG(WARNING) << "read dump meta info failed! error:" << s.ToString();
    return;
  }
}

Status DB::GetBgSaveUUID(std::string* snapshot_uuid) {
  if (snapshot_uuid_.empty()) {
    std::string info_data;
    const std::string infoPath = bgsave_info().path + "/info";
    // TODO: using file read function to replace rocksdb::ReadFileToString
    rocksdb::Status s = rocksdb::ReadFileToString(rocksdb::Env::Default(), infoPath, &info_data);
    if (!s.ok()) {
      LOG(WARNING) << "read dump meta info failed! error:" << s.ToString();
      return Status::IOError("read dump meta info failed", infoPath);
    }
    pstd::MD5 md5 = pstd::MD5(info_data);
    snapshot_uuid_ = md5.hexdigest();
  }
  *snapshot_uuid = snapshot_uuid_;
  return Status::OK();
}
// 尝试更新主节点的偏移量（master offset）
// 当从主节点完成数据库同步时可能需要执行此操作
// 执行以下操作：
// 1. 检查数据库同步是否完成，并获取新的 binlog 偏移量
// 2. 替换旧的数据库文件
// 3. 更新主节点的偏移量，PikaAuxiliaryThread 的定时任务会连接主节点并执行 slaveof 操作
bool DB::TryUpdateMasterOffset() {
  // 获取当前数据库对应的从节点实例（SyncSlaveDB）
  std::shared_ptr<SyncSlaveDB> slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name_));
  if (!slave_db) {
    // 如果从节点不存在，记录错误日志并设置状态为错误
    LOG(ERROR) << "Slave DB: " << db_name_ << " not exist";
    slave_db->SetReplState(ReplState::kError);
    return false;
  }

  // 构造同步信息文件的路径
  std::string info_path = dbsync_path_ + kBgsaveInfoFile;
  if (!pstd::FileExists(info_path)) {
    // 如果同步信息文件不存在，记录警告日志并将从节点状态设置为尝试重新连接
    LOG(WARNING) << "info path: " << info_path << " not exist, Slave DB:" << GetDBName()
                 << " will restart the sync process...";
    slave_db->SetReplState(ReplState::kTryConnect);
    return false;
  }

  // 打开同步信息文件以读取主节点的 binlog 偏移量信息
  std::ifstream is(info_path);
  if (!is) {
    // 如果文件打开失败，记录警告日志并设置从节点状态为错误
    LOG(WARNING) << "DB: " << db_name_ << ", Failed to open info file after db sync";
    slave_db->SetReplState(ReplState::kError);
    return false;
  }

  // 变量定义，用于解析同步信息文件内容
  std::string line;
  std::string master_ip;    // 主节点 IP 地址
  int lineno = 0;           // 行号
  int64_t filenum = 0;      // binlog 文件号
  int64_t offset = 0;       // binlog 偏移量
  int64_t term = 0;         // 主节点的任期
  int64_t index = 0;        // binlog 索引
  int64_t tmp = 0;          // 临时变量
  int64_t master_port = 0;  // 主节点端口号

  // 按行读取同步信息文件内容
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      // 第二行是主节点 IP 地址
      master_ip = line;
    } else if (lineno > 2 && lineno < 8) {
      // 第 3~7 行是同步信息的具体数据
      if ((pstd::string2int(line.data(), line.size(), &tmp) == 0) || tmp < 0) {
        // 如果数据格式不正确，记录警告日志并设置从节点状态为错误
        LOG(WARNING) << "DB: " << db_name_ << ", Format of info file after db sync error, line : " << line;
        is.close();
        slave_db->SetReplState(ReplState::kError);
        return false;
      }
      // 根据行号分别赋值
      if (lineno == 3) {
        master_port = tmp;
      } else if (lineno == 4) {
        filenum = tmp;
      } else if (lineno == 5) {
        offset = tmp;
      } else if (lineno == 6) {
        term = tmp;
      } else if (lineno == 7) {
        index = tmp;
      }
    } else if (lineno > 8) {
      // 如果文件有多余的行，说明格式错误
      LOG(WARNING) << "DB: " << db_name_ << ", Format of info file after db sync error, line : " << line;
      is.close();
      slave_db->SetReplState(ReplState::kError);
      return false;
    }
  }
  is.close();  // 关闭文件

  // 记录从同步信息文件中解析出的主节点信息
  LOG(INFO) << "DB: " << db_name_ << " Information from dbsync info"
            << ",  master_ip: " << master_ip << ", master_port: " << master_port << ", filenum: " << filenum
            << ", offset: " << offset << ", term: " << term << ", index: " << index;

  // 删除同步信息文件
  pstd::DeleteFile(info_path);
  if (!ChangeDb(dbsync_path_)) {
    // 替换旧数据库文件失败，记录警告日志并设置从节点状态为错误
    LOG(WARNING) << "DB: " << db_name_ << ", Failed to change db";
    slave_db->SetReplState(ReplState::kError);
    return false;
  }

  // 更新主节点的偏移量
  std::shared_ptr<SyncMasterDB> master_db = g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name_));
  if (!master_db) {
    // 如果主节点实例不存在，记录警告日志并设置从节点状态为错误
    LOG(WARNING) << "Master DB: " << db_name_ << " not exist";
    slave_db->SetReplState(ReplState::kError);
    return false;
  }

  // 设置主节点的 binlog 偏移量
  master_db->Logger()->SetProducerStatus(filenum, offset);
  slave_db->SetReplState(ReplState::kTryConnect);

  // 完成全量同步后，移除未完成的全量同步计数
  g_pika_conf->RemoveInternalUsedUnfinishedFullSync(slave_db->DBName());

  return true;  // 同步完成
}

void DB::PrepareRsync() {
  // 如果同步目录已存在，则先删除
  pstd::DeleteDirIfExist(dbsync_path_);

  // 为每个数据库实例创建对应的同步目录
  int db_instance_num = g_pika_conf->db_instance_num();
  for (int index = 0; index < db_instance_num; index++) {
    pstd::CreatePath(dbsync_path_ + std::to_string(index));
  }
}
bool DB::IsBgSaving() {
  std::lock_guard ml(bgsave_protector_);
  return bgsave_info_.bgsaving;
}
/*
 * 更改数据库路径，定位到新的路径 new_path
 * 成功时返回 true
 * 如果失败，数据库路径保持不变，返回 false
 */
bool DB::ChangeDb(const std::string& new_path) {
  std::string tmp_path(db_path_);
  // 如果原路径以 '/' 结尾，移除 '/' 符号
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";  // 设置备份路径

  // 如果备份路径存在，删除它
  pstd::DeleteDirIfExist(tmp_path);

  // 加锁，确保数据库路径修改过程中线程安全
  std::lock_guard l(dbs_rw_);
  LOG(INFO) << "DB: " << db_name_ << ", Prepare change db from: " << tmp_path;
  storage_.reset();  // 重置存储对象

  // 尝试将当前数据库路径重命名为备份路径
  if (0 != pstd::RenameFile(db_path_, tmp_path)) {
    LOG(WARNING) << "DB: " << db_name_ << ", Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }

  // 尝试将新的数据库路径重命名为当前路径
  if (0 != pstd::RenameFile(new_path, db_path_)) {
    LOG(WARNING) << "DB: " << db_name_ << ", Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }

  // 初始化存储对象
  storage_ = std::make_shared<storage::Storage>(g_pika_conf->db_instance_num(), g_pika_conf->default_slot_num(),
                                                g_pika_conf->classic_mode());
  // 打开新的数据库存储
  rocksdb::Status s = storage_->Open(g_pika_server->storage_options(), db_path_);
  assert(storage_);
  assert(s.ok());

  // 删除备份路径
  pstd::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "DB: " << db_name_ << ", Change db success";
  return true;
}
void DB::ClearBgsave() {
  std::lock_guard l(bgsave_protector_);
  bgsave_info_.Clear();
}

void DB::UpdateCacheInfo(CacheInfo& cache_info) {
  std::unique_lock<std::shared_mutex> lock(cache_info_rwlock_);

  cache_info_.status = cache_info.status;
  cache_info_.cache_num = cache_info.cache_num;
  cache_info_.keys_num = cache_info.keys_num;
  cache_info_.used_memory = cache_info.used_memory;
  cache_info_.waitting_load_keys_num = cache_info.waitting_load_keys_num;
  cache_usage_ = cache_info.used_memory;

  uint64_t all_cmds = cache_info.hits + cache_info.misses;
  cache_info_.hitratio_all = (0 >= all_cmds) ? 0.0 : (cache_info.hits * 100.0) / all_cmds;

  uint64_t cur_time_us = pstd::NowMicros();
  uint64_t delta_time = cur_time_us - cache_info_.last_time_us + 1;
  uint64_t delta_hits = cache_info.hits - cache_info_.hits;
  cache_info_.hits_per_sec = delta_hits * 1000000 / delta_time;

  uint64_t delta_all_cmds = all_cmds - (cache_info_.hits + cache_info_.misses);
  cache_info_.read_cmd_per_sec = delta_all_cmds * 1000000 / delta_time;

  cache_info_.hitratio_per_sec = (0 >= delta_all_cmds) ? 0.0 : (delta_hits * 100.0) / delta_all_cmds;

  uint64_t delta_load_keys = cache_info.async_load_keys_num - cache_info_.last_load_keys_num;
  cache_info_.load_keys_per_sec = delta_load_keys * 1000000 / delta_time;

  cache_info_.hits = cache_info.hits;
  cache_info_.misses = cache_info.misses;
  cache_info_.last_time_us = cur_time_us;
  cache_info_.last_load_keys_num = cache_info.async_load_keys_num;
}

void DB::ResetDisplayCacheInfo(int status) {
  std::unique_lock<std::shared_mutex> lock(cache_info_rwlock_);
  cache_info_.status = status;
  cache_info_.cache_num = 0;
  cache_info_.keys_num = 0;
  cache_info_.used_memory = 0;
  cache_info_.hits = 0;
  cache_info_.misses = 0;
  cache_info_.hits_per_sec = 0;
  cache_info_.read_cmd_per_sec = 0;
  cache_info_.hitratio_per_sec = 0.0;
  cache_info_.hitratio_all = 0.0;
  cache_info_.load_keys_per_sec = 0;
  cache_info_.waitting_load_keys_num = 0;
  cache_usage_ = 0;
}
