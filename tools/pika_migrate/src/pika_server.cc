// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_server.h"

#include <ctime>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/resource.h>

#include "slash/include/env.h"
#include "slash/include/rsync.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"
#include "pink/include/bg_thread.h"

#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_sender.h"
#include "include/migrator_thread.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_cmd_table_manager.h"

extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

void DoPurgeDir(void* arg) {
  std::string path = *(static_cast<std::string*>(arg));
  LOG(INFO) << "Delete dir: " << path << " start";
  slash::DeleteDir(path);
  LOG(INFO) << "Delete dir: " << path << " done";
  delete static_cast<std::string*>(arg);
}

void DoDBSync(void* arg) {
  DBSyncArg* dbsa = reinterpret_cast<DBSyncArg*>(arg);
  PikaServer* const ps = dbsa->p;
  ps->DbSyncSendFile(dbsa->ip, dbsa->port,
          dbsa->table_name, dbsa->partition_id);
  delete dbsa;
}

PikaServer::PikaServer() :
  exit_(false),
  slot_state_(INFREE),
  have_scheduled_crontask_(false),
  last_check_compact_time_({0, 0}),
  master_ip_(""),
  master_port_(0),
  repl_state_(PIKA_REPL_NO_CONNECT),
  role_(PIKA_ROLE_SINGLE),
  loop_partition_state_machine_(false),
  force_full_sync_(false),
  slowlog_entry_id_(0) {

  //Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }

  InitBlackwidowOptions();

  pthread_rwlockattr_t tables_rw_attr;
  pthread_rwlockattr_init(&tables_rw_attr);
  pthread_rwlockattr_setkind_np(&tables_rw_attr,
          PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&tables_rw_, &tables_rw_attr);

  // Create thread
  worker_num_ = std::min(g_pika_conf->thread_num(),
                         PIKA_MAX_WORKER_THREAD_NUM);

  std::set<std::string> ips;
  if (g_pika_conf->network_interface().empty()) {
    ips.insert("0.0.0.0");
  } else {
    ips.insert("127.0.0.1");
    ips.insert(host_);
  }
  // We estimate the queue size
  int worker_queue_limit = g_pika_conf->maxclients() / worker_num_ + 100;
  LOG(INFO) << "Worker queue limit is " << worker_queue_limit;
  pika_dispatch_thread_ = new PikaDispatchThread(ips, port_, worker_num_, 3000,
                                                 worker_queue_limit);
  pika_monitor_thread_ = new PikaMonitorThread();
  pika_rsync_service_ = new PikaRsyncService(g_pika_conf->db_sync_path(),
                                             g_pika_conf->port() + kPortShiftRSync);
  pika_pubsub_thread_ = new pink::PubSubThread();
  pika_auxiliary_thread_ = new PikaAuxiliaryThread();
  pika_thread_pool_ = new pink::ThreadPool(g_pika_conf->thread_pool_size(), 100000);

  // Create redis sender
  for (int i = 0; i < g_pika_conf->redis_sender_num(); i++) {
    redis_senders_.emplace_back(
            new RedisSender(int(i),
                            g_pika_conf->target_redis_host(),
                            g_pika_conf->target_redis_port(),
                            g_pika_conf->target_redis_pwd()));
  }

  pthread_rwlock_init(&state_protector_, NULL);
  pthread_rwlock_init(&slowlog_protector_, NULL);
}

PikaServer::~PikaServer() {

  // DispatchThread will use queue of worker thread,
  // so we need to delete dispatch before worker.
  pika_thread_pool_->stop_thread_pool();
  delete pika_dispatch_thread_;

  {
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    while (iter != slaves_.end()) {
      iter =  slaves_.erase(iter);
      LOG(INFO) << "Delete slave success";
    }
  }

  delete pika_pubsub_thread_;
  delete pika_auxiliary_thread_;
  delete pika_rsync_service_;
  delete pika_thread_pool_;
  delete pika_monitor_thread_;

  for (size_t i = 0; i < redis_senders_.size(); i++) {
    redis_senders_[i]->Stop();
  }
  // wait thread exit
  sleep(1);
  for (size_t i = 0; i < redis_senders_.size(); i++) {
    delete redis_senders_[i];
  }
  redis_senders_.clear();

  bgsave_thread_.StopThread();
  key_scan_thread_.StopThread();

  tables_.clear();

  pthread_rwlock_destroy(&tables_rw_);
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&slowlog_protector_);

  LOG(INFO) << "PikaServer " << pthread_self() << " exit!!!";
}

bool PikaServer::ServerInit() {
  std::string network_interface = g_pika_conf->network_interface();

  if (network_interface == "") {

    std::ifstream routeFile("/proc/net/route", std::ios_base::in);
    if (!routeFile.good())
    {
      return false;
    }

    std::string line;
    std::vector<std::string> tokens;
    while(std::getline(routeFile, line))
    {
      std::istringstream stream(line);
      std::copy(std::istream_iterator<std::string>(stream),
          std::istream_iterator<std::string>(),
          std::back_inserter<std::vector<std::string> >(tokens));

      // the default interface is the one having the second
      // field, Destination, set to "00000000"
      if ((tokens.size() >= 2) && (tokens[1] == std::string("00000000")))
      {
        network_interface = tokens[0];
        break;
      }

      tokens.clear();
    }
    routeFile.close();
  }
  LOG(INFO) << "Using Networker Interface: " << network_interface;

  struct ifaddrs * ifAddrStruct = NULL;
  struct ifaddrs * ifa = NULL;
  void * tmpAddrPtr = NULL;

  if (getifaddrs(&ifAddrStruct) == -1) {
    LOG(FATAL) << "getifaddrs failed: " << strerror(errno);
  }

  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) {
      continue;
    }
    if (ifa ->ifa_addr->sa_family==AF_INET) { // Check it is
      // a valid IPv4 address
      tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host_ = addressBuffer;
        break;
      }
    } else if (ifa->ifa_addr->sa_family==AF_INET6) { // Check it is
      // a valid IPv6 address
      tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host_ = addressBuffer;
        break;
      }
    }
  }

  if (ifAddrStruct != NULL) {
    freeifaddrs(ifAddrStruct);
  }
  if (ifa == NULL) {
    LOG(FATAL) << "error network interface: " << network_interface << ", please check!";
  }

  port_ = g_pika_conf->port();
  LOG(INFO) << "host: " << host_ << " port: " << port_;
  return true;
}

void PikaServer::Start() {
  int ret = 0;
  // start rsync first, rocksdb opened fd will not appear in this fork
  ret = pika_rsync_service_->StartRsync();
  if (0 != ret) {
    tables_.clear();
    LOG(FATAL) << "Start Rsync Error: bind port " +std::to_string(pika_rsync_service_->ListenPort()) + " failed"
      <<  ", Listen on this port to receive Master FullSync Data";
  }

  // We Init Table Struct Before Start The following thread
  InitTableStruct();

  ret = pika_thread_pool_->start_thread_pool();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start ThreadPool Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  ret = pika_dispatch_thread_->StartThread();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start Dispatch Error: " << ret << (ret == pink::kBindError ? ": bind port " + std::to_string(port_) + " conflict"
            : ": other error") << ", Listen on this port to handle the connected redis client";
  }
  ret = pika_pubsub_thread_->StartThread();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start Pubsub Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
  }

  ret = pika_auxiliary_thread_->StartThread();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start Auxiliary Thread Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  for (size_t i = 0; i < redis_senders_.size(); i++) {
    ret = redis_senders_[i]->StartThread();
    if (ret != pink::kSuccess) {
      LOG(FATAL) << "Start Redis Sender Thread Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
    }
  }

  time(&start_time_s_);

  std::string slaveof = g_pika_conf->slaveof();
  if (!slaveof.empty()) {
    int32_t sep = slaveof.find(":");
    std::string master_ip = slaveof.substr(0, sep);
    int32_t master_port = std::stoi(slaveof.substr(sep+1));
    if ((master_ip == "127.0.0.1" || master_ip == host_) && master_port == port_) {
      LOG(FATAL) << "you will slaveof yourself as the config file, please check";
    } else {
      SetMaster(master_ip, master_port);
    }
  }

  LOG(INFO) << "Pika Server going to start";
  while (!exit_) {
    DoTimingTask();
    // wake up every 10 second
    int try_num = 0;
    while (!exit_ && try_num++ < 10) {
      sleep(1);
    }
  }
  LOG(INFO) << "Goodbye...";
}

void PikaServer::Exit() {
  exit_ = true;
}

std::string PikaServer::host() {
  return host_;
}

int PikaServer::port() {
  return port_;
}

time_t PikaServer::start_time_s() {
  return start_time_s_;
}

std::string PikaServer::master_ip() {
  slash::RWLock(&state_protector_, false);
  return master_ip_;
}

int PikaServer::master_port() {
  slash::RWLock(&state_protector_, false);
  return master_port_;
}

int PikaServer::role() {
  slash::RWLock(&state_protector_, false);
  return role_;
}

bool PikaServer::readonly(const std::string& table_name, const std::string& key) {
  slash::RWLock(&state_protector_, false);
  if ((role_ & PIKA_ROLE_SLAVE)
    && g_pika_conf->slave_read_only()) {
    return true;
  }
  if (!g_pika_conf->classic_mode()) {
    std::shared_ptr<Table> table = GetTable(table_name);
    if (table == nullptr) {
      // swallow this error will process later
      return false;
    }
    uint32_t index = g_pika_cmd_table_manager->DistributeKey(
        key, table->PartitionNum());
    int role = 0;
    Status s = g_pika_rm->CheckPartitionRole(table_name, index, &role);
    if (!s.ok()) {
      // swallow this error will process later
      return false;
    }
    if (role & PIKA_ROLE_SLAVE) {
      return true;
    }
  }
  return false;
}

int PikaServer::repl_state() {
  slash::RWLock(&state_protector_, false);
  return repl_state_;
}

std::string PikaServer::repl_state_str() {
  slash::RWLock(&state_protector_, false);
  switch (repl_state_) {
    case PIKA_REPL_NO_CONNECT:
      return "no connect";
    case PIKA_REPL_SHOULD_META_SYNC:
      return "should meta sync";
    case PIKA_REPL_META_SYNC_DONE:
      return "meta sync done";
    case PIKA_REPL_ERROR:
      return "error";
    default:
      return "";
  }
}

bool PikaServer::force_full_sync() {
  return force_full_sync_;
}

void PikaServer::SetForceFullSync(bool v) {
  force_full_sync_ = v;
}

void PikaServer::SetDispatchQueueLimit(int queue_limit) {
  rlimit limit;
  rlim_t maxfiles = g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    LOG(WARNING) << "getrlimit error: " << strerror(errno);
  } else if (limit.rlim_cur < maxfiles) {
    rlim_t old_limit = limit.rlim_cur;
    limit.rlim_cur = maxfiles;
    limit.rlim_max = maxfiles;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
      LOG(WARNING) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
    } else {
      LOG(FATAL) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno) << "), do it by yourself";
    }
  }
  pika_dispatch_thread_->SetQueueLimit(queue_limit);
}

blackwidow::BlackwidowOptions PikaServer::bw_options() {
  return bw_options_;
}

void PikaServer::InitTableStruct() {
  std::string db_path = g_pika_conf->db_path();
  std::string log_path = g_pika_conf->log_path();
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  slash::RWLock rwl(&tables_rw_, true);
  for (const auto& table : table_structs) {
    std::string name = table.table_name;
    uint32_t num = table.partition_num;
    std::shared_ptr<Table> table_ptr = std::make_shared<Table>(
        name, num, db_path, log_path);
    table_ptr->AddPartitions(table.partition_ids);
    tables_.emplace(name, table_ptr);
  }
}

std::shared_ptr<Table> PikaServer::GetTable(const std::string &table_name) {
  slash::RWLock l(&tables_rw_, false);
  auto iter = tables_.find(table_name);
  return (iter == tables_.end()) ? NULL : iter->second;
}

std::set<uint32_t> PikaServer::GetTablePartitionIds(const std::string& table_name) {
  std::set<uint32_t> empty;
  slash::RWLock l(&tables_rw_, false);
  auto iter = tables_.find(table_name);
  return (iter == tables_.end()) ? empty : iter->second->GetPartitionIds();
}

bool PikaServer::IsBgSaving() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& patition_item : table_item.second->partitions_) {
      if (patition_item.second->IsBgSaving()) {
        return true;
      }
    }
  }
  return false;
}

bool PikaServer::IsKeyScaning() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    if (table_item.second->IsKeyScaning()) {
      return true;
    }
  }
  return false;
}

bool PikaServer::IsCompacting() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->DbRWLockReader();
      std::string task_type = partition_item.second->db()->GetCurrentTaskType();
      partition_item.second->DbRWUnLock();
      if (strcasecmp(task_type.data(), "no")) {
        return true;
      }
    }
  }
  return false;
}

bool PikaServer::IsTableExist(const std::string& table_name) {
  return GetTable(table_name) ? true : false;
}

bool PikaServer::IsTablePartitionExist(const std::string& table_name,
                                       uint32_t partition_id) {
  std::shared_ptr<Table> table_ptr = GetTable(table_name);
  if (!table_ptr) {
    return false;
  } else {
    return table_ptr->GetPartitionById(partition_id) ? true : false;
  }
}

bool PikaServer::IsCommandSupport(const std::string& command) {
  if (g_pika_conf->classic_mode()) {
    return true;
  } else {
    std::string cmd = command;
    slash::StringToLower(cmd);
    return !ShardingModeNotSupportCommands.count(cmd);
  }
}

bool PikaServer::IsTableBinlogIoError(const std::string& table_name) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->IsBinlogIoError() : true;
}

// If no collection of specified tables is given, we execute task in all tables
Status PikaServer::DoSameThingSpecificTable(const TaskType& type, const std::set<std::string>& tables) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    if (!tables.empty()
      && tables.find(table_item.first) == tables.end()) {
      continue;
    } else {
      switch (type) {
        case TaskType::kCompactAll:
          table_item.second->Compact(blackwidow::DataType::kAll);
          break;
        case TaskType::kCompactStrings:
          table_item.second->Compact(blackwidow::DataType::kStrings);
          break;
        case TaskType::kCompactHashes:
          table_item.second->Compact(blackwidow::DataType::kHashes);
          break;
        case TaskType::kCompactSets:
          table_item.second->Compact(blackwidow::DataType::kSets);
          break;
        case TaskType::kCompactZSets:
          table_item.second->Compact(blackwidow::DataType::kZSets);
          break;
        case TaskType::kCompactList:
          table_item.second->Compact(blackwidow::DataType::kLists);
          break;
        case TaskType::kStartKeyScan:
          table_item.second->KeyScan();
          break;
        case TaskType::kStopKeyScan:
          table_item.second->StopKeyScan();
          break;
        case TaskType::kBgSave:
          table_item.second->BgSaveTable();
          break;
        default:
          break;
      }
    }
  }
  return Status::OK();
}

void PikaServer::PreparePartitionTrySync() {
  slash::RWLock rwl(&tables_rw_, false);
  ReplState state = force_full_sync_ ?
      ReplState::kTryDBSync : ReplState::kTryConnect;
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      Status s = g_pika_rm->ActivateSyncSlavePartition(
          RmNode(g_pika_server->master_ip(),
            g_pika_server->master_port(),
            table_item.second->GetTableName(),
            partition_item.second->GetPartitionId()), state);
      if (!s.ok()) {
        LOG(WARNING) << s.ToString();
      }
    }
  }
  force_full_sync_ = false;
  loop_partition_state_machine_ = true;
  LOG(INFO) << "Mark try connect finish";
}

void PikaServer::PartitionSetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->DbRWLockReader();
      partition_item.second->db()->SetMaxCacheStatisticKeys(max_cache_statistic_keys);
      partition_item.second->DbRWUnLock();
    }
  }
}

void PikaServer::PartitionSetSmallCompactionThreshold(uint32_t small_compaction_threshold) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->DbRWLockReader();
      partition_item.second->db()->SetSmallCompactionThreshold(small_compaction_threshold);
      partition_item.second->DbRWUnLock();
    }
  }
}

bool PikaServer::GetTablePartitionBinlogOffset(const std::string& table_name,
                                               uint32_t partition_id,
                                               BinlogOffset* const boffset) {
  std::shared_ptr<Partition> partition = GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    return false;
  } else {
    return partition->GetBinlogOffset(boffset);
  }
}

// Only use in classic mode
std::shared_ptr<Partition> PikaServer::GetPartitionByDbName(const std::string& db_name) {
  std::shared_ptr<Table> table = GetTable(db_name);
  return table ? table->GetPartitionById(0) : NULL;
}

std::shared_ptr<Partition> PikaServer::GetTablePartitionById(
                                    const std::string& table_name,
                                    uint32_t partition_id) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartitionById(partition_id) : NULL;
}

std::shared_ptr<Partition> PikaServer::GetTablePartitionByKey(
                                    const std::string& table_name,
                                    const std::string& key) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartitionByKey(key) : NULL;
}

Status PikaServer::DoSameThingEveryPartition(const TaskType& type) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      switch (type) {
        case TaskType::kResetReplState:
          {
            Status s = g_pika_rm->SetSlaveReplState(
                    PartitionInfo(table_item.second->GetTableName(),
                        partition_item.second->GetPartitionId()),
                    ReplState::kNoConnect);
            if (!s.ok()) {
              LOG(WARNING) << s.ToString();
            }
            break;
          }
        case TaskType::kPurgeLog:
          partition_item.second->PurgeLogs();
          break;
        default:
          break;
      }
    }
  }
  return Status::OK();
}

void PikaServer::BecomeMaster() {
  slash::RWLock l(&state_protector_, true);
  role_ |= PIKA_ROLE_MASTER;
}

void PikaServer::DeleteSlave(int fd) {
  std::string ip;
  int port = -1;
  bool is_find = false;
  int slave_num = -1;
  {
    slash::MutexLock l(&slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    while (iter != slaves_.end()) {
      if (iter->conn_fd == fd) {
        ip = iter->ip;
        port = iter->port;
        is_find = true;
        g_pika_rm->LostConnection(iter->ip, iter->port);
        g_pika_rm->DropItemInWriteQueue(iter->ip, iter->port);
        LOG(INFO) << "Delete Slave Success, ip_port: " << iter->ip << ":" << iter->port;
        slaves_.erase(iter);
        break;
      }
      iter++;
    }
    slave_num = slaves_.size();
  }

  if (is_find) {
    g_pika_rm->LostConnection(ip, port);
    g_pika_rm->DropItemInWriteQueue(ip, port);
  }

  if (slave_num == 0) {
    slash::RWLock l(&state_protector_, true);
    role_ &= ~PIKA_ROLE_MASTER;
  }
}

int32_t PikaServer::CountSyncSlaves() {
  slash::MutexLock ldb(&db_sync_protector_);
  return db_sync_slaves_.size();
}

int32_t PikaServer::GetShardingSlaveListString(std::string& slave_list_str) {
  std::vector<std::string> complete_replica;
  g_pika_rm->FindCompleteReplica(&complete_replica);
  std::stringstream tmp_stream;
  size_t index = 0;
  for (auto replica : complete_replica) {
    std::string ip;
    int port;
    if(!slash::ParseIpPortString(replica, ip, port)) {
      continue;
    }
    tmp_stream << "slave" << index++ << ":ip=" << ip << ",port=" << port << "\r\n";
  }
  slave_list_str.assign(tmp_stream.str());
  return index;
}

int32_t PikaServer::GetSlaveListString(std::string& slave_list_str) {
  size_t index = 0;
  SlaveState slave_state;
  BinlogOffset master_boffset;
  BinlogOffset sent_slave_boffset;
  BinlogOffset acked_slave_boffset;
  std::stringstream tmp_stream;
  slash::MutexLock l(&slave_mutex_);
  for (const auto& slave : slaves_) {
    tmp_stream << "slave" << index++ << ":ip=" << slave.ip << ",port=" << slave.port << ",conn_fd=" << slave.conn_fd << ",lag=";
    for (const auto& ts : slave.table_structs) {
      for (size_t idx = 0; idx < ts.partition_num; ++idx) {
        std::shared_ptr<Partition> partition = GetTablePartitionById(ts.table_name, idx);
        RmNode rm_node(slave.ip, slave.port, ts.table_name, idx);
        Status s = g_pika_rm->GetSyncMasterPartitionSlaveState(rm_node, &slave_state);
        if (s.ok()
          && slave_state == SlaveState::kSlaveBinlogSync
          && g_pika_rm->GetSyncBinlogStatus(rm_node, &sent_slave_boffset, &acked_slave_boffset).ok()) {
          if (!partition || !partition->GetBinlogOffset(&master_boffset)) {
            continue;
          } else {
            uint64_t lag =
              (master_boffset.filenum - sent_slave_boffset.filenum) * g_pika_conf->binlog_file_size()
              + (master_boffset.offset - sent_slave_boffset.offset);
            tmp_stream << "(" << partition->GetPartitionName() << ":" << lag << ")";
          }
        } else {
          tmp_stream << "(" << partition->GetPartitionName() << ":not syncing)";
        }
      }
    }
    tmp_stream << "\r\n";
  }
  slave_list_str.assign(tmp_stream.str());
  return index;
}

// Try add Slave, return true if success,
// return false when slave already exist
bool PikaServer::TryAddSlave(const std::string& ip, int64_t port, int fd,
                             const std::vector<TableStruct>& table_structs) {
  std::string ip_port = slash::IpPortString(ip, port);

  slash::MutexLock l(&slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  while (iter != slaves_.end()) {
    if (iter->ip_port == ip_port) {
      LOG(WARNING) << "Slave Already Exist, ip_port: " << ip << ":" << port;
      return false;
    }
    iter++;
  }

  // Not exist, so add new
  LOG(INFO) << "Add New Slave, " << ip << ":" << port;
  SlaveItem s;
  s.ip_port = ip_port;
  s.ip = ip;
  s.port = port;
  s.conn_fd = fd;
  s.stage = SLAVE_ITEM_STAGE_ONE;
  s.table_structs = table_structs;
  gettimeofday(&s.create_time, NULL);
  slaves_.push_back(s);
  return true;
}

void PikaServer::SyncError() {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_ERROR;
  LOG(WARNING) << "Sync error, set repl_state to PIKA_REPL_ERROR";
}

void PikaServer::RemoveMaster() {
  {
    slash::RWLock l(&state_protector_, true);
    repl_state_ = PIKA_REPL_NO_CONNECT;
    role_ &= ~PIKA_ROLE_SLAVE;

    if (master_ip_ != "" && master_port_ != -1) {
      g_pika_rm->CloseReplClientConn(master_ip_, master_port_ + kPortShiftReplServer);
      g_pika_rm->LostConnection(master_ip_, master_port_);
      loop_partition_state_machine_ = false;
      LOG(INFO) << "Remove Master Success, ip_port: " << master_ip_ << ":" << master_port_;
    }

    master_ip_ = "";
    master_port_ = -1;
    DoSameThingEveryPartition(TaskType::kResetReplState);
  }
}

bool PikaServer::SetMaster(std::string& master_ip, int master_port) {
  if (master_ip == "127.0.0.1") {
    master_ip = host_;
  }
  slash::RWLock l(&state_protector_, true);
  if ((role_ ^ PIKA_ROLE_SLAVE) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    role_ |= PIKA_ROLE_SLAVE;
    repl_state_ = PIKA_REPL_SHOULD_META_SYNC;
    return true;
  }
  return false;
}

bool PikaServer::ShouldMetaSync() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_ == PIKA_REPL_SHOULD_META_SYNC;
}

void PikaServer::FinishMetaSync() {
  slash::RWLock l(&state_protector_, true);
  assert(repl_state_ == PIKA_REPL_SHOULD_META_SYNC);
  repl_state_ = PIKA_REPL_META_SYNC_DONE;
}

bool PikaServer::MetaSyncDone() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_ == PIKA_REPL_META_SYNC_DONE;
}

void PikaServer::ResetMetaSyncStatus() {
  slash::RWLock sp_l(&state_protector_, true);
  if (role_ & PIKA_ROLE_SLAVE) {
    // not change by slaveof no one, so set repl_state = PIKA_REPL_SHOULD_META_SYNC,
    // continue to connect master
    repl_state_ = PIKA_REPL_SHOULD_META_SYNC;
    loop_partition_state_machine_ = false;
    DoSameThingEveryPartition(TaskType::kResetReplState);
  }
}

bool PikaServer::AllPartitionConnectSuccess() {
  bool all_partition_connect_success = true;
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      ReplState repl_state;
      Status s = g_pika_rm->GetSlaveReplState(
              PartitionInfo(table_item.second->GetTableName(),
                  partition_item.second->GetPartitionId()), &repl_state);
      if (!s.ok()) {
        return false;
      }
      if (repl_state != ReplState::kConnected) {
        all_partition_connect_success = false;
        break;
      }
    }
  }
  return all_partition_connect_success;
}

bool PikaServer::LoopPartitionStateMachine() {
  slash::RWLock sp_l(&state_protector_, false);
  return loop_partition_state_machine_;
}

void PikaServer::SetLoopPartitionStateMachine(bool need_loop) {
  slash::RWLock sp_l(&state_protector_, true);
  assert(repl_state_ == PIKA_REPL_META_SYNC_DONE);
  loop_partition_state_machine_ = need_loop;
}

void PikaServer::Schedule(pink::TaskFunc func, void* arg) {
  pika_thread_pool_->Schedule(func, arg);
}

void PikaServer::BGSaveTaskSchedule(pink::TaskFunc func, void* arg) {
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(func, arg);
}

void PikaServer::PurgelogsTaskSchedule(pink::TaskFunc func, void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(func, arg);
}

void PikaServer::PurgeDir(const std::string& path) {
  std::string* dir_path = new std::string(path);
  PurgeDirTaskSchedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::PurgeDirTaskSchedule(void (*function)(void*), void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(function, arg);
}

void PikaServer::DBSync(const std::string& ip, int port,
                        const std::string& table_name,
                        uint32_t partition_id) {
  {
    std::string task_index =
      DbSyncTaskIndex(ip, port, table_name, partition_id);
    slash::MutexLock ml(&db_sync_protector_);
    if (db_sync_slaves_.find(task_index) != db_sync_slaves_.end()) {
      return;
    }
    db_sync_slaves_.insert(task_index);
  }
  // Reuse the bgsave_thread_
  // Since we expect BgSave and DBSync execute serially
  bgsave_thread_.StartThread();
  DBSyncArg* arg = new DBSyncArg(this, ip, port, table_name, partition_id);
  bgsave_thread_.Schedule(&DoDBSync, reinterpret_cast<void*>(arg));
}

void PikaServer::TryDBSync(const std::string& ip, int port,
                           const std::string& table_name,
                           uint32_t partition_id, int32_t top) {
  std::shared_ptr<Partition> partition =
    GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << partition->GetPartitionName()
      << " Not Found, TryDBSync Failed";
  } else {
    BgSaveInfo bgsave_info = partition->bgsave_info();
    std::string logger_filename = partition->logger()->filename;
    if (slash::IsDir(bgsave_info.path) != 0
      || !slash::FileExists(NewFileName(logger_filename, bgsave_info.filenum))
      || top - bgsave_info.filenum > kDBSyncMaxGap) {
      // Need Bgsave first
      partition->BgSavePartition();
    }
    DBSync(ip, port, table_name, partition_id);
  }
}

void PikaServer::DbSyncSendFile(const std::string& ip, int port,
                                const std::string& table_name,
                                uint32_t partition_id) {
  std::shared_ptr<Partition> partition = GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << partition->GetPartitionName()
      << " Not Found, DbSync send file Failed";
    return;
  }

  BgSaveInfo bgsave_info = partition->bgsave_info();
  std::string bg_path = bgsave_info.path;
  uint32_t binlog_filenum = bgsave_info.filenum;
  uint64_t binlog_offset = bgsave_info.offset;

  // Get all files need to send
  std::vector<std::string> descendant;
  int ret = 0;
  LOG(INFO) << "Partition: " << partition->GetPartitionName()
    << " Start Send files in " << bg_path << " to " << ip;
  ret = slash::GetChildren(bg_path, descendant);
  if (ret != 0) {
    std::string ip_port = slash::IpPortString(ip, port);
    slash::MutexLock ldb(&db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
    LOG(WARNING) << "Partition: " << partition->GetPartitionName()
      << " Get child directory when try to do sync failed, error: " << strerror(ret);
    return;
  }

  std::string local_path, target_path;
  std::string remote_path = g_pika_conf->classic_mode() ? table_name : table_name + "/" + std::to_string(partition_id);
  std::vector<std::string>::const_iterator iter = descendant.begin();
  slash::RsyncRemote remote(ip, port, kDBSyncModule, g_pika_conf->db_sync_speed() * 1024);
  std::string secret_file_path = g_pika_conf->db_sync_path();
  if (g_pika_conf->db_sync_path().back() != '/') {
    secret_file_path += "/";
  }
  secret_file_path += slash::kRsyncSubDir + "/" + kPikaSecretFile;

  for (; iter != descendant.end(); ++iter) {
    local_path = bg_path + "/" + *iter;
    target_path = remote_path + "/" + *iter;

    if (*iter == kBgsaveInfoFile) {
      continue;
    }

    if (slash::IsDir(local_path) == 0 &&
        local_path.back() != '/') {
      local_path.push_back('/');
      target_path.push_back('/');
    }

    // We need specify the speed limit for every single file
    ret = slash::RsyncSendFile(local_path, target_path, secret_file_path, remote);
    if (0 != ret) {
      LOG(WARNING) << "Partition: " << partition->GetPartitionName()
        << " RSync send file failed! From: " << *iter
        << ", To: " << target_path
        << ", At: " << ip << ":" << port
        << ", Error: " << ret;
      break;
    }
  }
  // Clear target path
  slash::RsyncSendClearTarget(bg_path + "/strings", remote_path + "/strings", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/hashes", remote_path + "/hashes", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/lists", remote_path + "/lists", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/sets", remote_path + "/sets", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/zsets", remote_path + "/zsets", secret_file_path, remote);

  pink::PinkCli* cli = pink::NewRedisCli();
  std::string lip(host_);
  if (cli->Connect(ip, port, "").ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), (struct sockaddr*) &laddr, &llen);
    lip = inet_ntoa(laddr.sin_addr);
    cli->Close();
    delete cli;
  } else {
    LOG(WARNING) << "Rsync try connect slave rsync service error"
        << ", slave rsync service(" << ip << ":" << port << ")";
    delete cli;
  }

  // Send info file at last
  if (0 == ret) {
    // need to modify the IP addr in the info file
    if (lip.compare(host_)) {
      std::ofstream fix;
      std::string fn = bg_path + "/" + kBgsaveInfoFile + "." + std::to_string(time(NULL));
      fix.open(fn, std::ios::in | std::ios::trunc);
      if (fix.is_open()) {
        fix << "0s\n" << lip << "\n" << port_ << "\n" << binlog_filenum << "\n" << binlog_offset << "\n";
        fix.close();
      }
      ret = slash::RsyncSendFile(fn, remote_path + "/" + kBgsaveInfoFile, secret_file_path, remote);
      slash::DeleteFile(fn);
      if (ret != 0) {
        LOG(WARNING) << "Partition: " << partition->GetPartitionName() << " Send Modified Info File Failed";
      }
    } else if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, remote_path + "/" + kBgsaveInfoFile, secret_file_path, remote))) {
      LOG(WARNING) << "Partition: " << partition->GetPartitionName() << " Send Info File Failed";
    }
  }
  // remove slave
  {
    std::string task_index =
      DbSyncTaskIndex(ip, port, table_name, partition_id);
    slash::MutexLock ml(&db_sync_protector_);
    db_sync_slaves_.erase(task_index);
  }

  if (0 == ret) {
    LOG(INFO) << "Partition: " << partition->GetPartitionName() << " RSync Send Files Success";
  }
}

std::string PikaServer::DbSyncTaskIndex(const std::string& ip,
                                        int port,
                                        const std::string& table_name,
                                        uint32_t partition_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s:%d_%s:%d",
      ip.data(), port, table_name.data(), partition_id);
  return buf;
}

void PikaServer::KeyScanTaskSchedule(pink::TaskFunc func, void* arg) {
  key_scan_thread_.StartThread();
  key_scan_thread_.Schedule(func, arg);
}

void PikaServer::ClientKillAll() {
  pika_dispatch_thread_->ClientKillAll();
  pika_monitor_thread_->ThreadClientKill();
}

int PikaServer::ClientKill(const std::string &ip_port) {
  if (pika_dispatch_thread_->ClientKill(ip_port)
    || pika_monitor_thread_->ThreadClientKill(ip_port)) {
    return 1;
  }
  return 0;
}

int64_t PikaServer::ClientList(std::vector<ClientInfo> *clients) {
  int64_t clients_num = 0;
  clients_num += pika_dispatch_thread_->ThreadClientList(clients);
  clients_num += pika_monitor_thread_->ThreadClientList(clients);
  return clients_num;
}

bool PikaServer::HasMonitorClients() {
  return pika_monitor_thread_->HasMonitorClients();
}

void PikaServer::AddMonitorMessage(const std::string& monitor_message) {
  pika_monitor_thread_->AddMonitorMessage(monitor_message);
}

void PikaServer::AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr) {
  pika_monitor_thread_->AddMonitorClient(client_ptr);
}

void PikaServer::SlowlogTrim() {
  pthread_rwlock_wrlock(&slowlog_protector_);
  while (slowlog_list_.size() > static_cast<uint32_t>(g_pika_conf->slowlog_max_len())) {
    slowlog_list_.pop_back();
  }
  pthread_rwlock_unlock(&slowlog_protector_);
}

void PikaServer::SlowlogReset() {
  pthread_rwlock_wrlock(&slowlog_protector_);
  slowlog_list_.clear();
  pthread_rwlock_unlock(&slowlog_protector_);
}

uint32_t PikaServer::SlowlogLen() {
  RWLock l(&slowlog_protector_, false);
  return slowlog_list_.size();
}

void PikaServer::SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs) {
  pthread_rwlock_rdlock(&slowlog_protector_);
  slowlogs->clear();
  std::list<SlowlogEntry>::const_iterator iter = slowlog_list_.begin();
  while (number-- && iter != slowlog_list_.end()) {
    slowlogs->push_back(*iter);
    iter++;
  }
  pthread_rwlock_unlock(&slowlog_protector_);
}

void PikaServer::SlowlogPushEntry(const PikaCmdArgsType& argv, int32_t time, int64_t duration) {
  SlowlogEntry entry;
  uint32_t slargc = (argv.size() < SLOWLOG_ENTRY_MAX_ARGC)
      ? argv.size() : SLOWLOG_ENTRY_MAX_ARGC;

  for (uint32_t idx = 0; idx < slargc; ++idx) {
    if (slargc != argv.size() && idx == slargc - 1) {
      char buffer[32];
      sprintf(buffer, "... (%lu more arguments)", argv.size() - slargc + 1);
      entry.argv.push_back(std::string(buffer));
    } else {
      if (argv[idx].size() > SLOWLOG_ENTRY_MAX_STRING) {
        char buffer[32];
        sprintf(buffer, "... (%lu more bytes)", argv[idx].size() - SLOWLOG_ENTRY_MAX_STRING);
        std::string suffix(buffer);
        std::string brief = argv[idx].substr(0, SLOWLOG_ENTRY_MAX_STRING);
        entry.argv.push_back(brief + suffix);
      } else {
        entry.argv.push_back(argv[idx]);
      }
    }
  }

  pthread_rwlock_wrlock(&slowlog_protector_);
  entry.id = slowlog_entry_id_++;
  entry.start_time = time;
  entry.duration = duration;
  slowlog_list_.push_front(entry);
  pthread_rwlock_unlock(&slowlog_protector_);

  SlowlogTrim();
}

void PikaServer::ResetStat() {
  statistic_data_.accumulative_connections.store(0);
  statistic_data_.thread_querynum.store(0);
  statistic_data_.last_thread_querynum.store(0);
}

uint64_t PikaServer::ServerQueryNum() {
  return statistic_data_.thread_querynum.load();
}

uint64_t PikaServer::ServerCurrentQps() {
  return statistic_data_.last_sec_thread_querynum.load();
}

uint64_t PikaServer::accumulative_connections() {
  return statistic_data_.accumulative_connections.load();
}

void PikaServer::incr_accumulative_connections() {
  ++statistic_data_.accumulative_connections;
}

// only one thread invoke this right now
void PikaServer::ResetLastSecQuerynum() {
  uint64_t last_query = statistic_data_.last_thread_querynum.load();
  uint64_t cur_query = statistic_data_.thread_querynum.load();
  uint64_t last_time_us = statistic_data_.last_time_us.load();
  if (cur_query < last_query) {
    cur_query = last_query;
  }
  uint64_t delta_query = cur_query - last_query;
  uint64_t cur_time_us = slash::NowMicros();
  if (cur_time_us <= last_time_us) {
    cur_time_us = last_time_us + 1;
  }
  uint64_t delta_time_us = cur_time_us - last_time_us;
  statistic_data_.last_sec_thread_querynum.store(delta_query
       * 1000000 / (delta_time_us));
  statistic_data_.last_thread_querynum.store(cur_query);
  statistic_data_.last_time_us.store(cur_time_us);
}

void PikaServer::UpdateQueryNumAndExecCountTable(const std::string& command) {
  std::string cmd(command);
  statistic_data_.thread_querynum++;
  statistic_data_.exec_count_table[slash::StringToUpper(cmd)]++;
}

std::unordered_map<std::string, uint64_t> PikaServer::ServerExecCountTable() {
  std::unordered_map<std::string, uint64_t> res;
  for (auto& cmd : statistic_data_.exec_count_table) {
    res[cmd.first] = cmd.second.load();
  }
  return res;
}

int PikaServer::SendToPeer() {
  return g_pika_rm->ConsumeWriteQueue();
}

void PikaServer::SignalAuxiliary() {
  pika_auxiliary_thread_->mu_.Lock();
  pika_auxiliary_thread_->cv_.Signal();
  pika_auxiliary_thread_->mu_.Unlock();
}

Status PikaServer::TriggerSendBinlogSync() {
  return g_pika_rm->WakeUpBinlogSync();
}

int PikaServer::PubSubNumPat() {
  return pika_pubsub_thread_->PubSubNumPat();
}

int PikaServer::Publish(const std::string& channel, const std::string& msg) {
  int receivers = pika_pubsub_thread_->Publish(channel, msg);
  return receivers;
}

int PikaServer::UnSubscribe(std::shared_ptr<pink::PinkConn> conn,
                            const std::vector<std::string>& channels,
                            bool pattern,
                            std::vector<std::pair<std::string, int>>* result) {
  int subscribed = pika_pubsub_thread_->UnSubscribe(conn, channels, pattern, result);
  return subscribed;
}

void PikaServer::Subscribe(std::shared_ptr<pink::PinkConn> conn,
                           const std::vector<std::string>& channels,
                           bool pattern,
                           std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->Subscribe(conn, channels, pattern, result);
}

void PikaServer::PubSubChannels(const std::string& pattern,
                      std::vector<std::string >* result) {
  pika_pubsub_thread_->PubSubChannels(pattern, result);
}

void PikaServer::PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->PubSubNumSub(channels, result);
}

int PikaServer::SendRedisCommand(const std::string& command, const std::string& key) {
  // Send command
  size_t idx = std::hash<std::string>()(key) % redis_senders_.size();
  redis_senders_[idx]->SendRedisCommand(command);
  return 0;
}

void PikaServer::RetransmitData(const std::string& path) {

  blackwidow::BlackWidow *db = new blackwidow::BlackWidow();
  rocksdb::Status s = db->Open(g_pika_server->bw_options(), path);

  if (!s.ok()) {
    LOG(FATAL) << "open received database error: " << s.ToString();
    return;
  }

  // Init SenderThread
  int thread_num = g_pika_conf->redis_sender_num();
  std::string target_host = g_pika_conf->target_redis_host();
  int target_port = g_pika_conf->target_redis_port();
  std::string target_pwd = g_pika_conf->target_redis_pwd();

  LOG(INFO) << "open received database success, start retransmit data to redis("
    << target_host << ":" << target_port << ")";

  std::vector<PikaSender*> pika_senders;
  std::vector<MigratorThread*> migrators;

  for (int i = 0; i < thread_num; i++) {
    pika_senders.emplace_back(new PikaSender(target_host, target_port, target_pwd));
  }
  migrators.emplace_back(new MigratorThread(db, &pika_senders, blackwidow::kStrings, thread_num));
  migrators.emplace_back(new MigratorThread(db, &pika_senders, blackwidow::kLists, thread_num));
  migrators.emplace_back(new MigratorThread(db, &pika_senders, blackwidow::kHashes, thread_num));
  migrators.emplace_back(new MigratorThread(db, &pika_senders, blackwidow::kSets, thread_num));
  migrators.emplace_back(new MigratorThread(db, &pika_senders, blackwidow::kZSets, thread_num));

  for (size_t i = 0; i < pika_senders.size(); i++) {
    pika_senders[i]->StartThread();
  }
  for (size_t i = 0; i < migrators.size(); i++) {
    migrators[i]->StartThread();
  }

  for (size_t i = 0; i < migrators.size(); i++) {
    migrators[i]->JoinThread();
  }
  for (size_t i = 0; i < pika_senders.size(); i++) {
    pika_senders[i]->Stop();
  }
  for (size_t i = 0; i < pika_senders.size(); i++) {
    pika_senders[i]->JoinThread();
  }

  int64_t replies = 0, records = 0;
  for (size_t i = 0; i < migrators.size(); i++) {
    records += migrators[i]->num();
    delete migrators[i];
  }
  migrators.clear();
  for (size_t i = 0; i < pika_senders.size(); i++) {
    replies += pika_senders[i]->elements();
    delete pika_senders[i];
  }
  pika_senders.clear();

  LOG(INFO) << "=============== Retransmit Finish =====================";
  LOG(INFO) << "Total records : " << records << " have been Scaned";
  LOG(INFO) << "Total replies : " << replies << " received from redis server";
  LOG(INFO) << "=======================================================";
}

/******************************* PRIVATE *******************************/

void PikaServer::DoTimingTask() {
  // Maybe schedule compactrange
  AutoCompactRange();
  // Purge log
  AutoPurge();
  // Delete expired dump
  AutoDeleteExpiredDump();
  // Cheek Rsync Status
  AutoKeepAliveRSync();
}

void PikaServer::AutoCompactRange() {
  struct statfs disk_info;
  int ret = statfs(g_pika_conf->db_path().c_str(), &disk_info);
  if (ret == -1) {
    LOG(WARNING) << "statfs error: " << strerror(errno);
    return;
  }

  uint64_t total_size = disk_info.f_bsize * disk_info.f_blocks;
  uint64_t free_size = disk_info.f_bsize * disk_info.f_bfree;
  std::string ci = g_pika_conf->compact_interval();
  std::string cc = g_pika_conf->compact_cron();

  if (ci != "") {
    std::string::size_type slash = ci.find("/");
    int interval = std::atoi(ci.substr(0, slash).c_str());
    int usage = std::atoi(ci.substr(slash+1).c_str());
    struct timeval now;
    gettimeofday(&now, NULL);
    if (last_check_compact_time_.tv_sec == 0 ||
      now.tv_sec - last_check_compact_time_.tv_sec >= interval * 3600) {
      gettimeofday(&last_check_compact_time_, NULL);
      if (((double)free_size / total_size) * 100 >= usage) {
        Status s = DoSameThingSpecificTable(TaskType::kCompactAll);
        if (s.ok()) {
          LOG(INFO) << "[Interval]schedule compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
        } else {
          LOG(INFO) << "[Interval]schedule compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
            << "MB, error: " << s.ToString();
        }
      } else {
        LOG(WARNING) << "compact-interval failed, because there is not enough disk space left, freesize"
          << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
      }
    }
    return;
  }

  if (cc != "") {
    bool have_week = false;
    std::string compact_cron, week_str;
    int slash_num = count(cc.begin(), cc.end(), '/');
    if (slash_num == 2) {
      have_week = true;
      std::string::size_type first_slash = cc.find("/");
      week_str = cc.substr(0, first_slash);
      compact_cron = cc.substr(first_slash + 1);
    } else {
      compact_cron = cc;
    }

    std::string::size_type colon = compact_cron.find("-");
    std::string::size_type underline = compact_cron.find("/");
    int week = have_week ? (std::atoi(week_str.c_str()) % 7) : 0;
    int start = std::atoi(compact_cron.substr(0, colon).c_str());
    int end = std::atoi(compact_cron.substr(colon+1, underline).c_str());
    int usage = std::atoi(compact_cron.substr(underline+1).c_str());
    std::time_t t = std::time(nullptr);
    std::tm* t_m = std::localtime(&t);

    bool in_window = false;
    if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
      in_window = have_week ? (week == t_m->tm_wday) : true;
    } else if (start > end && ((t_m->tm_hour >= start && t_m->tm_hour < 24) ||
          (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
      in_window = have_week ? false : true;
    } else {
      have_scheduled_crontask_ = false;
    }

    if (!have_scheduled_crontask_ && in_window) {
      if (((double)free_size / total_size) * 100 >= usage) {
        Status s = DoSameThingEveryPartition(TaskType::kCompactAll);
        if (s.ok()) {
          LOG(INFO) << "[Cron]schedule compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
        } else {
          LOG(INFO) << "[Cron]schedule compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
            << "MB, error: " << s.ToString();
        }
        have_scheduled_crontask_ = true;
      } else {
        LOG(WARNING) << "compact-cron failed, because there is not enough disk space left, freesize"
          << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
      }
    }
  }
}

void PikaServer::AutoPurge() {
  DoSameThingEveryPartition(TaskType::kPurgeLog);
}

void PikaServer::AutoDeleteExpiredDump() {
  std::string db_sync_prefix = g_pika_conf->bgsave_prefix();
  std::string db_sync_path = g_pika_conf->bgsave_path();
  int expiry_days = g_pika_conf->expire_dump_days();
  std::vector<std::string> dump_dir;

  // Never expire
  if (expiry_days <= 0) {
    return;
  }

  // Dump is not exist
  if (!slash::FileExists(db_sync_path)) {
    return;
  }

  // Directory traversal
  if (slash::GetChildren(db_sync_path, dump_dir) != 0) {
    return;
  }
  // Handle dump directory
  for (size_t i = 0; i < dump_dir.size(); i++) {
    if (dump_dir[i].substr(0, db_sync_prefix.size()) != db_sync_prefix || dump_dir[i].size() != (db_sync_prefix.size() + 8)) {
      continue;
    }

    std::string str_date = dump_dir[i].substr(db_sync_prefix.size(), (dump_dir[i].size() - db_sync_prefix.size()));
    char *end = NULL;
    std::strtol(str_date.c_str(), &end, 10);
    if (*end != 0) {
      continue;
    }

    // Parse filename
    int dump_year = std::atoi(str_date.substr(0, 4).c_str());
    int dump_month = std::atoi(str_date.substr(4, 2).c_str());
    int dump_day = std::atoi(str_date.substr(6, 2).c_str());

    time_t t = time(NULL);
    struct tm *now = localtime(&t);
    int now_year = now->tm_year + 1900;
    int now_month = now->tm_mon + 1;
    int now_day = now->tm_mday;

    struct tm dump_time, now_time;

    dump_time.tm_year = dump_year;
    dump_time.tm_mon = dump_month;
    dump_time.tm_mday = dump_day;
    dump_time.tm_hour = 0;
    dump_time.tm_min = 0;
    dump_time.tm_sec = 0;

    now_time.tm_year = now_year;
    now_time.tm_mon = now_month;
    now_time.tm_mday = now_day;
    now_time.tm_hour = 0;
    now_time.tm_min = 0;
    now_time.tm_sec = 0;

    long dump_timestamp = mktime(&dump_time);
    long now_timestamp = mktime(&now_time);
    // How many days, 1 day = 86400s
    int interval_days = (now_timestamp - dump_timestamp) / 86400;

    if (interval_days >= expiry_days) {
      std::string dump_file = db_sync_path + dump_dir[i];
      if (CountSyncSlaves() == 0) {
        LOG(INFO) << "Not syncing, delete dump file: " << dump_file;
        slash::DeleteDirIfExist(dump_file);
      } else {
        LOG(INFO) << "Syncing, can not delete " << dump_file << " dump file";
      }
    }
  }
}

void PikaServer::AutoKeepAliveRSync() {
  if (!pika_rsync_service_->CheckRsyncAlive()) {
    LOG(WARNING) << "The Rsync service is down, Try to restart";
    pika_rsync_service_->StartRsync();
  }
}

void PikaServer::InitBlackwidowOptions() {

  // For rocksdb::Options
  bw_options_.options.create_if_missing = true;
  bw_options_.options.keep_log_file_num = 10;
  bw_options_.options.max_manifest_file_size = 64 * 1024 * 1024;
  bw_options_.options.max_log_file_size = 512 * 1024 * 1024;

  bw_options_.options.write_buffer_size =
      g_pika_conf->write_buffer_size();
  bw_options_.options.write_buffer_manager.reset(
          new rocksdb::WriteBufferManager(g_pika_conf->max_write_buffer_size()));
  bw_options_.options.target_file_size_base =
      g_pika_conf->target_file_size_base();
  bw_options_.options.max_background_flushes =
      g_pika_conf->max_background_flushes();
  bw_options_.options.max_background_compactions =
      g_pika_conf->max_background_compactions();
  bw_options_.options.max_open_files =
      g_pika_conf->max_cache_files();
  bw_options_.options.max_bytes_for_level_multiplier =
      g_pika_conf->max_bytes_for_level_multiplier();
  bw_options_.options.optimize_filters_for_hits =
      g_pika_conf->optimize_filters_for_hits();
  bw_options_.options.level_compaction_dynamic_level_bytes =
      g_pika_conf->level_compaction_dynamic_level_bytes();


  if (g_pika_conf->compression() == "none") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kNoCompression;
  } else if (g_pika_conf->compression() == "snappy") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kSnappyCompression;
  } else if (g_pika_conf->compression() == "zlib") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kZlibCompression;
  }

  // For rocksdb::BlockBasedTableOptions
  bw_options_.table_options.block_size = g_pika_conf->block_size();
  bw_options_.table_options.cache_index_and_filter_blocks =
      g_pika_conf->cache_index_and_filter_blocks();
  bw_options_.block_cache_size = g_pika_conf->block_cache();
  bw_options_.share_block_cache = g_pika_conf->share_block_cache();

  if (bw_options_.block_cache_size == 0) {
    bw_options_.table_options.no_block_cache = true;
  } else if (bw_options_.share_block_cache) {
    bw_options_.table_options.block_cache =
      rocksdb::NewLRUCache(bw_options_.block_cache_size);
  }

  // For Blackwidow small compaction
  bw_options_.statistics_max_size = g_pika_conf->max_cache_statistic_keys();
  bw_options_.small_compaction_threshold =
      g_pika_conf->small_compaction_threshold();
}
