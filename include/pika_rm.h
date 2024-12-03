// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RM_H_
#define PIKA_RM_H_

#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "pstd/include/pstd_status.h"

#include "include/pika_binlog_reader.h"
#include "include/pika_consensus.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_slave_node.h"
#include "include/pika_stable_log.h"
#include "include/rsync_client.h"

// 常量定义
#define kBinlogSendPacketNum 40  // 每次发送的 Binlog 包数量
#define kBinlogSendBatchNum 100  // 批量发送的 Binlog 数量

// unit seconds
#define kSendKeepAliveTimeout (2 * 1000000)   // 发送端保持连接的超时时间，单位：微秒
#define kRecvKeepAliveTimeout (20 * 1000000)  // 接收端保持连接的超时时间，单位：微秒

// SyncDB 类用于管理数据库同步的基本信息，
// 包括数据库的名称和数据库同步信息。
class SyncDB {
 public:
  // 构造函数，传入数据库名称进行初始化
  SyncDB(const std::string& db_name);

  // 虚拟析构函数，确保派生类可以正确清理资源
  virtual ~SyncDB() = default;

  // 获取数据库同步信息
  DBInfo& SyncDBInfo() { return db_info_; }

  // 获取数据库名称
  std::string DBName();

 protected:
  DBInfo db_info_;  // 存储数据库信息
};

// SyncMasterDB 类继承自 SyncDB，主要用于处理主数据库的同步操作。
// 包含管理从节点、处理 Binlog 同步、获取从节点状态等功能。
class SyncMasterDB : public SyncDB {
 public:
  // 构造函数，初始化主数据库名称
  SyncMasterDB(const std::string& db_name);

  // 添加一个新的从节点
  pstd::Status AddSlaveNode(const std::string& ip, int port, int session_id);

  // 移除指定的从节点
  pstd::Status RemoveSlaveNode(const std::string& ip, int port);

  // 激活从节点的 Binlog 同步
  pstd::Status ActivateSlaveBinlogSync(const std::string& ip, int port, const LogOffset& offset);

  // 激活从节点的数据库同步
  pstd::Status ActivateSlaveDbSync(const std::string& ip, int port);

  // 将 Binlog 同步到从节点的写队列中
  pstd::Status SyncBinlogToWq(const std::string& ip, int port);

  // 获取从节点的 Binlog 同步信息
  pstd::Status GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset,
                                      BinlogOffset* acked_offset);

  // 获取从节点的状态信息
  pstd::Status GetSlaveState(const std::string& ip, int port, SlaveState* slave_state);

  // 设置从节点的最后接收时间
  pstd::Status SetLastRecvTime(const std::string& ip, int port, uint64_t time);

  // 获取安全的 Binlog 清理点
  pstd::Status GetSafetyPurgeBinlog(std::string* safety_purge);

  // 唤醒从节点的 Binlog 同步
  pstd::Status WakeUpSlaveBinlogSync();

  // 检查从节点同步是否超时
  pstd::Status CheckSyncTimeout(uint64_t now);

  // 获取从节点的会话 ID
  pstd::Status GetSlaveNodeSession(const std::string& ip, int port, int32_t* session);

  // 获取当前从节点的数量
  int GetNumberOfSlaveNode();

  // 执行 Binlog 清理
  bool BinlogCloudPurge(uint32_t index);

  // 检查从节点是否存在
  bool CheckSlaveNodeExist(const std::string& ip, int port);

  // 调试用，获取当前状态字符串
  std::string ToStringStatus();

  // 生成新的会话 ID
  int32_t GenSessionId();

  // 检查给定的会话 ID 是否有效
  bool CheckSessionId(const std::string& ip, int port, const std::string& db_name, int session_id);

  // 一致性协议相关操作
  pstd::Status ConsensusUpdateSlave(const std::string& ip, int port, const LogOffset& start, const LogOffset& end);
  pstd::Status ConsensusProposeLog(const std::shared_ptr<Cmd>& cmd_ptr);
  pstd::Status ConsensusProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute);
  LogOffset ConsensusCommittedIndex();  // 获取一致性协议中已提交的日志偏移量
  LogOffset ConsensusLastIndex();       // 获取一致性协议中最后一条日志的偏移量

  // 获取稳定日志记录器
  std::shared_ptr<StableLog> StableLogger() { return coordinator_.StableLogger(); }

  // 获取 Binlog 记录器
  std::shared_ptr<Binlog> Logger() {
    if (!coordinator_.StableLogger()) {
      return nullptr;
    }
    return coordinator_.StableLogger()->Logger();
  }

 private:
  // 需要持有 slave_mu_ 锁来读取 Binlog 文件并同步到写队列
  pstd::Status ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr);

  // 获取指定的从节点
  std::shared_ptr<SlaveNode> GetSlaveNode(const std::string& ip, int port);

  // 获取所有从节点的列表
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> GetAllSlaveNodes();

  // 用于保护会话 ID 的互斥锁
  pstd::Mutex session_mu_;
  int32_t session_id_ = 0;  // 当前会话 ID

  // 一致性协议协调器
  ConsensusCoordinator coordinator_;
};

// SyncSlaveDB 类继承自 SyncDB，主要用于处理从数据库的同步操作。
// 包含激活/停用同步、检查同步超时、获取同步信息等功能。
class SyncSlaveDB : public SyncDB {
 public:
  // 构造函数，初始化从数据库名称
  SyncSlaveDB(const std::string& db_name);

  // 激活从节点同步，传入主节点信息和复制状态
  void Activate(const RmNode& master, const ReplState& repl_state);

  // 停用从节点同步
  void Deactivate();

  // 设置从节点的最后接收时间
  void SetLastRecvTime(uint64_t time);

  // 设置复制状态
  void SetReplState(const ReplState& repl_state);

  // 获取当前复制状态
  ReplState State();

  // 检查同步是否超时
  pstd::Status CheckSyncTimeout(uint64_t now);

  // 获取从节点同步信息（用于显示）
  pstd::Status GetInfo(std::string* info);

  // 调试用，获取当前状态字符串
  std::string ToStringStatus();

  // 获取本地 IP 地址
  std::string LocalIp();

  // 获取主节点的会话 ID
  int32_t MasterSessionId();

  // 获取主节点的 IP 地址
  const std::string& MasterIp();

  // 获取主节点的端口号
  int MasterPort();

  // 设置主节点的会话 ID
  void SetMasterSessionId(int32_t session_id);

  // 设置本地 IP 地址
  void SetLocalIp(const std::string& local_ip);

  // 停止 Rsync 同步操作
  void StopRsync();

  // 激活 Rsync 同步操作
  pstd::Status ActivateRsync();

  // 检查 Rsync 是否退出
  bool IsRsyncExited() { return rsync_cli_->IsExitedFromRunning(); }

 private:
  std::unique_ptr<rsync::RsyncClient> rsync_cli_;  // Rsync 客户端
  int32_t rsync_init_retry_count_{0};              // Rsync 初始化重试次数

  // 用于保护数据库同步状态的互斥锁
  pstd::Mutex db_mu_;

  // 主节点信息
  RmNode m_info_;
  // 复制状态
  ReplState repl_state_{kNoConnect};

  // 本地 IP 地址
  std::string local_ip_;
};

// PikaReplicaManager 类管理主从数据库的复制任务和同步操作，
// 包含启动/停止复制，处理复制请求，更新复制状态等功能。
// 此类还负责调度和管理写队列，及相关的后台任务。
class PikaReplicaManager {
 public:
  // 构造函数，初始化复制管理器
  PikaReplicaManager();

  // 析构函数，释放相关资源
  ~PikaReplicaManager() = default;

  // Cmd 类的友元，允许 Cmd 类访问 PikaReplicaManager 的私有成员
  friend Cmd;

  // 启动复制管理器
  void Start();

  // 停止复制管理器
  void Stop();

  // 检查主数据库的同步是否完成
  bool CheckMasterSyncFinished();

  // 激活从数据库的同步
  pstd::Status ActivateSyncSlaveDB(const RmNode& node, const ReplState& repl_state);

  // ---------------------- Pika Repl Client 线程相关 ----------------------

  // 发送元数据同步请求
  pstd::Status SendMetaSyncRequest();

  // 发送移除从节点的请求
  pstd::Status SendRemoveSlaveNodeRequest(const std::string& table);

  // 尝试发送同步请求
  pstd::Status SendTrySyncRequest(const std::string& db_name);

  // 发送数据库同步请求
  pstd::Status SendDBSyncRequest(const std::string& db_name);

  // 发送 Binlog 同步确认请求
  pstd::Status SendBinlogSyncAckRequest(const std::string& table, const LogOffset& ack_start, const LogOffset& ack_end,
                                        bool is_first_send = false);

  // 关闭与某个从节点的连接
  pstd::Status CloseReplClientConn(const std::string& ip, int32_t port);

  // ---------------------- Pika Repl Server 线程相关 ----------------------

  // 向从节点请求 Binlog 数据
  pstd::Status SendSlaveBinlogChipsRequest(const std::string& ip, int port, const std::vector<WriteTask>& tasks);

  // ---------------------- SyncMasterDB 相关 ----------------------

  // 根据数据库信息获取对应的 SyncMasterDB 实例
  std::shared_ptr<SyncMasterDB> GetSyncMasterDBByName(const DBInfo& p_info);

  // ---------------------- SyncSlaveDB 相关 ----------------------

  // 根据数据库信息获取对应的 SyncSlaveDB 实例
  std::shared_ptr<SyncSlaveDB> GetSyncSlaveDBByName(const DBInfo& p_info);

  // 执行从数据库状态机逻辑
  pstd::Status RunSyncSlaveDBStateMachine();

  // 检查同步是否超时
  pstd::Status CheckSyncTimeout(uint64_t now);

  // ---------------------- 数据库角色检查相关 ----------------------

  // 检查某个从数据库的状态，通常用于 pkcluster 信息命令
  static bool CheckSlaveDBState(const std::string& ip, int port);

  // 查找公共主数据库
  void FindCommonMaster(std::string* master);

  // 获取 Pika 复制管理器的状态信息，用于调试
  void RmStatus(std::string* debug_info);

  // 检查数据库角色，可能用于区分主库和从库
  pstd::Status CheckDBRole(const std::string& table, int* role);

  // 处理连接丢失的情况
  pstd::Status LostConnection(const std::string& ip, int port);

  // 停止从数据库的同步
  pstd::Status DeactivateSyncSlaveDB(const std::string& ip, int port);

  // ---------------------- Binlog 同步相关 ----------------------

  // 更新同步 Binlog 状态并尝试发送下一个 Binlog
  pstd::Status UpdateSyncBinlogStatus(const RmNode& slave, const LogOffset& offset_start, const LogOffset& offset_end);

  // 唤醒 Binlog 同步
  pstd::Status WakeUpBinlogSync();

  // ---------------------- 写队列相关 ----------------------

  // 向指定从节点的写队列中添加任务
  void ProduceWriteQueue(const std::string& ip, int port, std::string db_name, const std::vector<WriteTask>& tasks);

  // 从指定数据库的写队列中删除某个项
  void DropItemInOneWriteQueue(const std::string& ip, int port, const std::string& db_name);

  // 从写队列中删除指定节点的所有任务
  void DropItemInWriteQueue(const std::string& ip, int port);

  // 消费写队列中的任务
  int ConsumeWriteQueue();

  // ---------------------- 背景任务调度 ----------------------

  // 调度复制服务器后台任务
  void ScheduleReplServerBGTask(net::TaskFunc func, void* arg);

  // 调度复制客户端后台任务
  void ScheduleReplClientBGTask(net::TaskFunc func, void* arg);

  // 调度写 Binlog 任务
  void ScheduleWriteBinlogTask(const std::string& db_name, const std::shared_ptr<InnerMessage::InnerResponse>& res,
                               const std::shared_ptr<net::PbConn>& conn, void* res_private_data);

  // 调度写数据库任务
  void ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const std::string& db_name);

  // 按照数据库名称调度复制客户端后台任务
  void ScheduleReplClientBGTaskByDBName(net::TaskFunc, void* arg, const std::string& db_name);

  // 移除复制服务器的客户端连接
  void ReplServerRemoveClientConn(int fd);

  // 更新复制服务器客户端连接映射
  void ReplServerUpdateClientConnMap(const std::string& ip_port, int fd);

  // 获取数据库操作锁
  std::shared_mutex& GetDBLock() { return dbs_rw_; }

  // 锁定数据库操作
  void DBLock() { dbs_rw_.lock(); }

  // 解锁数据库操作
  void DBUnlock() { dbs_rw_.unlock(); }

  // 获取所有 SyncMasterDB 实例
  std::unordered_map<DBInfo, std::shared_ptr<SyncMasterDB>, hash_db_info>& GetSyncMasterDBs() {
    return sync_master_dbs_;
  }

  // 获取所有 SyncSlaveDB 实例
  std::unordered_map<DBInfo, std::shared_ptr<SyncSlaveDB>, hash_db_info>& GetSyncSlaveDBs() { return sync_slave_dbs_; }

  // 获取指定数据库名的未完成的异步写任务数量
  int32_t GetUnfinishedAsyncWriteDBTaskCount(const std::string& db_name) {
    return pika_repl_client_->GetUnfinishedAsyncWriteDBTaskCount(db_name);
  }

 private:
  // 初始化数据库
  void InitDB();

  // 选择本地 IP 地址，通常用于根据远程 IP 地址选择合适的本地地址
  pstd::Status SelectLocalIp(const std::string& remote_ip, int remote_port, std::string* local_ip);

  // ---------------------- 数据成员 ----------------------

  // 数据库操作的共享锁
  std::shared_mutex dbs_rw_;

  // 存储 SyncMasterDB 实例的哈希表，键为 DBInfo，值为 SyncMasterDB 对象
  std::unordered_map<DBInfo, std::shared_ptr<SyncMasterDB>, hash_db_info> sync_master_dbs_;

  // 存储 SyncSlaveDB 实例的哈希表，键为 DBInfo，值为 SyncSlaveDB 对象
  std::unordered_map<DBInfo, std::shared_ptr<SyncSlaveDB>, hash_db_info> sync_slave_dbs_;

  // 用于保护写队列的互斥锁
  pstd::Mutex write_queue_mu_;

  // 存储写队列的哈希表，键为 "ip + port"，值为每个数据库的写队列
  std::unordered_map<std::string, std::unordered_map<std::string, std::queue<WriteTask>>> write_queues_;

  // Pika 复制客户端实例
  std::unique_ptr<PikaReplClient> pika_repl_client_;

  // Pika 复制服务器实例
  std::unique_ptr<PikaReplServer> pika_repl_server_;
};

#endif  //  PIKA_RM_H
