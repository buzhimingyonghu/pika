// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DB_H_
#define PIKA_DB_H_

#include <shared_mutex>

#include "include/pika_command.h"
#include "lock_mgr.h"
#include "pika_cache.h"
#include "pika_define.h"
#include "storage/backupable.h"
#include "storage/storage.h"

class PikaCache;
class CacheInfo;
/*
 *Keyscan used
 */
struct KeyScanInfo {
  time_t start_time = 0;
  std::string s_start_time;
  int32_t duration = -3;
  std::vector<storage::KeyInfo> key_infos;  // the order is strings, hashes, lists, zsets, sets, streams
  bool key_scaning_ = false;
  KeyScanInfo()
      : s_start_time("0"),
        key_infos({{0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}}) {}
};

struct BgSaveInfo {
  bool bgsaving = false;
  time_t start_time = 0;
  std::string s_start_time;
  std::string path;
  LogOffset offset;
  BgSaveInfo() = default;
  void Clear() {
    bgsaving = false;
    path.clear();
    offset = LogOffset();
  }
};

struct DisplayCacheInfo {
  int status = 0;
  uint32_t cache_num = 0;
  uint64_t keys_num = 0;
  uint64_t used_memory = 0;
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t hits_per_sec = 0;
  uint64_t read_cmd_per_sec = 0;
  double hitratio_per_sec = 0.0;
  double hitratio_all = 0.0;
  uint64_t load_keys_per_sec = 0;
  uint64_t last_time_us = 0;
  uint64_t last_load_keys_num = 0;
  uint32_t waitting_load_keys_num = 0;
  DisplayCacheInfo& operator=(const DisplayCacheInfo& obj) {
    status = obj.status;
    cache_num = obj.cache_num;
    keys_num = obj.keys_num;
    used_memory = obj.used_memory;
    hits = obj.hits;
    misses = obj.misses;
    hits_per_sec = obj.hits_per_sec;
    read_cmd_per_sec = obj.read_cmd_per_sec;
    hitratio_per_sec = obj.hitratio_per_sec;
    hitratio_all = obj.hitratio_all;
    load_keys_per_sec = obj.load_keys_per_sec;
    last_time_us = obj.last_time_us;
    last_load_keys_num = obj.last_load_keys_num;
    waitting_load_keys_num = obj.waitting_load_keys_num;
    return *this;
  }
};

class DB : public std::enable_shared_from_this<DB>, public pstd::noncopyable {
 public:
  // 构造函数，初始化数据库名称、数据库路径和日志路径
  DB(std::string db_name, const std::string& db_path, const std::string& log_path);

  // 析构函数
  virtual ~DB();

  // 友元类声明，使这些类可以访问 DB 的私有成员
  friend class Cmd;
  friend class InfoCmd;
  friend class PkClusterInfoCmd;
  friend class PikaServer;

  /**
   * 升级数据库版本时清理数据的函数，特别是从 4.0.0 到 4.0.1 的版本升级需要调用该函数来清理数据。
   * 返回 true 表示成功，false 表示失败。
   * @see https://github.com/OpenAtomFoundation/pika/issues/2886
   */
  bool WashData();

  // 获取数据库的名称
  std::string GetDBName();

  // 获取数据库的存储对象
  std::shared_ptr<storage::Storage> storage() const;

  // 获取后台保存的元数据（如文件名、快照UUID）
  void GetBgSaveMetaData(std::vector<std::string>* fileNames, std::string* snapshot_uuid);

  // 启动后台保存数据库
  void BgSaveDB();

  // 设置数据库的 Binlog I/O 错误状态
  void SetBinlogIoError();

  // 解除数据库的 Binlog I/O 错误状态
  void SetBinlogIoErrorrelieve();

  // 检查是否存在 Binlog I/O 错误
  bool IsBinlogIoError();

  // 获取数据库的缓存对象
  std::shared_ptr<PikaCache> cache() const;

  // 获取数据库的读写锁
  std::shared_mutex& GetDBLock() { return dbs_rw_; }

  // 锁住数据库
  void DBLock() { dbs_rw_.lock(); }

  // 以共享模式锁住数据库
  void DBLockShared() { dbs_rw_.lock_shared(); }

  // 解锁数据库
  void DBUnlock() { dbs_rw_.unlock(); }

  // 释放共享锁
  void DBUnlockShared() { dbs_rw_.unlock_shared(); }

  // KeyScan 相关函数
  void KeyScan();                                    // 启动 Key 扫描
  bool IsKeyScaning();                               // 检查是否正在进行 Key 扫描
  void RunKeyScan();                                 // 执行 Key 扫描
  void StopKeyScan();                                // 停止 Key 扫描
  void ScanDatabase(const storage::DataType& type);  // 扫描数据库
  KeyScanInfo GetKeyScanInfo();                      // 获取 Key 扫描信息

  // 数据库压缩相关函数
  void Compact(const storage::DataType& type);  // 执行压缩
  void CompactRange(const storage::DataType& type, const std::string& start,
                    const std::string& end);                           // 压缩指定范围的数据
  void LongestNotCompactionSstCompact(const storage::DataType& type);  // 执行最长未压缩的 SST 文件压缩

  // 设置压缩范围的选项
  void SetCompactRangeOptions(const bool is_canceled);

  // 获取锁管理器
  std::shared_ptr<pstd::lock::LockMgr> LockMgr();

  /*
   * 缓存使用
   */
  DisplayCacheInfo GetCacheInfo();              // 获取缓存信息
  void UpdateCacheInfo(CacheInfo& cache_info);  // 更新缓存信息
  void ResetDisplayCacheInfo(int status);       // 重置缓存显示信息
  uint64_t cache_usage_;                        // 缓存使用量

  // 初始化数据库
  void Init();

  // 尝试更新主节点偏移量
  bool TryUpdateMasterOffset();

  /*
   * FlushDB 操作
   */
  bool FlushDBWithoutLock();                                        // 无锁刷新数据库
  bool ChangeDb(const std::string& new_path);                       // 更改数据库路径
  pstd::Status GetBgSaveUUID(std::string* snapshot_uuid);           // 获取后台保存的 UUID
  void PrepareRsync();                                              // 准备 Rsync
  bool IsBgSaving();                                                // 检查是否正在进行后台保存
  BgSaveInfo bgsave_info();                                         // 获取后台保存信息
  pstd::Status GetKeyNum(std::vector<storage::KeyInfo>* key_info);  // 获取键的数量

 private:
  bool opened_ = false;                // 数据库是否已打开
  std::string dbsync_path_;            // 数据库同步路径
  std::string db_name_;                // 数据库名称
  std::string db_path_;                // 数据库路径
  std::string snapshot_uuid_;          // 快照 UUID
  std::string log_path_;               // 日志路径
  std::string bgsave_sub_path_;        // 后台保存的子路径
  pstd::Mutex key_info_protector_;     // 键信息保护锁
  std::atomic<bool> binlog_io_error_;  // 是否发生了 Binlog I/O 错误
  std::shared_mutex dbs_rw_;           // 数据库的读写锁

  // 类的共享锁管理器
  std::shared_ptr<pstd::lock::LockMgr> lock_mgr_;

  // 存储对象
  std::shared_ptr<storage::Storage> storage_;

  // 缓存对象
  std::shared_ptr<PikaCache> cache_;

  /*
   * KeyScan 使用
   */
  static void DoKeyScan(void* arg);  // 执行 Key 扫描的静态函数
  void InitKeyScan();                // 初始化 Key 扫描
  pstd::Mutex key_scan_protector_;   // 键扫描保护锁
  KeyScanInfo key_scan_info_;        // 键扫描信息

  /*
   * 缓存使用
   */
  DisplayCacheInfo cache_info_;          // 缓存显示信息
  std::shared_mutex cache_info_rwlock_;  // 缓存信息的读写锁

  /*
   * BgSave 使用
   */
  static void DoBgSave(void* arg);  // 执行后台保存的静态函数
  bool RunBgsaveEngine();           // 启动后台保存引擎

  bool InitBgsaveEnv();                                   // 初始化后台保存环境
  bool InitBgsaveEngine();                                // 初始化后台保存引擎
  void ClearBgsave();                                     // 清除后台保存状态
  void FinishBgsave();                                    // 完成后台保存
  BgSaveInfo bgsave_info_;                                // 后台保存信息
  pstd::Mutex bgsave_protector_;                          // 后台保存保护锁
  std::shared_ptr<storage::BackupEngine> bgsave_engine_;  // 后台保存引擎
};

struct BgTaskArg {
  std::shared_ptr<DB> db;
};

#endif
