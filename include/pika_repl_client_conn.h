// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_CONN_H_  // 防止重复包含该头文件
#define PIKA_REPL_CLIENT_CONN_H_

#include "net/include/pb_conn.h"  // 引入 Protocol Buffer 网络连接基类

#include <memory>   // 引入智能指针库
#include <utility>  // 引入工具库

#include "include/pika_conf.h"      // 配置文件相关
#include "pika_inner_message.pb.h"  // 引入 Pika 内部消息的 Protocol Buffers 定义

class SyncMasterDB;  // 前向声明
class SyncSlaveDB;   // 前向声明

// PikaReplClientConn 继承自 net::PbConn，用于 Pika 复制客户端的具体连接操作
class PikaReplClientConn : public net::PbConn {
 public:
  // 构造函数，初始化网络连接，设置文件描述符、IP 地址、线程等信息
  PikaReplClientConn(int fd, const std::string& ip_port, net::Thread* thread, void* worker_specific_data,
                     net::NetMultiplexer* mpx);

  // 默认析构函数
  ~PikaReplClientConn() override = default;

  // 处理元数据同步响应的静态函数
  static void HandleMetaSyncResponse(void* arg);

  // 处理数据库同步响应的静态函数
  static void HandleDBSyncResponse(void* arg);

  // 处理尝试同步响应的静态函数
  static void HandleTrySyncResponse(void* arg);

  // 处理移除从节点响应的静态函数
  static void HandleRemoveSlaveNodeResponse(void* arg);

  // 检查数据库结构是否一致
  static bool IsDBStructConsistent(const std::vector<DBStruct>& current_dbs,
                                   const std::vector<DBStruct>& expect_tables);

  // 处理接收到的消息
  int DealMessage() override;

 private:
  // 根据数据库名称分发 Binlog 响应
  void DispatchBinlogRes(const std::shared_ptr<InnerMessage::InnerResponse>& response);
};

#endif
