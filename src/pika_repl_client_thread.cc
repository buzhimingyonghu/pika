// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_thread.h"

#include "include/pika_rm.h"
#include "include/pika_server.h"

#include "pstd/include/pstd_string.h"

extern PikaServer* g_pika_server;                      // 外部变量，指向 PikaServer 实例
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;  // 外部变量，指向 PikaReplicaManager 实例

// PikaReplClientThread 构造函数
PikaReplClientThread::PikaReplClientThread(int cron_interval, int keepalive_timeout)
    // 初始化父类 ClientThread，并传递参数
    : ClientThread(&conn_factory_, cron_interval, keepalive_timeout, &handle_, nullptr) {}

// ReplClientHandle 类中处理连接关闭的函数
void PikaReplClientThread::ReplClientHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  // 函数作用：当连接关闭时的处理函数
  // 功能：记录连接关闭事件并根据 IP 和端口判断是否需要执行重连操作

  LOG(INFO) << "ReplClient Close conn, fd=" << fd << ", ip_port=" << ip_port;  // 打印日志

  std::string ip;
  int port = 0;

  // 解析传入的 ip_port 字符串，提取出 ip 和 port
  if (!pstd::ParseIpPortString(ip_port, ip, port)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;  // 如果解析失败，输出警告日志
    return;
  }

  // 判断是否是与主服务器的连接断开，且当前状态不是错误状态
  if (ip == g_pika_server->master_ip() && port == g_pika_server->master_port() + kPortShiftReplServer &&
      PIKA_REPL_ERROR != g_pika_server->repl_state()) {  // 如果状态机处于错误状态，不进行重试
    LOG(WARNING) << "Master conn disconnect : " << ip_port << " try reconnect";  // 记录主服务器连接断开并尝试重连
    g_pika_server->ResetMetaSyncStatus();                                        // 重置元数据同步状态
  }

  // 更新元数据同步的时间戳
  g_pika_server->UpdateMetaSyncTimestamp();
};

// ReplClientHandle 类中处理连接超时的函数
void PikaReplClientThread::ReplClientHandle::FdTimeoutHandle(int fd, const std::string& ip_port) const {
  // 函数作用：当连接超时时的处理函数
  // 功能：记录连接超时事件并根据 IP 和端口判断是否需要执行重连操作

  LOG(INFO) << "ReplClient Timeout conn, fd=" << fd << ", ip_port=" << ip_port;  // 打印日志

  std::string ip;
  int port = 0;

  // 解析传入的 ip_port 字符串，提取出 ip 和 port
  if (!pstd::ParseIpPortString(ip_port, ip, port)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;  // 如果解析失败，输出警告日志
    return;
  }

  // 判断是否是与主服务器的连接超时，且当前状态不是错误状态，并且可以重试
  if (ip == g_pika_server->master_ip() && port == g_pika_server->master_port() + kPortShiftReplServer &&
      PIKA_REPL_ERROR != g_pika_server->repl_state() &&
      PikaReplicaManager::CheckSlaveDBState(
          ip, port)) {  // 如果状态机等于 kDBNoConnect（执行命令 'dbslaveof db no one'），则不重试
    LOG(WARNING) << "Master conn timeout : " << ip_port << " try reconnect";  // 记录主服务器连接超时并尝试重连
    g_pika_server->ResetMetaSyncStatus();                                     // 重置元数据同步状态
  }

  // 更新元数据同步的时间戳
  g_pika_server->UpdateMetaSyncTimestamp();
};
