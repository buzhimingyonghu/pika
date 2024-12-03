// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_auxiliary_thread.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

using namespace std::chrono_literals;

PikaAuxiliaryThread::~PikaAuxiliaryThread() {
  StopThread();
  LOG(INFO) << "PikaAuxiliary thread " << thread_id() << " exit!!!";
}

void* PikaAuxiliaryThread::ThreadMain() {
  // 进入线程主循环，直到 should_stop() 返回 true，表明线程应该停止
  while (!should_stop()) {
    // 检查是否需要进行元数据同步
    if (g_pika_server->ShouldMetaSync()) {
      // 发送元数据同步请求
      g_pika_rm->SendMetaSyncRequest();
    } else if (g_pika_server->MetaSyncDone()) {
      // 如果元数据同步已完成，则运行同步从库状态机
      g_pika_rm->RunSyncSlaveDBStateMachine();
    }

    // 检查同步超时，pstd::NowMicros() 获取当前时间戳（微秒级）
    pstd::Status s = g_pika_rm->CheckSyncTimeout(pstd::NowMicros());
    if (!s.ok()) {
      // 如果同步超时，记录警告日志
      LOG(WARNING) << s.ToString();
    }

    // 检查是否处于领导保护模式
    g_pika_server->CheckLeaderProtectedMode();

    // TODO: 需要处理超时逻辑
    // 触发发送 binlog 同步请求
    s = g_pika_server->TriggerSendBinlogSync();
    if (!s.ok()) {
      // 如果 binlog 同步请求失败，记录警告日志
      LOG(WARNING) << s.ToString();
    }

    // 向对等服务器发送数据
    int res = g_pika_server->SendToPeer();
    if (res == 0) {
      // 如果没有数据发送，睡眠 100 毫秒
      std::unique_lock lock(mu_);
      cv_.wait_for(lock, 100ms);
    } else {
      // 发送了数据时，每1000次打印一次日志（这里的日志被注释掉了）
      // LOG_EVERY_N(INFO, 1000) << "Consume binlog number " << res;
    }
  }
  // 线程结束时返回 nullptr
  return nullptr;
}
