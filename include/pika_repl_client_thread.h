// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_THREAD_H_  // 防止重复包含该头文件
#define PIKA_REPL_CLIENT_THREAD_H_

#include <memory>  // 引入智能指针库
#include <string>  // 引入字符串库

#include "include/pika_repl_client_conn.h"  // 引入 Pika 复制客户端连接类

#include "net/include/client_thread.h"  // 网络客户端线程管理
#include "net/include/net_conn.h"       // 网络连接管理

// PikaReplClientThread 继承自 net::ClientThread，用于管理复制客户端的工作线程
class PikaReplClientThread : public net::ClientThread {
 public:
  // 构造函数，初始化复制客户端线程，传入定时器周期和保持连接超时
  PikaReplClientThread(int cron_interval, int keepalive_timeout);

  // 默认析构函数
  ~PikaReplClientThread() override = default;

 private:
  // 连接工厂类，用于创建 PikaReplClientConn 对象
  class ReplClientConnFactory : public net::ConnFactory {
   public:
    // 创建新的网络连接，返回 PikaReplClientConn 对象
    std::shared_ptr<net::NetConn> NewNetConn(int connfd, const std::string& ip_port, net::Thread* thread,
                                             void* worker_specific_data, net::NetMultiplexer* net) const override {
      return std::static_pointer_cast<net::NetConn>(  // 强制转换为 PikaReplClientConn 类型
          std::make_shared<PikaReplClientConn>(connfd, ip_port, thread, worker_specific_data, net));
    }
  };

  // 客户端处理类
  class ReplClientHandle : public net::ClientHandle {
   public:
    // 定时器处理函数，暂时未实现
    void CronHandle() const override {}

    // 文件描述符超时处理函数
    void FdTimeoutHandle(int fd, const std::string& ip_port) const override;

    // 文件描述符关闭处理函数
    void FdClosedHandle(int fd, const std::string& ip_port) const override;

    // 访问处理函数，始终返回 true，表示允许访问
    bool AccessHandle(std::string& ip) const override { return true; }

    // 创建工作线程特定数据，返回 0 表示成功
    int CreateWorkerSpecificData(void** data) const override { return 0; }

    // 删除工作线程特定数据，返回 0 表示成功
    int DeleteWorkerSpecificData(void* data) const override { return 0; }

    // 连接失败处理函数，暂时未实现
    void DestConnectFailedHandle(const std::string& ip_port, const std::string& reason) const override {}
  };

  // 连接工厂实例
  ReplClientConnFactory conn_factory_;

  // 客户端处理实例
  ReplClientHandle handle_;
};

#endif  // PIKA_REPL_CLIENT_THREAD_H_
