// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_THREAD_H_  // 防止重复包含该头文件
#define PIKA_REPL_SERVER_THREAD_H_

#include "net/src/holy_thread.h"  // 引入线程管理库

#include "include/pika_repl_server_conn.h"  // 引入复制服务器连接

// PikaReplServerThread 类，继承自 HolyThread 用于处理复制服务器的线程
class PikaReplServerThread : public net::HolyThread {
 public:
  // 构造函数，初始化时设置 IP 地址、端口和定时器周期
  PikaReplServerThread(const std::set<std::string>& ips, int port, int cron_interval);

  // 析构函数
  ~PikaReplServerThread() override = default;

  // 启动监听端口，接收客户端请求
  int ListenPort();

 private:
  // 连接工厂类，用于生成新的连接实例
  class ReplServerConnFactory : public net::ConnFactory {
   public:
    explicit ReplServerConnFactory(PikaReplServerThread* binlog_receiver) : binlog_receiver_(binlog_receiver) {}

    // 创建一个新的连接对象
    std::shared_ptr<net::NetConn> NewNetConn(int connfd, const std::string& ip_port, net::Thread* thread,
                                             void* worker_specific_data, net::NetMultiplexer* net) const override {
      // 返回一个 PikaReplServerConn 类型的连接对象
      return std::static_pointer_cast<net::NetConn>(
          std::make_shared<PikaReplServerConn>(connfd, ip_port, thread, binlog_receiver_, net));
    }

   private:
    PikaReplServerThread* binlog_receiver_ = nullptr;  // 关联的 binlog 接收器
  };

  // 处理客户端连接关闭的回调处理类
  class ReplServerHandle : public net::ServerHandle {
   public:
    // 处理连接关闭的函数
    void FdClosedHandle(int fd, const std::string& ip_port) const override;
  };

  ReplServerConnFactory conn_factory_;  // 连接工厂
  ReplServerHandle handle_;             // 连接关闭处理类
  int port_ = 0;                        // 服务器监听的端口
  uint64_t serial_ = 0;                 // 序列号，可能用于标识连接
};

#endif
