// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_PB_CONN_H_
#define NET_INCLUDE_PB_CONN_H_

#include <map>
#include <queue>
#include <string>

#include "google/protobuf/message.h"
#include "net/include/net_conn.h"
#include "net/include/net_define.h"
#include "pstd/include/pstd_status.h"

namespace net {

using pstd::Status;

// PbConn 类继承自 NetConn，表示基于 Protobuf 协议的网络连接。
// 它封装了 Protobuf 消息的收发逻辑，同时提供了对读写缓冲区的管理和协议处理的抽象。
class PbConn : public NetConn {
 public:
  // 定义写缓冲区结构
  struct WriteBuf {
    WriteBuf(const size_t item_pos = 0) : item_pos_(item_pos) {}  // 初始化队列索引位置
    std::queue<std::string> queue_;                               // 存储待发送的响应数据
    size_t item_pos_;                                             // 当前正在处理的队列位置
  };

  // 构造函数：初始化连接信息和所属线程
  PbConn(int fd, const std::string& ip_port, Thread* thread, NetMultiplexer* net_mpx = nullptr);

  // 析构函数：释放资源
  ~PbConn() override;

  // 覆盖父类方法：从缓冲区中读取请求数据
  ReadStatus GetRequest() override;

  // 覆盖父类方法：向客户端发送响应
  WriteStatus SendReply() override;

  // 调整缓冲区大小以优化内存使用
  void TryResizeBuffer() override;

  // 写入响应数据到写缓冲区
  int WriteResp(const std::string& resp) override;

  // 通知连接准备写入响应
  void NotifyWrite();

  // 通知关闭连接
  void NotifyClose();

  // 设置是否正在进行响应
  void set_is_reply(bool reply) override;

  // 获取是否正在进行响应的状态
  bool is_reply() override;

  // 缓冲区相关成员变量
  uint32_t header_len_{static_cast<uint32_t>(-1)};  // Protobuf 消息头长度
  char* rbuf_;                                      // 读缓冲区
  uint32_t cur_pos_{0};                             // 当前读缓冲区位置
  uint32_t rbuf_len_{0};                            // 缓冲区长度
  int32_t remain_packet_len_{0};                    // 剩余待处理的消息包长度

  // 当前连接状态，用于区分消息头和消息体的解析
  ConnStatus connStatus_{kHeader};

 protected:
  /**
   * @brief 处理接收到的 Protobuf 消息。
   *
   * 此方法需要子类实现，用于处理解析后的 Protobuf 消息。
   *
   * - 返回值：
   *   - 如果返回非 0（如 -1），服务器会关闭此连接。
   *   - 如果返回 0，表示逻辑正常，继续保持连接。
   *
   * - 错误分类：
   *   1. **协议解析错误**：
   *      - 收到的消息不是定义的 Protobuf 消息，无法继续解析。
   *      - 此时需要关闭连接（返回 -1）。
   *   2. **服务逻辑错误**：
   *      - 收到的消息是合法的 Protobuf 消息，但在业务处理时出错。
   *      - 此时应将错误信息写入 `res_`，返回 0 继续使用连接。
   *
   * - 发送响应：
   *   - 子类需自行构造 Protobuf 格式的响应消息，调用 `WriteResp` 写入响应数据。
   *   - 如果需要立即发送响应，还需调用 `NotifyWrite`。
   */
  virtual int DealMessage() = 0;

 private:
  pstd::Mutex resp_mu_;      // 保护响应写缓冲区的互斥锁
  WriteBuf write_buf_;       // 写缓冲区，用于存储待发送的响应数据
  pstd::Mutex is_reply_mu_;  // 保护响应状态的互斥锁
  int64_t is_reply_{0};      // 标识是否有响应待发送

  // 构建内部标签，用于附加到响应数据中（子类可实现）
  virtual void BuildInternalTag(const std::string& resp, std::string* tag);
};

}  // namespace net
#endif  // NET_INCLUDE_PB_CONN_H_
