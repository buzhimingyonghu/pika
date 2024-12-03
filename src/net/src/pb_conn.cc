// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/pb_conn.h"

#include <arpa/inet.h>
#include <string>

#include <glog/logging.h>

#include "net/include/net_define.h"
#include "net/include/net_stats.h"
#include "pstd/include/xdebug.h"

extern std::unique_ptr<net::NetworkStatistic> g_network_statistic;

namespace net {

// 构造函数：初始化连接和缓冲区
PbConn::PbConn(const int fd, const std::string& ip_port, Thread* thread, NetMultiplexer* mpx)
    : NetConn(fd, ip_port, thread, mpx),  // 初始化基类 NetConn
      write_buf_(0) {                     // 初始化写缓冲区
  // 分配接收缓冲区内存，大小为 PB_IOBUF_LEN
  rbuf_ = reinterpret_cast<char*>(malloc(sizeof(char) * PB_IOBUF_LEN));
  rbuf_len_ = PB_IOBUF_LEN;  // 设置缓冲区长度
}

// 析构函数：释放接收缓冲区内存
PbConn::~PbConn() {
  free(rbuf_);  // 释放接收缓冲区的内存
}

// 读取请求数据，并根据协议解析请求
// Msg格式为 [ length(COMMAND_HEADER_LENGTH) | body(length bytes) ]
// 步骤 1: kHeader 状态下，读取 COMMAND_HEADER_LENGTH 字节；
// 步骤 2: kPacket 状态下，读取完整的消息体。
ReadStatus PbConn::GetRequest() {
  while (true) {
    switch (connStatus_) {
      case kHeader: {
        // 读取命令头部信息，长度为 COMMAND_HEADER_LENGTH - cur_pos_（当前位置到头部长度的差）
        ssize_t nread = read(fd(), rbuf_ + cur_pos_, COMMAND_HEADER_LENGTH - cur_pos_);

        if (nread == -1) {  // 读取错误
          if (errno == EAGAIN) {
            return kReadHalf;  // 暂时无法读取，返回部分读取
          } else {
            return kReadError;  // 发生错误，返回读取错误
          }
        } else if (nread == 0) {  // 连接关闭
          return kReadClose;      // 连接已关闭
        } else {
          g_network_statistic->IncrReplInputBytes(nread);  // 统计输入字节
          cur_pos_ += nread;                               // 更新当前位置

          // 如果头部已读完，解析出包的总长度
          if (cur_pos_ == COMMAND_HEADER_LENGTH) {
            uint32_t integer = 0;
            memcpy(reinterpret_cast<char*>(&integer), rbuf_, sizeof(uint32_t));  // 从头部获取包体长度
            header_len_ = ntohl(integer);  // 将字节序从网络字节序转换为主机字节序
            remain_packet_len_ = static_cast<int32_t>(header_len_);  // 设置剩余数据长度
            connStatus_ = kPacket;                                   // 切换到包体状态
            continue;
          }
          return kReadHalf;  // 还没读取完整头部，返回部分读取
        }
      }

      case kPacket: {
        // 如果包体长度大于当前缓冲区大小，扩展缓冲区
        if (header_len_ > rbuf_len_ - COMMAND_HEADER_LENGTH) {
          uint32_t new_size = header_len_ + COMMAND_HEADER_LENGTH;

          // 如果新的缓冲区大小小于最大限制（kProtoMaxMessage），扩展缓冲区
          if (new_size < kProtoMaxMessage) {
            rbuf_ = reinterpret_cast<char*>(realloc(rbuf_, sizeof(char) * new_size));  // 扩展缓冲区
            if (!rbuf_) {                                                              // 内存分配失败
              return kFullError;                                                       // 返回缓冲区溢出错误
            }
            rbuf_len_ = new_size;  // 更新缓冲区长度
            LOG(INFO) << "Thread_id " << pthread_self() << " Expand rbuf to " << new_size << ", cur_pos_ " << cur_pos_;
          } else {
            return kFullError;  // 如果新的缓冲区大小超过最大限制，返回错误
          }
        }

        // 读取包体数据
        ssize_t nread = read(fd(), rbuf_ + cur_pos_, remain_packet_len_);

        if (nread == -1) {  // 读取错误
          if (errno == EAGAIN) {
            return kReadHalf;  // 暂时无法读取，返回部分读取
          } else {
            return kReadError;  // 发生错误，返回读取错误
          }
        } else if (nread == 0) {  // 连接关闭
          return kReadClose;      // 连接已关闭
        }

        g_network_statistic->IncrReplInputBytes(nread);     // 统计输入字节
        cur_pos_ += static_cast<uint32_t>(nread);           // 更新当前位置
        remain_packet_len_ -= static_cast<int32_t>(nread);  // 减少剩余包体数据长度

        // 如果数据包读取完毕，切换到完成状态
        if (remain_packet_len_ == 0) {
          connStatus_ = kComplete;  // 切换到完成状态
          continue;
        }
        return kReadHalf;  // 数据包还未读取完整，返回部分读取
      }

      case kComplete: {
        // 处理完整的消息
        if (DealMessage() != 0) {
          return kDealError;  // 如果处理消息出错，返回处理错误
        }
        // 完成消息处理后，切换回读取头部状态，准备接收下一个请求
        connStatus_ = kHeader;
        cur_pos_ = 0;     // 重置当前读取位置
        return kReadAll;  // 返回已读取所有数据
      }

      // 为了避免编译器警告添加的空的 case 分支
      case kBuildObuf:
        break;

      case kWriteObuf:
        break;
    }
  }

  return kReadHalf;  // 如果未能处理完，返回部分读取
}

// 发送回复数据到客户端
WriteStatus PbConn::SendReply() {
  ssize_t nwritten = 0;         // 用于记录每次写入的字节数
  size_t item_len;              // 存储当前要发送数据项的长度
  std::lock_guard l(resp_mu_);  // 使用锁保证线程安全，防止多个线程同时操作写缓冲区

  // 循环发送缓冲区中的数据项，直到队列为空
  while (!write_buf_.queue_.empty()) {
    std::string item = write_buf_.queue_.front();  // 获取队列中的第一个数据项
    item_len = item.size();                        // 获取数据项的长度

    // 循环将数据项分批次写入
    while (item_len - write_buf_.item_pos_ > 0) {
      // 将数据写入到文件描述符中
      nwritten = write(fd(), item.data() + write_buf_.item_pos_, item_len - write_buf_.item_pos_);
      if (nwritten <= 0) {
        break;  // 如果没有成功写入数据，退出循环
      }

      // 统计写入的字节数
      g_network_statistic->IncrReplOutputBytes(nwritten);
      write_buf_.item_pos_ += nwritten;  // 更新当前写入的位置

      // 如果数据项已经全部写完，移除队列中的数据项
      if (write_buf_.item_pos_ == item_len) {
        write_buf_.queue_.pop();
        write_buf_.item_pos_ = 0;
        item_len = 0;  // 重置数据项长度
      }
    }

    // 如果写入过程中发生错误，判断是否是 EAGAIN 错误
    if (nwritten == -1) {
      if (errno == EAGAIN) {
        return kWriteHalf;  // 如果是 EAGAIN 错误，表示缓冲区未准备好，返回部分写入
      } else {
        // 如果发生其他错误，返回写入错误状态
        return kWriteError;
      }
    }

    // 如果数据项还有剩余未写入，返回部分写入状态
    if (item_len - write_buf_.item_pos_ != 0) {
      return kWriteHalf;
    }
  }

  // 如果所有数据项都已写入完毕，返回所有数据已写入状态
  return kWriteAll;
}

// 设置回复状态
void PbConn::set_is_reply(const bool is_reply) {
  std::lock_guard l(is_reply_mu_);  // 使用锁保证线程安全

  // 根据 is_reply 的值更新回复状态
  if (is_reply) {
    is_reply_++;  // 增加回复计数
  } else {
    is_reply_--;  // 减少回复计数
  }

  // 防止 is_reply_ 计数器变为负数
  if (is_reply_ < 0) {
    is_reply_ = 0;
  }
}

// 获取当前是否正在进行回复
bool PbConn::is_reply() {
  std::lock_guard l(is_reply_mu_);  // 使用锁保证线程安全
  return is_reply_ > 0;             // 如果 is_reply_ 大于 0，表示正在回复
}

// 将响应数据写入写缓冲区
int PbConn::WriteResp(const std::string& resp) {
  std::string tag;
  BuildInternalTag(resp, &tag);  // 为响应构建内部标识标签
  std::lock_guard l(resp_mu_);   // 使用锁保证线程安全

  // 将标识标签和响应数据推入写缓冲区
  write_buf_.queue_.push(tag);
  write_buf_.queue_.push(resp);
  set_is_reply(true);  // 设置正在回复
  return 0;            // 返回成功
}

// 构建响应数据的内部标签，表示数据的大小
void PbConn::BuildInternalTag(const std::string& resp, std::string* tag) {
  uint32_t resp_size = resp.size();                            // 获取响应数据的大小
  resp_size = htonl(resp_size);                                // 将响应大小转换为网络字节序
  *tag = std::string(reinterpret_cast<char*>(&resp_size), 4);  // 创建 4 字节的标签
}

// 尝试调整接收缓冲区的大小
void PbConn::TryResizeBuffer() {
  struct timeval now;
  gettimeofday(&now, nullptr);                               // 获取当前时间
  time_t idletime = now.tv_sec - last_interaction().tv_sec;  // 计算空闲时间

  // 判断是否需要调整缓冲区大小
  if (rbuf_len_ > PB_IOBUF_LEN && ((rbuf_len_ / (cur_pos_ + 1)) > 2 || idletime > 2)) {
    // 计算新的缓冲区大小
    uint32_t new_size = ((cur_pos_ + PB_IOBUF_LEN) / PB_IOBUF_LEN) * PB_IOBUF_LEN;
    if (new_size < rbuf_len_) {  // 如果新的缓冲区大小小于当前大小，执行缩减操作
      rbuf_ = static_cast<char*>(realloc(rbuf_, new_size));  // 重新分配缓冲区
      rbuf_len_ = new_size;
      LOG(INFO) << "Thread_id " << pthread_self() << "Shrink rbuf to " << rbuf_len_ << ", cur_pos_: " << cur_pos_;
    }
  }
}

// 通知写事件发生
void PbConn::NotifyWrite() {
  // 创建通知事件，并将其注册到事件多路复用器
  net::NetItem ti(fd(), ip_port(), net::kNotiWrite);
  net_multiplexer()->Register(ti, true);
}

// 通知连接关闭事件
void PbConn::NotifyClose() {
  // 创建关闭通知事件，并将其注册到事件多路复用器
  net::NetItem ti(fd(), ip_port(), net::kNotiClose);
  net_multiplexer()->Register(ti, true);
}

}  // namespace net
