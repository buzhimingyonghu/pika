// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include <utility>

#include "pstd/include/pstd_coding.h"
#include "pstd/include/pstd_string.h"

#include "binlog_receiver_thread.h"
#include "binlog_transverter.h"
#include "const.h"
#include "master_conn.h"
#include "pika_port.h"

extern PikaPort* g_pika_port;

MasterConn::MasterConn(int fd, std::string ip_port, void* worker_specific_data)
    : NetConn(fd, std::move(ip_port), nullptr),
      rbuf_(nullptr),
      rbuf_len_(0),
      rbuf_size_(REDIS_IOBUF_LEN),
      rbuf_cur_pos_(0),
      is_authed_(false) {
  binlog_receiver_ = reinterpret_cast<BinlogReceiverThread*>(worker_specific_data);
  rbuf_ = static_cast<char*>(realloc(rbuf_, REDIS_IOBUF_LEN));
}

MasterConn::~MasterConn() { free(rbuf_); }

net::ReadStatus MasterConn::ReadRaw(uint32_t count) {
  if (rbuf_cur_pos_ + count > rbuf_size_) {
    return net::kFullError;
  }
  int32_t nread = read(fd(), rbuf_ + rbuf_len_, count - (rbuf_len_ - rbuf_cur_pos_));
  if (nread == -1) {
    if (errno == EAGAIN) {
      return net::kReadHalf;
    } else {
      return net::kReadError;
    }
  } else if (nread == 0) {
    return net::kReadClose;
  }

  rbuf_len_ += nread;
  if (rbuf_len_ - rbuf_cur_pos_ != count) {
    return net::kReadHalf;
  }
  return net::kReadAll;
}

net::ReadStatus MasterConn::ReadHeader() {
  if (rbuf_len_ >= HEADER_LEN) {
    return net::kReadAll;
  }

  net::ReadStatus status = ReadRaw(HEADER_LEN);
  if (status != net::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += HEADER_LEN;
  return net::kReadAll;
}

net::ReadStatus MasterConn::ReadBody(uint32_t body_length) {
  if (rbuf_len_ == HEADER_LEN + body_length) {
    return net::kReadAll;
  } else if (rbuf_len_ > HEADER_LEN + body_length) {
    LOG(INFO) << "rbuf_len_ larger than sum of header length (6 Byte)"
              << " and body_length, rbuf_len_: " << rbuf_len_ << ", body_length: " << body_length;
  }

  net::ReadStatus status = ReadRaw(body_length);
  if (status != net::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += body_length;
  return net::kReadAll;
}

int32_t MasterConn::FindNextSeparators(const std::string& content, int32_t pos) {
  int32_t length = content.size();
  if (pos >= length) {
    return -1;
  }
  while (pos < length) {
    if (content[pos] == '\n') {
      return pos;
    }
    pos++;
  }
  return -1;
}

int32_t MasterConn::GetNextNum(const std::string& content, int32_t left_pos, int32_t right_pos, long* value) {
  //  left_pos        right_pos
  //      |------   -------|
  //            |   |
  //            *3\r\n
  //            012 3
  // num range [left_pos + 1, right_pos - 2]
  assert(left_pos < right_pos);
  if (pstd::string2int(content.data() + left_pos + 1, right_pos - left_pos - 2, value) != 0) {
    return 0;
  }
  return -1;
}

// RedisRESPArray : *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
net::ReadStatus MasterConn::ParseRedisRESPArray(const std::string& content, net::RedisCmdArgsType* argv) {
  int32_t pos = 0;
  int32_t next_parse_pos = 0;
  int32_t content_len = content.size();
  long multibulk_len = 0;
  long bulk_len = 0;
  if (content.empty() || content[0] != '*') {
    LOG(INFO) << "Content empty() or the first character of the redis protocol string not equal '*'";
    return net::kParseError;
  }
  pos = FindNextSeparators(content, next_parse_pos);
  if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &multibulk_len) != -1) {
    next_parse_pos = pos + 1;
  } else {
    LOG(INFO) << "Find next separators error or get next num error";
    return net::kParseError;
  }

  // next_parst_pos          pos
  //        |-------   -------|
  //               |   |
  //               $3\r\nset\r\n
  //               012 3 4567 8

  argv->clear();
  while (multibulk_len != 0) {
    if (content[next_parse_pos] != '$') {
      LOG(INFO) << "The first charactor of the RESP type element not equal '$'";
      return net::kParseError;
    }

    bulk_len = -1;
    pos = FindNextSeparators(content, next_parse_pos);
    if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &bulk_len) != -1) {
      if (pos + 1 + bulk_len + 2 > content_len) {
        return net::kParseError;
      } else {
        next_parse_pos = pos + 1;
        argv->emplace_back(content.data() + next_parse_pos, bulk_len);
        next_parse_pos = next_parse_pos + bulk_len + 2;
        multibulk_len--;
      }
    } else {
      LOG(INFO) << "Find next separators error or get next num error";
      return net::kParseError;
    }
  }
  if (content_len != next_parse_pos) {
    LOG(INFO) << "Incomplete parse";
    return net::kParseError;
  } else {
    return net::kOk;
  }
}

void MasterConn::ResetStatus() {
  rbuf_len_ = 0;
  rbuf_cur_pos_ = 0;
}

// GetRequest: 读取请求并进行处理。
// 功能：该方法从网络连接中读取请求数据，并根据请求类型进行相应的处理。
// 作用：读取请求头，解析请求体，处理身份认证请求或 binlog 数据请求，
//       如果请求类型不支持或解析失败，则返回错误状态。

net::ReadStatus MasterConn::GetRequest() {
  // 读取请求头
  net::ReadStatus status;
  // 如果读取头部失败，则返回相应状态
  if ((status = ReadHeader()) != net::kReadAll) {
    return status;  // 读取头部失败，返回错误状态
  }

  // 解析头部，获取 body 长度和类型
  uint16_t type = 0;                        // 请求类型
  uint32_t body_length = 0;                 // 请求体的长度
  std::string header(rbuf_, HEADER_LEN);    // 从缓冲区中获取头部数据
  pstd::GetFixed16(&header, &type);         // 获取固定长度的 16 位类型
  pstd::GetFixed32(&header, &body_length);  // 获取固定长度的 32 位 body 长度

  // 如果请求类型不是 kTypePortAuth 或 kTypePortBinlog，返回解析错误
  if (type != kTypePortAuth && type != kTypePortBinlog) {
    LOG(INFO) << "Unrecognizable Type: " << type << " maybe identify binlog type error";
    return net::kParseError;  // 返回解析错误状态
  }

  // 根据头部和 body 长度重新分配缓冲区（如果需要）
  uint32_t needed_size = HEADER_LEN + body_length;  // 计算总需要的缓冲区大小
  // 如果当前缓冲区大小不足，进行扩容
  if (rbuf_size_ < needed_size) {
    // 如果需要的大小超过最大限制，则返回缓冲区溢出错误
    if (needed_size > REDIS_MAX_MESSAGE) {
      return net::kFullError;  // 返回缓冲区溢出错误
    } else {
      // 否则，重新分配缓冲区
      rbuf_ = static_cast<char*>(realloc(rbuf_, needed_size));
      rbuf_size_ = needed_size;  // 更新缓冲区大小
    }
  }

  // 读取请求体
  if ((status = ReadBody(body_length)) != net::kReadAll) {
    return status;  // 读取请求体失败，返回相应状态
  }

  net::RedisCmdArgsType argv;                         // 存储解析后的 Redis 命令参数
  std::string body(rbuf_ + HEADER_LEN, body_length);  // 获取请求体内容
  // 如果是身份认证请求
  if (type == kTypePortAuth) {
    // 解析 Redis RESP 格式的数组
    if ((status = ParseRedisRESPArray(body, &argv)) != net::kOk) {
      LOG(INFO) << "Type auth ParseRedisRESPArray error";
      return status;  // 解析失败，返回错误状态
    }
    // 处理身份认证请求
    if (!ProcessAuth(argv)) {
      return net::kDealError;  // 处理身份认证失败，返回错误状态
    }
  }
  // 如果是 binlog 请求
  else if (type == kTypePortBinlog) {
    PortBinlogItem item;
    // 解码 binlog 数据
    if (!PortBinlogTransverter::PortBinlogDecode(PortTypeFirst, body, &item)) {
      LOG(INFO) << "Binlog decode error: " << item.ToString();
      return net::kParseError;  // binlog 解码失败，返回解析错误
    }
    // 解析 binlog 请求体中的 Redis RESP 数组
    if ((status = ParseRedisRESPArray(item.content(), &argv)) != net::kOk) {
      LOG(INFO) << "Type Binlog ParseRedisRESPArray error: " << item.ToString();
      return status;  // 解析失败，返回错误状态
    }
    // 处理 binlog 数据
    if (!ProcessBinlogData(argv, item)) {
      return net::kDealError;  // 处理 binlog 数据失败，返回错误状态
    }
  } else {
    LOG(INFO) << "Unrecognizable Type";  // 如果请求类型无法识别，返回解析错误
    return net::kParseError;
  }

  // 重置连接状态
  ResetStatus();
  return net::kReadAll;  // 返回读取完成状态
}

net::WriteStatus MasterConn::SendReply() { return net::kWriteAll; }

void MasterConn::TryResizeBuffer() {}

bool MasterConn::ProcessAuth(const net::RedisCmdArgsType& argv) {
  if (argv.empty() || argv.size() != 2) {
    return false;
  }

  if (argv[0] == "auth") {
    if (argv[1] == std::to_string(g_pika_port->sid())) {
      is_authed_ = true;
      LOG(INFO) << "BinlogReceiverThread AccessHandle succeeded, My server id: " << g_pika_port->sid()
                << ", Master auth server id: " << argv[1];
      return true;
    }
  }

  LOG(INFO) << "BinlogReceiverThread AccessHandle failed, My server id: " << g_pika_port->sid()
            << ", Master auth server id: " << argv[1];

  return false;
}
// 处理从主服务器接收到的 binlog 数据
bool MasterConn::ProcessBinlogData(const net::RedisCmdArgsType& argv, const PortBinlogItem& binlog_item) {
  // 如果尚未通过身份验证，返回错误
  if (!is_authed_) {
    LOG(INFO) << "Need Auth First";  // 记录日志：需要先进行身份验证
    return false;                    // 返回身份验证失败
  } else if (argv.empty()) {         // 如果参数为空，返回失败
    return false;
  }

  // 获取 Redis 命令的 key
  std::string key(" ");
  if (argv.size() > 1) {
    key = argv[1];  // 如果有第二个参数，将其作为 key
  }

  std::string command;
  if (argv[0] == "pksetexat") {  // 如果是 pksetexat 命令
    std::string temp("");        // 临时字符串，用于存储超时时间
    std::string time_out("");    // 存储计算后的超时时间
    std::string time_cmd("");    // 存储生成的时间命令
    int start;                   // 用于计算命令字符串的开始位置
    int old_time_size;           // 存储原始时间字符串的长度
    int new_time_size;           // 存储新时间字符串的长度
    int diff;                    // 计算时间差异，决定替换的字符数

    temp = argv[2];                      // 获取超时时间
    unsigned long int sec = time(NULL);  // 获取当前时间的秒数
    unsigned long int tot;
    tot = std::stol(temp) - sec;     // 计算超时差
    time_out = std::to_string(tot);  // 将超时差转换为字符串

    command = binlog_item.content();              // 获取 binlog 内容
    command.erase(0, 4);                          // 移除 binlog 内容前四个字符（假设为命令标识）
    command.replace(0, 13, "*4\r\n$5\r\nsetex");  // 修改命令的前部分，构造新的命令

    // 计算新的时间字符串并替换命令中的超时部分
    start = 13 + 3 + std::to_string(key.size()).size() + 2 + key.size() + 3;
    old_time_size = std::to_string(temp.size()).size() + 2 + temp.size();          // 计算原时间字符串的长度
    new_time_size = std::to_string(time_out.size()).size() + 2 + time_out.size();  // 计算新时间字符串的长度
    diff = old_time_size - new_time_size;                                          // 计算时间差

    // 移除旧时间部分，并插入新的时间字符串
    command.erase(start, diff);
    time_cmd = std::to_string(time_out.size()) + "\r\n" + time_out;  // 构造新的时间字符串格式
    command.replace(start, new_time_size, time_cmd);                 // 替换命令中的时间部分
  } else {  // 如果不是 pksetexat 命令，直接使用 binlog 内容
    command = binlog_item.content();
  }

  // 通过 pika_port 发送构造的 Redis 命令
  int ret = g_pika_port->SendRedisCommand(command, key);
  if (ret != 0) {  // 如果发送失败，记录警告日志
    LOG(WARNING) << "send redis command:" << command << ", ret:" << ret;
  }

  return true;  // 成功处理 binlog 数据
}
