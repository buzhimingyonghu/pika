// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/redis_parser.h"

#include <cassert> /* assert */

#include <glog/logging.h>

#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

namespace net {

static bool IsHexDigit(char ch) {
  return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
}

static int HexDigitToInt32(char ch) {
  if (ch <= '9' && ch >= '0') {
    return ch - '0';
  } else if (ch <= 'F' && ch >= 'A') {
    return ch - 'A';
  } else if (ch <= 'f' && ch >= 'a') {
    return ch - 'a';
  } else {
    return 0;
  }
}

static int split2args(const std::string& req_buf, RedisCmdArgsType& argv) {
  const char* p = req_buf.data();
  std::string arg;

  while (true) {
    // skip blanks
    while ((*p != 0) && (isspace(*p) != 0)) {
      p++;
    }
    if (*p != 0) {
      // get a token
      int inq = 0;   // set to 1 if we are in "quotes"
      int insq = 0;  // set to 1 if we are in 'single quotes'
      int done = 0;

      arg.clear();
      while (done == 0) {
        if (inq != 0) {
          if (*p == '\\' && *(p + 1) == 'x' && IsHexDigit(*(p + 2)) && IsHexDigit(*(p + 3))) {
            char byte = static_cast<char>(HexDigitToInt32(*(p + 2)) * 16 + HexDigitToInt32(*(p + 3)));
            arg.append(1, byte);
            p += 3;
          } else if (*p == '\\' && (*(p + 1) != 0)) {
            char c;

            p++;
            switch (*p) {
              case 'n':
                c = '\n';
                break;
              case 'r':
                c = '\r';
                break;
              case 't':
                c = '\t';
                break;
              case 'b':
                c = '\b';
                break;
              case 'a':
                c = '\a';
                break;
              default:
                c = *p;
                break;
            }
            arg.append(1, c);
          } else if (*p == '"') {
            /* closing quote must be followed by a space or
             * nothing at all. */
            if ((*(p + 1) != 0) && (isspace(*(p + 1)) == 0)) {
              argv.clear();
              return -1;
            }
            done = 1;
          } else if (*p == 0) {
            // unterminated quotes
            argv.clear();
            return -1;
          } else {
            arg.append(1, *p);
          }
        } else if (insq != 0) {
          if (*p == '\\' && *(p + 1) == '\'') {
            p++;
            arg.append(1, '\'');
          } else if (*p == '\'') {
            /* closing quote must be followed by a space or
             * nothing at all. */
            if ((*(p + 1) != 0) && (isspace(*(p + 1)) == 0)) {
              argv.clear();
              return -1;
            }
            done = 1;
          } else if (*p == 0) {
            // unterminated quotes
            argv.clear();
            return -1;
          } else {
            arg.append(1, *p);
          }
        } else {
          switch (*p) {
            case ' ':
            case '\n':
            case '\r':
            case '\t':
            case '\0':
              done = 1;
              break;
            case '"':
              inq = 1;
              break;
            case '\'':
              insq = 1;
              break;
            default:
              // current = sdscatlen(current,p,1);
              arg.append(1, *p);
              break;
          }
        }
        if (*p != 0) {
          p++;
        }
      }
      argv.push_back(arg);
    } else {
      return 0;
    }
  }
}

int RedisParser::FindNextSeparators() {
  if (cur_pos_ > length_ - 1) {
    return -1;
  }
  int pos = cur_pos_;
  while (pos <= length_ - 1) {
    if (input_buf_[pos] == '\n') {
      return pos;
    }
    pos++;
  }
  return -1;
}

int RedisParser::GetNextNum(int pos, long* value) {
  assert(pos > cur_pos_);
  //     cur_pos_       pos
  //      |    ----------|
  //      |    |
  //      *3\r\n
  // [cur_pos_ + 1, pos - cur_pos_ - 2]
  if (pstd::string2int(input_buf_ + cur_pos_ + 1, pos - cur_pos_ - 2, value) != 0) {
    return 0;  // Success
  }
  return -1;  // Failed
}

RedisParser::RedisParser() : redis_type_(0), bulk_len_(-1), redis_parser_type_(REDIS_PARSER_REQUEST) {}

void RedisParser::SetParserStatus(RedisParserStatus status, RedisParserError error) {
  if (status == kRedisParserHalf) {
    CacheHalfArgv();
  }
  status_code_ = status;
  error_code_ = error;
}

void RedisParser::CacheHalfArgv() {
  std::string tmp(input_buf_ + cur_pos_, length_ - cur_pos_);
  half_argv_ = tmp;
  cur_pos_ = length_;
}

RedisParserStatus RedisParser::RedisParserInit(RedisParserType type, const RedisParserSettings& settings) {
  if (status_code_ != kRedisParserNone) {
    SetParserStatus(kRedisParserError, kRedisParserInitError);
    return status_code_;
  }
  if (type != REDIS_PARSER_REQUEST && type != REDIS_PARSER_RESPONSE) {
    SetParserStatus(kRedisParserError, kRedisParserInitError);
    return status_code_;
  }
  redis_parser_type_ = type;
  parser_settings_ = settings;
  SetParserStatus(kRedisParserInitDone);
  return status_code_;
}

// 解析 Redis 的 "inline 命令" 格式（即不使用多行协议的简单命令）
RedisParserStatus RedisParser::ProcessInlineBuffer() {
  int pos;  // 用于存储找到的分隔符位置
  int ret;  // 用于存储分割结果状态

  // 查找下一个命令的分隔符（CRLF，即 \r\n）
  pos = FindNextSeparators();
  if (pos == -1) {  // 如果未找到分隔符
    // 检查输入缓冲区的长度是否超过了 Redis 的最大限制（默认是 64KB）
    if (length_ > REDIS_INLINE_MAXLEN) {
      // 超过最大长度，设置解析器状态为 "错误 - 输入超长"
      SetParserStatus(kRedisParserError, kRedisParserFullError);
      return status_code_;  // 返回错误状态
    } else {
      // 输入尚未完整，设置解析器状态为 "部分解析完成"
      SetParserStatus(kRedisParserHalf);
      return status_code_;  // 返回半完成状态
    }
  }

  // 如果找到分隔符，提取当前命令的内容
  // `cur_pos_` 是当前解析的位置，`pos + 1` 是分隔符的结束位置
  std::string req_buf(input_buf_ + cur_pos_, pos + 1 - cur_pos_);

  // 清空上一次解析的参数数组
  argv_.clear();

  // 将命令字符串分割为命令及其参数，并存入 `argv_`
  ret = split2args(req_buf, argv_);

  // 更新解析位置，跳过当前命令所占用的字节
  cur_pos_ = pos + 1;

  // 检查分割结果是否失败
  if (ret == -1) {
    // 分割失败，说明协议格式错误，设置解析器状态为 "错误 - 协议错误"
    SetParserStatus(kRedisParserError, kRedisParserProtoError);
    return status_code_;  // 返回错误状态
  }

  // 分割成功，设置解析器状态为 "解析完成"
  SetParserStatus(kRedisParserDone);
  return status_code_;  // 返回完成状态
}

RedisParserStatus RedisParser::ProcessMultibulkBuffer() {
  int pos = 0;
  if (multibulk_len_ == 0) {
    /* The client should have been reset */
    pos = FindNextSeparators();
    if (pos != -1) {
      if (GetNextNum(pos, &multibulk_len_) != 0) {
        // Protocol error: invalid multibulk length
        SetParserStatus(kRedisParserError, kRedisParserProtoError);
        return status_code_;
      }
      cur_pos_ = pos + 1;
      argv_.clear();
      if (cur_pos_ > length_ - 1) {
        SetParserStatus(kRedisParserHalf);
        return status_code_;
      }
    } else {
      SetParserStatus(kRedisParserHalf);
      return status_code_;  // HALF
    }
  }
  while (multibulk_len_ != 0) {
    if (bulk_len_ == -1) {
      pos = FindNextSeparators();
      if (pos != -1) {
        if (input_buf_[cur_pos_] != '$') {
          SetParserStatus(kRedisParserError, kRedisParserProtoError);
          return status_code_;  // PARSE_ERROR
        }

        if (GetNextNum(pos, &bulk_len_) != 0) {
          // Protocol error: invalid bulk length
          SetParserStatus(kRedisParserError, kRedisParserProtoError);
          return status_code_;
        }
        cur_pos_ = pos + 1;
      }
      if (pos == -1 || cur_pos_ > length_ - 1) {
        SetParserStatus(kRedisParserHalf);
        return status_code_;
      }
    }
    if ((length_ - 1) - cur_pos_ + 1 < bulk_len_ + 2) {
      // Data not enough
      break;
    } else {
      argv_.emplace_back(input_buf_ + cur_pos_, bulk_len_);
      cur_pos_ = static_cast<int32_t>(cur_pos_ + bulk_len_ + 2);
      bulk_len_ = -1;
      multibulk_len_--;
    }
  }

  if (multibulk_len_ == 0) {
    SetParserStatus(kRedisParserDone);
    return status_code_;  // OK
  } else {
    SetParserStatus(kRedisParserHalf);
    return status_code_;  // HALF
  }
}

void RedisParser::PrintCurrentStatus() {
  LOG(INFO) << "status_code " << status_code_ << " error_code " << error_code_;
  LOG(INFO) << "multibulk_len_ " << multibulk_len_ << "bulk_len " << bulk_len_ << " redis_type " << redis_type_
            << " redis_parser_type " << redis_parser_type_;
  // for (auto& i : argv_) {
  //   UNUSED(i);
  //   log_info("parsed arguments: %s", i.c_str());
  // }
  LOG(INFO) << "cur_pos : " << cur_pos_;
  LOG(INFO) << "input_buf_ is clean ? " << (input_buf_ == nullptr);
  if (input_buf_) {
    LOG(INFO) << " input_buf " << input_buf_;
  }
  LOG(INFO) << "half_argv_ : " << half_argv_;
  LOG(INFO) << "input_buf len " << length_;
}

// 处理输入缓冲区，解析 Redis 请求或响应消息
RedisParserStatus RedisParser::ProcessInputBuffer(const char* input_buf, int length, int* parsed_len) {
  // 检查解析器的状态，确保它处于可继续处理的状态
  if (status_code_ == kRedisParserInitDone || status_code_ == kRedisParserHalf || status_code_ == kRedisParserDone) {
    // 创建一个临时字符串 `tmp_str`，避免直接操作 `input_buf`（可以优化以避免拷贝）
    // 将当前输入缓冲区的数据拼接到之前未完成的部分（`half_argv_`）后
    std::string tmp_str(input_buf, length);
    input_str_ = half_argv_ + tmp_str;

    // 更新 `input_buf_` 和 `length_`，以便后续解析使用
    input_buf_ = input_str_.c_str();
    length_ = static_cast<int32_t>(length + half_argv_.size());

    // 根据解析器的类型（请求或响应）处理相应的缓冲区
    if (redis_parser_type_ == REDIS_PARSER_REQUEST) {
      ProcessRequestBuffer();  // 解析 Redis 请求
    } else if (redis_parser_type_ == REDIS_PARSER_RESPONSE) {
      ProcessResponseBuffer();  // 解析 Redis 响应
    } else {
      // 如果解析器类型未知，设置错误状态并返回
      SetParserStatus(kRedisParserError, kRedisParserInitError);
      return status_code_;
    }

    // 更新解析的长度（`cur_pos_` 表示当前已解析的字节数）
    *parsed_len = cur_pos_;

    // 重置解析器状态，为下一次解析做好准备
    ResetRedisParser();

    // 打印当前解析状态（目前被注释掉了，可以用于调试）
    // PrintCurrentStatus();

    // 返回当前解析器的状态（成功或部分成功）
    return status_code_;
  }

  // 如果解析器状态异常（不是初始化完成、半完成或已完成），设置错误状态并返回
  SetParserStatus(kRedisParserError, kRedisParserInitError);
  return status_code_;
}

// TODO(): AZ
RedisParserStatus RedisParser::ProcessResponseBuffer() {
  SetParserStatus(kRedisParserDone);
  return status_code_;
}
RedisParserStatus RedisParser::ProcessRequestBuffer() {
  RedisParserStatus ret;

  // 循环解析输入缓冲区的数据
  while (cur_pos_ <= length_ - 1) {
    // 如果尚未确定请求类型，根据第一个字符判断
    if (redis_type_ == 0) {
      if (input_buf_[cur_pos_] == '*') {
        redis_type_ = REDIS_REQ_MULTIBULK;  // 多条命令请求
      } else {
        redis_type_ = REDIS_REQ_INLINE;  // 单行命令请求
      }
    }

    // 处理单行命令请求
    if (redis_type_ == REDIS_REQ_INLINE) {
      ret = ProcessInlineBuffer();    // 调用单行命令处理逻辑
      if (ret != kRedisParserDone) {  // 如果未完成，返回当前状态
        return ret;
      }
    }
    // 处理多条命令请求
    else if (redis_type_ == REDIS_REQ_MULTIBULK) {
      ret = ProcessMultibulkBuffer();  // 调用多条命令处理逻辑
      if (ret != kRedisParserDone) {   // FULL_ERROR || HALF || PARSE_ERROR
        return ret;                    // 如果未完成，返回当前状态
      }
    }
    // 如果是未知的请求类型，返回错误状态
    else {
      return kRedisParserError;
    }

    // 如果解析出了一个完整的命令，将其加入到命令集合
    if (!argv_.empty()) {
      argvs_.push_back(argv_);

      // 如果设置了回调函数 `DealMessage`，则调用处理
      if (parser_settings_.DealMessage) {
        if (parser_settings_.DealMessage(this, argv_) != 0) {
          SetParserStatus(kRedisParserError, kRedisParserDealError);  // 处理失败
          return status_code_;
        }
      }
    }

    argv_.clear();  // 清空当前命令参数列表

    // 重置命令解析状态，准备解析下一个命令
    ResetCommandStatus();
  }

  // 如果设置了回调函数 `Complete`，则调用处理已完成的命令集合
  if (parser_settings_.Complete) {
    if (parser_settings_.Complete(this, argvs_) != 0) {
      SetParserStatus(kRedisParserError, kRedisParserCompleteError);  // 处理失败
      return status_code_;
    }
  }

  argvs_.clear();  // 清空已完成命令集合

  // 设置解析状态为完成
  SetParserStatus(kRedisParserDone);

  return status_code_;  // 返回最终状态
}

void RedisParser::ResetCommandStatus() {
  redis_type_ = 0;
  multibulk_len_ = 0;
  bulk_len_ = -1;
  half_argv_.clear();
}

void RedisParser::ResetRedisParser() {
  cur_pos_ = 0;
  input_buf_ = nullptr;
  input_str_.clear();
  length_ = 0;
}

}  // namespace net
