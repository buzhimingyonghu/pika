// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_NET_THREAD_H_
#define NET_INCLUDE_NET_THREAD_H_

#include <pthread.h>
#include <atomic>
#include <string>

#include "pstd/include/noncopyable.h"
#include "pstd/include/pstd_mutex.h"

namespace net {

class Thread : public pstd::noncopyable {  // Thread类继承自pstd::noncopyable，表示线程类不可拷贝
 public:
  Thread();           // 构造函数
  virtual ~Thread();  // 虚析构函数

  // 启动线程，返回状态码
  virtual int StartThread();

  // 停止线程，返回状态码
  virtual int StopThread();

  // 等待线程结束
  int JoinThread();

  // 获取是否应该停止线程的状态
  bool should_stop() { return should_stop_.load(); }

  // 设置线程应该停止
  void set_should_stop() { should_stop_.store(true); }

  // 获取线程是否正在运行
  bool is_running() { return running_.load(); }

  // 获取线程ID
  pthread_t thread_id() const { return thread_id_; }

  // 获取线程名称
  std::string thread_name() const { return thread_name_; }

  // 设置线程名称
  virtual void set_thread_name(const std::string& name) { thread_name_ = name; }

 protected:
  std::atomic_bool should_stop_;  // 线程是否应该停止的标志，原子操作
  // 设置线程是否正在运行
  void set_is_running(bool is_running) {
    std::lock_guard l(running_mu_);  // 加锁保护对running_的修改
    running_ = is_running;
  }

 private:
  // 线程启动时执行的静态函数，传入参数arg
  static void* RunThread(void* arg);

  // 线程主逻辑，必须由派生类实现
  virtual void* ThreadMain() = 0;

  pstd::Mutex running_mu_;            // 用于保护running_变量的互斥锁
  std::atomic_bool running_ = false;  // 线程是否正在运行，原子操作
  pthread_t thread_id_{};             // 线程ID
  std::string thread_name_;           // 线程名称
};

}  // namespace net
#endif  // NET_INCLUDE_NET_THREAD_H_
