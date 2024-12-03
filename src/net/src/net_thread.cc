// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/net_thread.h"
#include "net/include/net_define.h"
#include "net/src/net_thread_name.h"
#include "pstd/include/xdebug.h"

namespace net {

// Thread 构造函数，初始化 should_stop_ 标志为 false，表示线程默认不应停止。
Thread::Thread() : should_stop_(false) {}

// Thread 析构函数。此处没有特定的清理工作，因此使用默认的析构函数。
Thread::~Thread() = default;

// 线程运行函数，由 pthread_create 调用。
// 它会将传入的线程对象（this）作为参数，调用线程的 ThreadMain() 方法。
// 同时会为线程设置名称（如果设置了线程名称）。
void* Thread::RunThread(void* arg) {
  // 将 void* 类型的参数重新解释为 Thread 指针
  auto thread = reinterpret_cast<Thread*>(arg);

  // 如果线程名称不为空，设置线程的名称
  if (!(thread->thread_name().empty())) {
    SetThreadName(pthread_self(), thread->thread_name());
  }

  // 执行 ThreadMain 方法，线程的具体工作逻辑通常由该方法实现
  thread->ThreadMain();
  return nullptr;  // 线程执行完成后返回 nullptr
}

// 启动线程，如果线程已经在运行或者不需要停止，则直接返回 0。
// 否则，创建新的线程并调用 RunThread。
int Thread::StartThread() {
  // 如果线程已经在运行并且不需要停止，则不需要重新启动
  if (!should_stop() && is_running()) {
    return 0;  // 返回 0 表示线程已经在运行
  }

  // 获取锁以确保线程启动状态的安全更新
  std::lock_guard l(running_mu_);

  // 设置线程不应停止，表示准备启动线程
  should_stop_ = false;

  // 如果线程未在运行，创建并启动新线程
  if (!running_) {
    running_ = true;
    // 创建新线程，执行 RunThread 函数，并传入当前对象（this）
    return pthread_create(&thread_id_, nullptr, RunThread, this);
  }

  return 0;  // 返回 0 表示线程已经在运行
}

// 停止线程：如果线程已经停止或不需要停止，则返回 0。
// 否则，设置线程停止标志，并等待线程结束。
int Thread::StopThread() {
  // 如果线程已经停止并且不需要停止，则直接返回
  if (should_stop() && !is_running()) {
    return 0;
  }

  // 获取锁以确保线程停止状态的安全更新
  std::lock_guard l(running_mu_);

  // 设置线程应停止的标志
  should_stop_ = true;

  // 如果线程正在运行，设置线程状态为未运行，并等待线程结束
  if (running_) {
    running_ = false;
    // 等待线程结束并回收线程资源
    return pthread_join(thread_id_, nullptr);
  }

  return 0;  // 返回 0 表示线程已经停止
}

// 等待线程结束并回收线程资源。通常由线程的创建者调用，确保线程正确终止。
int Thread::JoinThread() {
  // 等待线程结束并回收资源
  return pthread_join(thread_id_, nullptr);
}

}  // namespace net
