// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_BG_THREAD_H_
#define NET_INCLUDE_BG_THREAD_H_

#include <atomic>
#include <functional>
#include <queue>
#include "net/include/net_thread.h"

#include "pstd/include/pstd_mutex.h"

namespace net {

// TimerItem 用于保存定时任务的结构体
struct TimerItem {
  uint64_t exec_time;       // 执行时间，单位：毫秒
  void (*function)(void*);  // 指向执行任务的函数
  void* arg;                // 传递给函数的参数

  // 构造函数，用于初始化 TimerItem
  TimerItem(uint64_t _exec_time, void (*_function)(void*), void* _arg)
      : exec_time(_exec_time), function(_function), arg(_arg) {}

  // 重载小于运算符，用于在优先队列中排序。定时任务按执行时间从小到大排列
  bool operator<(const TimerItem& item) const { return exec_time > item.exec_time; }
};

// BGThread 类用于管理后台任务，它继承自 Thread 类
class BGThread final : public Thread {
 public:
  // 构造函数，默认最大队列大小为 100000
  explicit BGThread(int full = 100000) : full_(full) {}

  // 析构函数，调用 StopThread 停止线程
  ~BGThread() override {
    // 调用虚函数停止线程，BGThread 必须是 final，避免派生类重载虚析构函数
    StopThread();
  }

  // 停止线程：设置停止标志，通知等待的线程，并调用基类的 StopThread
  int StopThread() override {
    should_stop_ = true;          // 设置停止标志
    rsignal_.notify_one();        // 通知一个等待线程
    wsignal_.notify_one();        // 通知一个等待线程
    return Thread::StopThread();  // 调用基类的 StopThread
  }

  // 调度一个任务，传入任务函数和参数
  void Schedule(void (*function)(void*), void* arg);

  // 调度一个任务，传入任务函数、参数和可选的回调函数
  void Schedule(void (*function)(void*), void* arg, std::function<void()>& call_back);

  /*
   * DelaySchedule 函数用于延时调度一个任务，timeout 表示延迟的时间，单位是毫秒
   */
  void DelaySchedule(uint64_t timeout, void (*function)(void*), void* arg);

  // 获取队列的大小（包括优先级队列和普通队列）
  void QueueSize(int* pri_size, int* qu_size);

  // 清空队列中的所有任务
  void QueueClear();

  // 忽略已经准备好的任务（即跳过队列中的任务）
  void SwallowReadyTasks();

 private:
  // BGItem 用于保存后台任务的结构体
  class BGItem {
   public:
    void (*function)(void*);  // 指向任务函数的指针
    void* arg;                // 传递给任务函数的参数

    // dtor_call_back 是一个可选的回调函数，任务完成后会执行
    std::function<void()> dtor_call_back;

    // 构造函数：传入任务函数和参数
    BGItem(void (*_function)(void*), void* _arg) : function(_function), arg(_arg) {}

    // 构造函数：同时传入任务函数、参数和析构时执行的回调函数
    BGItem(void (*_function)(void*), void* _arg, std::function<void()>& _dtor_call_back)
        : function(_function), arg(_arg), dtor_call_back(_dtor_call_back) {}

    // 析构函数：在任务完成后执行回调函数（如果有提供）
    ~BGItem() {
      if (dtor_call_back) {
        dtor_call_back();  // 调用析构回调
      }
    }
  };

  // 用于存储任务的队列
  std::queue<std::unique_ptr<BGItem>> queue_;
  // 用于存储定时任务的优先队列，定时任务会按执行时间排序
  std::priority_queue<TimerItem> timer_queue_;

  size_t full_;            // 最大队列大小
  pstd::Mutex mu_;         // 互斥锁，用于保护对队列的访问
  pstd::CondVar rsignal_;  // 读信号量，通知等待线程
  pstd::CondVar wsignal_;  // 写信号量，通知等待线程

  // 线程主函数，后台任务的具体处理逻辑
  void* ThreadMain() override;
};

}  // namespace net
#endif  // NET_INCLUDE_BG_THREAD_H_
