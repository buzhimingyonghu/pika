// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/bg_thread.h"
#include <cstdlib>
#include <mutex>

namespace net {

void BGThread::Schedule(void (*function)(void*), void* arg) {
  std::unique_lock lock(mu_);

  wsignal_.wait(lock, [this]() { return queue_.size() < full_ || should_stop(); });

  if (!should_stop()) {
    queue_.emplace(std::make_unique<BGItem>(function, arg));
    rsignal_.notify_one();
  }
}

void BGThread::Schedule(void (*function)(void*), void* arg, std::function<void()>& call_back) {
  std::unique_lock lock(mu_);

  wsignal_.wait(lock, [this]() { return queue_.size() < full_ || should_stop(); });

  if (!should_stop()) {
    queue_.emplace(std::make_unique<BGItem>(function, arg, call_back));
    rsignal_.notify_one();
  }
};

void BGThread::QueueSize(int* pri_size, int* qu_size) {
  std::lock_guard lock(mu_);
  *pri_size = static_cast<int32_t>(timer_queue_.size());
  *qu_size = static_cast<int32_t>(queue_.size());
}

void BGThread::QueueClear() {
  std::lock_guard lock(mu_);
  std::queue<std::unique_ptr<BGItem>>().swap(queue_);
  std::priority_queue<TimerItem>().swap(timer_queue_);
  wsignal_.notify_one();
}

void BGThread::SwallowReadyTasks() {
  // it's safe to swallow all the remain tasks in ready and timer queue,
  // while the schedule function would stop to add any tasks.
  mu_.lock();
  while (!queue_.empty()) {
    std::unique_ptr<BGItem> task_item = std::move(queue_.front());
    queue_.pop();
    mu_.unlock();
    task_item->function(task_item->arg);
    mu_.lock();
  }
  mu_.unlock();

  auto now = std::chrono::system_clock::now();
  uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
  mu_.lock();

  while (!timer_queue_.empty()) {
    auto [exec_time, function, arg] = timer_queue_.top();
    if (unow < exec_time) {
      break;
    }
    timer_queue_.pop();
    // Don't lock while doing task
    mu_.unlock();
    (*function)(arg);
    mu_.lock();
  }
  mu_.unlock();
}

// ThreadMain: 线程主循环方法。
// 功能：该方法是后台线程的入口，负责处理队列中的任务和定时任务。
// 作用：该线程会不断循环，直到线程停止标志为真，
//      它会处理任务队列中的任务并执行定时任务，直到所有任务处理完毕。
//      同时，它会在没有任务时等待，避免占用过多 CPU 资源。
//      在线程停止时，会清理所有剩余的任务。

void* BGThread::ThreadMain() {
  while (!should_stop()) {       // 线程循环，直到线程停止标志为真
    std::unique_lock lock(mu_);  // 获取互斥锁，保护共享资源

    // 等待条件变量，直到队列非空、定时器队列非空，或线程需要停止
    rsignal_.wait(lock, [this]() { return !queue_.empty() || !timer_queue_.empty() || should_stop(); });

    // 如果线程需要停止，跳出循环
    if (should_stop()) {
      break;
    }

    // 如果定时器队列非空，处理定时任务
    if (!timer_queue_.empty()) {
      auto now = std::chrono::system_clock::now();  // 获取当前时间
      uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch())
                          .count();                          // 获取当前时间的微秒数
      auto [exec_time, function, arg] = timer_queue_.top();  // 获取定时器队列的最小元素，即下一次执行的任务
      if (unow >= exec_time) {  // 如果当前时间大于或等于定时任务的执行时间
        timer_queue_.pop();     // 移除队列中的任务
        lock.unlock();          // 解锁，允许其他线程访问共享资源
        (*function)(arg);       // 执行任务
        continue;               // 继续下一次循环，检查下一个任务
      } else if (queue_.empty() && !should_stop()) {
        // 如果任务队列为空，且线程不需要停止，等待直到定时任务的执行时间
        rsignal_.wait_for(lock, std::chrono::microseconds(exec_time - unow));

        lock.unlock();  // 解锁
        continue;       // 继续下一次循环
      }
    }

    // 如果任务队列非空，处理任务
    if (!queue_.empty()) {
      std::unique_ptr<BGItem> task_item = std::move(queue_.front());  // 获取任务队列中的第一个任务
      queue_.pop();                                                   // 移除队列中的任务
      wsignal_.notify_one();                                          // 通知其他线程有任务执行
      lock.unlock();                                                  // 解锁
      task_item->function(task_item->arg);                            // 执行任务
    }
  }

  // 在线程停止后，处理剩余的准备好的任务和定时任务
  SwallowReadyTasks();
  return nullptr;  // 线程结束，返回空指针
}

/*
 * DelaySchedule: 延迟执行任务的方法。
 * 功能：该方法将一个延迟任务添加到定时器队列中，并在指定的延迟时间后执行。
 * 作用：通过计算延迟时间，将任务添加到定时队列，并触发线程进行调度，确保任务在延迟时间到期后执行。
 * 参数：
 *   - timeout: 延迟时间，单位为毫秒。
 *   - function: 要执行的任务函数。
 *   - arg: 传递给任务函数的参数。
 */

void BGThread::DelaySchedule(uint64_t timeout, void (*function)(void*), void* arg) {
  auto now = std::chrono::system_clock::now();  // 获取当前时间
  uint64_t unow =
      std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();  // 获取当前时间的微秒数
  uint64_t exec_time = unow + timeout * 1000;  // 计算定时任务的执行时间（当前时间 + 延迟时间）

  std::lock_guard lock(mu_);                         // 获取互斥锁，保护共享资源
  if (!should_stop()) {                              // 如果线程不需要停止
    timer_queue_.emplace(exec_time, function, arg);  // 将定时任务加入定时器队列
    rsignal_.notify_one();                           // 通知线程，可能有新任务需要处理
  }
}

}  // namespace net
