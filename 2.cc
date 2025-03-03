#include <coroutine>
#include <iostream>
#include <cstdint>

// 生成器类模板
template<typename T>
struct Generator {
    // 协程的 promise 类型定义
    struct promise_type {
        T current_value;  // 当前生成的值

        // 获取生成器对象（将协程与生成器关联）
        Generator get_return_object() { 
            return Generator{ 
                std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }
        // 初始挂起点：协程启动后立即挂起
        std::suspend_always initial_suspend() { return {}; }
        
        // 最终挂起点：协程结束时挂起（保持状态供清理）
        std::suspend_always final_suspend() noexcept { return {}; }
        
        // 处理 co_yield 表达式
        std::suspend_always yield_value(T value) {
            current_value = value;
            return {};
        }
        
        // 处理 co_return（无返回值）
        void return_void() {}
        
        // 异常处理
        void unhandled_exception() { std::terminate(); }
    };

    // 协程句柄
    std::coroutine_handle<promise_type> coro_handle;

    // 构造函数
    explicit Generator(std::coroutine_handle<promise_type> h) 
        : coro_handle(h) {}
    
    // 析构函数
    ~Generator() { 
        if (coro_handle) coro_handle.destroy();
    }

    // 获取当前值
    T value() const {
        return coro_handle.promise().current_value;
    }
    // 恢复协程并检查是否完成
    bool next() {
        if (!coro_handle.done()) {
            coro_handle.resume();
            return !coro_handle.done();
        }
        return false;
    }

    // 支持范围 for 循环的迭代器
    struct iterator {
        Generator& gen;
        
        bool operator!=(const iterator&) const {
            return !gen.coro_handle.done();
        }
        
        void operator++() { gen.next(); }
        T operator*() const { return gen.value(); }
    };

    iterator begin() { return {*this}; }
    iterator end() { return {*this}; }
};

// 斐波那契数列生成器协程
Generator<uint64_t> fibonacci() {
    uint64_t a = 0, b = 1;
    while (true) {
        co_yield a;        // 产出当前值并挂起
        auto tmp = a;
        a = b;
        b += tmp;
    }
}

// 使用示例
int main() {
    // 创建生成器对象（此时协程尚未开始执行）
    auto fib_gen = fibonacci();

    // 使用范围 for 循环遍历前20个斐波那契数
    int count = 0;
    for (auto val : fib_gen) {
        std::cout << val << "\n";
        if (++count >= 20) break;
    }
}