// CR-004 §10: Dispatch queue / 背压单元测试
// 覆盖慢订阅者、队列满、突发消息、Broker callback 不阻塞
#include <gtest/gtest.h>
#include "mqtt_dispatch_queue.h"
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace tbox::mqtt;

// ---- 基本入队/出队 ----
TEST(MqttDispatchQueueTest, EnqueueAndDispatch) {
    std::atomic<int> count{0};
    std::vector<std::string> topics;
    std::mutex mtx;
    MqttDispatchQueue q(16, [&](const std::string& topic, const std::vector<uint8_t>&) {
        std::lock_guard<std::mutex> lk(mtx);
        topics.push_back(topic);
        count.fetch_add(1);
    });
    q.start();

    q.enqueue("topic/1", {0x01});
    q.enqueue("topic/2", {0x02});
    q.enqueue("topic/3", {0x03});

    // 等待 worker 处理
    for (int i = 0; i < 200 && count.load() < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    q.stop();

    EXPECT_EQ(count.load(), 3);
    ASSERT_EQ(topics.size(), 3u);
    EXPECT_EQ(topics[0], "topic/1");
    EXPECT_EQ(topics[1], "topic/2");
    EXPECT_EQ(topics[2], "topic/3");
}

// ---- 队列满丢弃 (CR-004 §6) ----
TEST(MqttDispatchQueueTest, QueueFullDrops) {
    std::atomic<int> pushed{0};
    std::mutex push_mtx;
    std::condition_variable push_cv;
    bool block = true;

    MqttDispatchQueue q(2, [&](const std::string&, const std::vector<uint8_t>&) {
        std::unique_lock<std::mutex> lk(push_mtx);
        pushed.fetch_add(1);
        push_cv.notify_one();
        push_cv.wait(lk, [&]{ return !block; });
    });
    q.start();

    // 入队 2 条（满）
    EXPECT_TRUE(q.enqueue("t1", {}));
    EXPECT_TRUE(q.enqueue("t2", {}));
    // 第 3 条：队列满，应丢弃
    EXPECT_FALSE(q.enqueue("t3", {}));

    EXPECT_EQ(q.dropped_count(), 1u);

    // 释放 worker
    {
        std::lock_guard<std::mutex> lk(push_mtx);
        block = false;
    }
    push_cv.notify_all();
    q.stop();

    EXPECT_GE(q.dropped_count(), 1u);
}

// ---- Broker callback 不阻塞 (CR-004 §4) ----
// 即使 push 回调慢，enqueue 应快速返回
TEST(MqttDispatchQueueTest, EnqueueDoesNotBlockOnSlowCallback) {
    MqttDispatchQueue q(64, [&](const std::string&, const std::vector<uint8_t>&) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    });
    q.start();

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 10; ++i) {
        q.enqueue("topic/" + std::to_string(i), {0x01});
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    // enqueue 10 次应在远小于 50ms（worker 每次处理 50ms）内完成
    EXPECT_LT(elapsed.count(), 50);
    q.stop();
}

// ---- stop flush 剩余项 ----
TEST(MqttDispatchQueueTest, StopFlushesRemaining) {
    std::atomic<int> count{0};
    MqttDispatchQueue q(32, [&](const std::string&, const std::vector<uint8_t>&) {
        count.fetch_add(1);
    });
    q.start();

    for (int i = 0; i < 5; ++i) {
        q.enqueue("t", {});
    }
    q.stop();

    // stop 后所有项应已处理
    EXPECT_EQ(count.load(), 5);
}

// ---- 突发消息不丢（容量内） ----
TEST(MqttDispatchQueueTest, BurstWithinCapacity) {
    std::atomic<int> count{0};
    MqttDispatchQueue q(128, [&](const std::string&, const std::vector<uint8_t>&) {
        count.fetch_add(1);
    });
    q.start();

    for (int i = 0; i < 100; ++i) {
        q.enqueue("t" + std::to_string(i), {});
    }
    for (int i = 0; i < 200 && count.load() < 100; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    q.stop();

    EXPECT_EQ(count.load(), 100);
    EXPECT_EQ(q.dropped_count(), 0u);
}
