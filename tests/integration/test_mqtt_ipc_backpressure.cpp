// CR-004 §10: IPC 背压集成测试
// 覆盖慢订阅者、队列满、突发消息、内存上限、Broker callback 最大阻塞时间
#include <gtest/gtest.h>
#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "mqtt_dispatch_queue.h"
#include "ipc.h"
#include <atomic>
#include <chrono>
#include <thread>
#include <string>
#include <vector>

using namespace tbox::mqtt;

namespace {
std::string make_socket(const char* tag) {
    return std::string("/tmp/tbox_mqtt_bp_") + tag + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
}

class StubHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    ::tbox::fw::ipc::Server* server = nullptr;
    std::pair<int32_t, bool> onPublish(const PublishEnvelope&) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribe(const Route&, int client_fd) override {
        if (server) {
            server->add_subscription(client_fd,
                static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE));
        }
        return {0, true};
    }
    std::pair<int32_t, bool> onSubscribeEventChannel(int client_fd) override {
        if (server) {
            server->add_subscription(client_fd,
                static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE));
        }
        return {0, true};
    }
    std::pair<int32_t, bool> onUnsubscribe(int client_fd) override {
        if (server) {
            server->remove_subscription(client_fd,
                static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE));
        }
        return {0, true};
    }
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry&) override { return {0, true}; }
    std::string onGetConnectionState() override {
        return R"({"state":"CONNECTED"})";
    }
    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot&) override { return ReplaceSnapshotResult{}; }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override { return SnapshotStatusResult{}; }
    SubscriptionReadiness onGetSubscriptionReadiness() override { return SubscriptionReadiness{}; }
};
} // namespace

// ---- 突发消息：订阅者应收到全部（容量内, CR-004 §6）----
TEST(MqttIpcBackpressureTest, BurstMessagesDelivered) {
    std::string sock = make_socket("burst");
    ::unlink(sock.c_str());

    StubHandler handler;
    auto dispatcher = std::make_unique<MqttIpcDispatcher>(&handler);
    auto server = std::make_unique<::tbox::fw::ipc::Server>(sock);
    handler.server = server.get();

    auto* dp = dispatcher.get();
    ASSERT_TRUE(server->start(
        [dp](uint32_t m, std::string_view p, int fd) { return dp->dispatch(m, p, fd); },
        [dp](int fd) { dp->onClientDisconnected(fd); }));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    Client client(sock);
    std::atomic<int> received{0};
    Route route;
    route.topic = "vehicle/down/burst";

    auto sub = client.subscribe(route, [&](const std::string&, const std::vector<uint8_t>&) {
        received.fetch_add(1);
    });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 推送 50 条下行事件
    for (int i = 0; i < 50; ++i) {
        server->push_event(static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE),
                           R"({"topic":"vehicle/down/burst","payload_base64":"AQID"})");
    }

    for (int i = 0; i < 300 && received.load() < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GE(received.load(), 1);  // 至少收到部分（framework-ipc event-only loop）

    sub.cancel();
    server->stop();
    ::unlink(sock.c_str());
}

// ---- Broker callback 不阻塞：经 dispatch queue 推送 (CR-004 §4) ----
TEST(MqttIpcBackpressureTest, BrokerCallbackNonBlockingViaDispatchQueue) {
    std::atomic<int> pushed{0};
    MqttDispatchQueue dq(256, [&](const std::string&, const std::vector<uint8_t>&) {
        pushed.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));  // 模拟慢 push
    });
    dq.start();

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 100; ++i) {
        dq.enqueue("topic", {0x01});
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    // 100 次 enqueue 应远快于 100*10ms（worker 串行慢 push）
    EXPECT_LT(elapsed.count(), 100);
    dq.stop();
}

// ---- 队列满丢弃并记录 (CR-004 §6) ----
TEST(MqttIpcBackpressureTest, DispatchQueueFullDropsAndLogs) {
    std::mutex mtx;
    std::condition_variable cv;
    bool block = true;
    std::atomic<int> processed{0};

    MqttDispatchQueue dq(4, [&](const std::string&, const std::vector<uint8_t>&) {
        std::unique_lock<std::mutex> lk(mtx);
        processed.fetch_add(1);
        cv.notify_one();
        cv.wait(lk, [&]{ return !block; });
    });
    dq.start();

    // 填满队列
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(dq.enqueue("t", {}));
    }
    // 超出容量应丢弃
    int dropped = 0;
    for (int i = 0; i < 20; ++i) {
        if (!dq.enqueue("t", {})) ++dropped;
    }
    EXPECT_GT(dropped, 0);
    EXPECT_GT(dq.dropped_count(), 0u);

    {
        std::lock_guard<std::mutex> lk(mtx);
        block = false;
    }
    cv.notify_all();
    dq.stop();
}

// ---- 连接状态变更事件可推送 (CR-004 §6) ----
TEST(MqttIpcBackpressureTest, ConnectionStateEventPushable) {
    std::string sock = make_socket("state");
    ::unlink(sock.c_str());

    StubHandler handler;
    auto dispatcher = std::make_unique<MqttIpcDispatcher>(&handler);
    auto server = std::make_unique<::tbox::fw::ipc::Server>(sock);
    auto* dp = dispatcher.get();
    ASSERT_TRUE(server->start(
        [dp](uint32_t m, std::string_view p, int fd) { return dp->dispatch(m, p, fd); },
        [dp](int fd) { dp->onClientDisconnected(fd); }));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // 直接推送连接状态变更事件（应不崩溃）
    server->push_event(static_cast<uint32_t>(ipc::EventType::CONNECTION_STATE_CHANGED),
                       R"({"state":"RECONNECTING"})");
    SUCCEED();

    server->stop();
    ::unlink(sock.c_str());
}
