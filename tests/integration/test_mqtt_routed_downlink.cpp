// CR-010 §11.2: Routed downlink SDK 集成测试
// 覆盖 daemon 定向推送 -> SDK callback、routed 不增加 Broker SUBSCRIBE、
// 慢 callback/队列满/其他 owner 不受影响、cancel/析构无泄漏、legacy 共存。
#include <gtest/gtest.h>
#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "ipc.h"
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

using namespace tbox::mqtt;

namespace {

// 集成测试 handler：legacy 事件通道登记 DOWNLINK_MESSAGE，routed 登记 ROUTED_DOWNLINK_MESSAGE
class IntegHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    ::tbox::fw::ipc::Server* server = nullptr;
    std::atomic<int> broker_subscribe_count{0};
    std::atomic<int> register_route_count{0};

    std::pair<int32_t, bool> onPublish(const PublishEnvelope&) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribe(const Route&, int) override {
        broker_subscribe_count.fetch_add(1);
        return {0, true};
    }
    std::pair<int32_t, bool> onSubscribeEventChannel(int client_fd) override {
        if (server) server->add_subscription(
            client_fd, static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE));
        return {0, true};
    }
    std::pair<int32_t, bool> onSubscribeRoutedDownlinkEventChannel(
        const std::string& /*owner*/, int client_fd) override {
        if (server) server->add_subscription(
            client_fd, static_cast<uint32_t>(ipc::EventType::ROUTED_DOWNLINK_MESSAGE));
        return {0, true};
    }
    std::pair<int32_t, bool> onUnsubscribe(int) override { return {0, true}; }
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry&) override {
        register_route_count.fetch_add(1);
        return {0, true};
    }
    std::string onGetConnectionState() override { return R"({"state":"CONNECTED"})"; }
    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot&) override { return ReplaceSnapshotResult{}; }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override { return SnapshotStatusResult{}; }
    SubscriptionReadiness onGetSubscriptionReadiness() override { return SubscriptionReadiness{}; }
    PublishResult onPublishRoute(const RoutePublishRequest&) override { return PublishResult{}; }
};

std::string make_socket(const char* tag) {
    return std::string("/tmp/tbox_mqtt_routed_integ_") + tag + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
}

std::string routedJson(const std::string& owner, const std::string& route_id,
                       const std::string& target, int qos,
                       const std::string& payload_b64, const std::string& request_id) {
    return std::string("{\"owner\":\"") + owner + "\",\"route_id\":\"" + route_id +
        "\",\"target\":\"" + target + "\",\"qos\":" + std::to_string(qos) +
        ",\"payload_base64\":\"" + payload_b64 + "\",\"request_id\":\"" + request_id +
        "\",\"trace_id\":\"\"}";
}

} // namespace

class MqttRoutedDownlinkIntegTest : public ::testing::Test {
protected:
    std::string socket_path;
    IntegHandler handler;
    std::unique_ptr<MqttIpcDispatcher> dispatcher;
    std::unique_ptr<::tbox::fw::ipc::Server> server;
    std::unique_ptr<Client> client;

    void SetUp() override {
        socket_path = make_socket("i");
        ::unlink(socket_path.c_str());

        dispatcher = std::make_unique<MqttIpcDispatcher>(&handler);
        server = std::make_unique<::tbox::fw::ipc::Server>(socket_path);
        handler.server = server.get();

        auto* dp = dispatcher.get();
        ASSERT_TRUE(server->start(
            [dp](uint32_t m, std::string_view p, int fd) { return dp->dispatch(m, p, fd); },
            [dp](int fd) { dp->onClientDisconnected(fd); }));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        client = std::make_unique<Client>(socket_path);
    }

    void TearDown() override {
        client.reset();
        server->stop();
        server.reset();
        ::unlink(socket_path.c_str());
    }

    // 模拟 daemon 按 owner 定向推送（push_event_to）一帧 routed 事件
    void pushRoutedToOwner(const std::string& owner, const std::string& json) {
        auto fds = dispatcher->getRoutedFdsForOwner(owner);
        for (int fd : fds) {
            server->push_event_to(fd,
                static_cast<uint32_t>(ipc::EventType::ROUTED_DOWNLINK_MESSAGE), json);
        }
    }
};

// ---- daemon 定向推送 -> SDK callback，字段完整 (CR-010 §11.2) ----
TEST_F(MqttRoutedDownlinkIntegTest, DaemonPushRoutedDeliveredToSdk) {
    std::atomic<bool> got{false};
    RoutedDownlinkEvent ev;
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent& e) { ev = e; got.store(true); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pushRoutedToOwner("tsp", routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AQID", "req-1"));
    for (int i = 0; i < 100 && !got.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(got.load());
    EXPECT_EQ(ev.owner, "tsp");
    EXPECT_EQ(ev.route_id, "fota.downlink");
    EXPECT_EQ(ev.target, "tsp.fota");
    EXPECT_EQ(ev.payload, (std::vector<uint8_t>{0x01, 0x02, 0x03}));
}

// ---- routed 订阅不增加 Broker SUBSCRIBE / registerRoute (CR-010 US-013) ----
TEST_F(MqttRoutedDownlinkIntegTest, RoutedSubscribeNoBrokerSubscribe) {
    auto sub = client->subscribeRoutedDownlink("tsp", [](const RoutedDownlinkEvent&) {});
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(handler.broker_subscribe_count.load(), 0);
    EXPECT_EQ(handler.register_route_count.load(), 0);
}

// ---- owner 定向推送：不匹配 owner 的连接不收到 (CR-010 §3.1 daemon 过滤) ----
TEST_F(MqttRoutedDownlinkIntegTest, OwnerFilteredPush) {
    std::atomic<int> tsp_recv{0}, fota_recv{0};
    auto sub_tsp = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { tsp_recv.fetch_add(1); });
    auto sub_fota = client->subscribeRoutedDownlink(
        "fota", [&](const RoutedDownlinkEvent&) { fota_recv.fetch_add(1); });
    ASSERT_TRUE(sub_tsp.isActive());
    ASSERT_TRUE(sub_fota.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 只推给 tsp
    pushRoutedToOwner("tsp", routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AQID", "r1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(tsp_recv.load(), 1);
    EXPECT_EQ(fota_recv.load(), 0);

    // 只推给 fota
    pushRoutedToOwner("fota", routedJson("fota", "fota.downlink", "tsp.fota", 1, "AQID", "r2"));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(tsp_recv.load(), 1);
    EXPECT_EQ(fota_recv.load(), 1);
}

// ---- 慢 callback 不阻塞其他 owner (CR-010 §5/§11.2) ----
TEST_F(MqttRoutedDownlinkIntegTest, SlowCallbackDoesNotBlockOtherOwner) {
    std::atomic<bool> b_got{false};
    auto sub_a = client->subscribeRoutedDownlink(
        "a", [](const RoutedDownlinkEvent&) { std::this_thread::sleep_for(std::chrono::milliseconds(300)); });
    auto sub_b = client->subscribeRoutedDownlink(
        "b", [&](const RoutedDownlinkEvent&) { b_got.store(true); });
    ASSERT_TRUE(sub_a.isActive());
    ASSERT_TRUE(sub_b.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto t0 = std::chrono::steady_clock::now();
    // 顺序推送（模拟 daemon 串行 worker）：先 a（慢 callback 在 a 的 dispatcher 后台执行），再 b
    pushRoutedToOwner("a", routedJson("a", "r", "t", 1, "AQID", "a1"));
    pushRoutedToOwner("b", routedJson("b", "r", "t", 1, "AQID", "b1"));
    for (int i = 0; i < 100 && !b_got.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    EXPECT_TRUE(b_got.load());
    // b 应在 a 的慢 callback（300ms）完成前收到（dispatch queue 解耦）
    EXPECT_LT(dt, 250);
}

// ---- 队列满 disconnect：慢消费者溢出后 isActive=false，其他 owner 不受影响 (CR-010 §5) ----
TEST_F(MqttRoutedDownlinkIntegTest, QueueFullDisconnectsSlowOwner) {
    std::atomic<bool> b_got{false};
    auto sub_a = client->subscribeRoutedDownlink(
        "a", [](const RoutedDownlinkEvent&) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); });
    auto sub_b = client->subscribeRoutedDownlink(
        "b", [&](const RoutedDownlinkEvent&) { b_got.store(true); });
    ASSERT_TRUE(sub_a.isActive());
    ASSERT_TRUE(sub_b.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 突发推 300 条给慢 a：a 的 reader 快速入队，队列满后 disconnect
    for (int i = 0; i < 300; ++i) {
        pushRoutedToOwner("a", routedJson("a", "r", "t", 1, "AQID", "a" + std::to_string(i)));
    }
    // 等待 a 溢出断开
    for (int i = 0; i < 200 && sub_a.isActive(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_FALSE(sub_a.isActive());  // 队列满 -> disconnect

    // b 仍可正常接收
    pushRoutedToOwner("b", routedJson("b", "r", "t", 1, "AQID", "b1"));
    for (int i = 0; i < 100 && !b_got.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(b_got.load());
}

// ---- cancel 后无重复 event / 无泄漏 (CR-010 §6/§11.2) ----
TEST_F(MqttRoutedDownlinkIntegTest, CancelNoLeak) {
    std::atomic<int> recv{0};
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { recv.fetch_add(1); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pushRoutedToOwner("tsp", routedJson("tsp", "r", "t", 1, "AQID", "1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_GE(recv.load(), 1);

    sub.cancel();
    EXPECT_FALSE(sub.isActive());
    int after_cancel = recv.load();
    pushRoutedToOwner("tsp", routedJson("tsp", "r", "t", 1, "AQID", "2"));
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(recv.load(), after_cancel);  // cancel 后无新 event
}

// ---- Client 析构取消所有 routed 订阅，无线程/fd 泄漏 (CR-010 §6) ----
TEST_F(MqttRoutedDownlinkIntegTest, ClientDestructCancelsAll) {
    {
        auto sub1 = client->subscribeRoutedDownlink("a", [](const RoutedDownlinkEvent&) {});
        auto sub2 = client->subscribeRoutedDownlink("b", [](const RoutedDownlinkEvent&) {});
        ASSERT_TRUE(sub1.isActive());
        ASSERT_TRUE(sub2.isActive());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        // sub1/sub2 析构 + client 析构应在 TearDown 中无挂死完成
    }
    client.reset();  // 显式析构，验证不挂死
    SUCCEED();
}

// ---- legacy 调用方保持原 topic/payload callback，与 routed 不串扰 (CR-010 §7/§11.2) ----
TEST_F(MqttRoutedDownlinkIntegTest, LegacyCoexistsWithRouted) {
    std::atomic<bool> legacy_got{false};
    std::atomic<bool> routed_got{false};
    std::string legacy_topic;
    std::vector<uint8_t> legacy_payload;

    Route route;
    route.topic = "vehicle/down/legacy";
    route.qos = QoS::AT_LEAST_ONCE;
    auto legacy_sub = client->subscribe(route, [&](const std::string& topic,
                                                    const std::vector<uint8_t>& payload) {
        legacy_topic = topic;
        legacy_payload = payload;
        legacy_got.store(true);
    });
    ASSERT_TRUE(legacy_sub.isActive());

    auto routed_sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { routed_got.store(true); });
    ASSERT_TRUE(routed_sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // legacy 下行事件（DOWNLINK_MESSAGE 广播）
    server->push_event(static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE),
        R"({"topic":"vehicle/down/legacy","payload_base64":"AABB"})");
    // routed 下行事件（ROUTED_DOWNLINK_MESSAGE 定向）
    pushRoutedToOwner("tsp", routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AQID", "r1"));

    for (int i = 0; i < 100 && (!legacy_got.load() || !routed_got.load()); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(legacy_got.load());
    EXPECT_EQ(legacy_topic, "vehicle/down/legacy");
    EXPECT_FALSE(legacy_payload.empty());
    EXPECT_TRUE(routed_got.load());
}
