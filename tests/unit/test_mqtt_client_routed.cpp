// CR-010 §11.1: Routed downlink SDK 单元测试
// 覆盖完整字段转换、缺失/类型错误/owner 不匹配/QoS 越界、base64 合法/非法/空/超限、
// callback 锁外/异常继续/顺序、同 owner 重复冲突在 IPC 前失败。
#include <gtest/gtest.h>
#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "ipc.h"
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace tbox::mqtt;

namespace {

// 测试用 service handler：routed 订阅时登记 fd 到 ROUTED_DOWNLINK_MESSAGE
class RoutedTestHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    ::tbox::fw::ipc::Server* server = nullptr;
    std::atomic<int> subscribe_count{0};      // legacy subscribe 次数（routed 不应触发）
    std::atomic<int> register_route_count{0};

    std::pair<int32_t, bool> onPublish(const PublishEnvelope&) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribe(const Route&, int) override {
        subscribe_count.fetch_add(1);
        return {0, true};
    }
    std::pair<int32_t, bool> onSubscribeEventChannel(int) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribeRoutedDownlinkEventChannel(
        const std::string& /*owner*/, int client_fd) override {
        if (server) {
            server->add_subscription(
                client_fd, static_cast<uint32_t>(ipc::EventType::ROUTED_DOWNLINK_MESSAGE));
        }
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
    return std::string("/tmp/tbox_mqtt_routed_") + tag + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
}

// 构造合法 routed downlink JSON
std::string routedJson(const std::string& owner = "tsp",
                       const std::string& route_id = "fota.downlink",
                       const std::string& target = "tsp.fota",
                       int qos = 1,
                       const std::string& payload_b64 = "AQID",
                       const std::string& request_id = "req-001",
                       const std::string& trace_id = "trace-001") {
    return std::string("{\"owner\":\"") + owner +
        "\",\"route_id\":\"" + route_id +
        "\",\"target\":\"" + target +
        "\",\"qos\":" + std::to_string(qos) +
        ",\"payload_base64\":\"" + payload_b64 +
        "\",\"request_id\":\"" + request_id +
        "\",\"trace_id\":\"" + trace_id + "\"}";
}

} // namespace

class MqttClientRoutedTest : public ::testing::Test {
protected:
    std::string socket_path;
    RoutedTestHandler handler;
    std::unique_ptr<MqttIpcDispatcher> dispatcher;
    std::unique_ptr<::tbox::fw::ipc::Server> server;
    std::unique_ptr<Client> client;

    void SetUp() override {
        socket_path = make_socket("u");
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

    // 向 ROUTED_DOWNLINK_MESSAGE 订阅者广播一帧 routed 事件
    void pushRouted(const std::string& json) {
        server->push_event(
            static_cast<uint32_t>(ipc::EventType::ROUTED_DOWNLINK_MESSAGE), json);
    }
};

// ---- 完整 routed JSON -> typed event，所有字段一致 (CR-010 §11.1) ----
TEST_F(MqttClientRoutedTest, CompleteRoutedEventDelivered) {
    std::atomic<bool> got{false};
    RoutedDownlinkEvent ev;
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent& e) { ev = e; got.store(true); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pushRouted(routedJson());
    for (int i = 0; i < 50 && !got.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(got.load());

    EXPECT_EQ(ev.owner, "tsp");
    EXPECT_EQ(ev.route_id, "fota.downlink");
    EXPECT_EQ(ev.target, "tsp.fota");
    EXPECT_EQ(ev.qos, 1);
    EXPECT_EQ(ev.payload, (std::vector<uint8_t>{0x01, 0x02, 0x03}));
    EXPECT_EQ(ev.request_id, "req-001");
    EXPECT_EQ(ev.trace_id, "trace-001");
}

// ---- routed 订阅不触发 Broker subscribe/registerRoute (CR-010 US-013) ----
TEST_F(MqttClientRoutedTest, RoutedSubscribeDoesNotTriggerBrokerSubscribe) {
    auto sub = client->subscribeRoutedDownlink("tsp", [](const RoutedDownlinkEvent&) {});
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_EQ(handler.subscribe_count.load(), 0);
    EXPECT_EQ(handler.register_route_count.load(), 0);
}

// ---- 缺失必填字段被拒绝，后续合法事件仍可交付 (CR-010 §11.1) ----
TEST_F(MqttClientRoutedTest, MissingFieldsRejectedThenValidDelivered) {
    std::atomic<int> received{0};
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { received.fetch_add(1); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 缺 owner / route_id / target / qos / payload_base64 / request_id
    pushRouted(routedJson("", "fota.downlink", "tsp.fota"));
    pushRouted(routedJson("tsp", "", "tsp.fota"));
    pushRouted(routedJson("tsp", "fota.downlink", ""));
    pushRouted("{\"owner\":\"tsp\",\"route_id\":\"fota.downlink\",\"target\":\"tsp.fota\","
               "\"payload_base64\":\"AQID\",\"request_id\":\"r\"}");  // 缺 qos
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "", "r"));  // 空 payload 合法
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(received.load(), 1);  // 仅空 payload 事件合法

    // 后续合法事件仍可交付
    pushRouted(routedJson());
    for (int i = 0; i < 50 && received.load() < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(received.load(), 2);
}

// ---- owner 不匹配被拒绝 (CR-010 §4 step 3, SDK 二次验证) ----
TEST_F(MqttClientRoutedTest, OwnerMismatchRejected) {
    std::atomic<int> received{0};
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { received.fetch_add(1); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pushRouted(routedJson("other"));  // 事件 owner=other != 订阅 owner=tsp
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(received.load(), 0);
}

// ---- QoS 越界被拒绝 (CR-010 §11.1) ----
TEST_F(MqttClientRoutedTest, QosOutOfRangeRejected) {
    std::atomic<int> received{0};
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { received.fetch_add(1); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 3));  // qos=3 非法
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", -1)); // qos=-1 非法
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 2));  // qos=2 合法
    for (int i = 0; i < 50 && received.load() < 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(received.load(), 1);
}

// ---- 非法 base64 被拒绝，不产生部分 payload (CR-010 §4) ----
TEST_F(MqttClientRoutedTest, InvalidBase64Rejected) {
    std::atomic<int> received{0};
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { received.fetch_add(1); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "!!!!"));   // 非法字符
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AQI"));    // 长度非 4 倍数
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "A==="));   // 非法填充
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(received.load(), 0);

    // 合法 base64 边界：1 字节
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AA=="));
    for (int i = 0; i < 50 && received.load() < 1; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(received.load(), 1);
}

// ---- 超限 payload 被拒绝 (CR-010 §4, payload 上限) ----
TEST_F(MqttClientRoutedTest, OversizePayloadRejected) {
    std::atomic<int> received{0};
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { received.fetch_add(1); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 生成 > 1MiB 的 base64（解码后超过上限）
    std::string big(static_cast<size_t>(1400000), 'A');
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, big));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(received.load(), 0);
}

// ---- 事件顺序保持 (CR-010 §5) ----
TEST_F(MqttClientRoutedTest, EventOrderPreserved) {
    std::vector<std::string> got_request_ids;
    std::mutex m;
    auto sub = client->subscribeRoutedDownlink("tsp", [&](const RoutedDownlinkEvent& e) {
        std::lock_guard<std::mutex> lk(m);
        got_request_ids.push_back(e.request_id);
    });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (int i = 0; i < 10; ++i) {
        pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AQID",
                              "req-" + std::to_string(i)));
    }
    for (int i = 0; i < 100 && got_request_ids.size() < 10; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(got_request_ids.size(), 10u);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(got_request_ids[i], "req-" + std::to_string(i));
    }
}

// ---- callback 抛异常不终止读取线程，后续事件仍交付 (CR-010 §5) ----
TEST_F(MqttClientRoutedTest, CallbackExceptionContinues) {
    std::atomic<int> received{0};
    auto sub = client->subscribeRoutedDownlink("tsp", [&](const RoutedDownlinkEvent& e) {
        received.fetch_add(1);
        if (e.request_id == "boom") throw std::runtime_error("biz error");
    });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AQID", "boom"));
    pushRouted(routedJson("tsp", "fota.downlink", "tsp.fota", 1, "AQID", "after"));
    for (int i = 0; i < 100 && received.load() < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(received.load(), 2);
    EXPECT_TRUE(sub.isActive());  // 读取线程仍在
}

// ---- 同 owner 重复 routed 订阅在 IPC 前失败 (CR-010 §7) ----
TEST_F(MqttClientRoutedTest, SameOwnerDuplicateRejectedBeforeIpc) {
    auto sub1 = client->subscribeRoutedDownlink("tsp", [](const RoutedDownlinkEvent&) {});
    ASSERT_TRUE(sub1.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int before = handler.subscribe_count.load();
    auto sub2 = client->subscribeRoutedDownlink("tsp", [](const RoutedDownlinkEvent&) {});
    EXPECT_FALSE(sub2.isActive());  // 互斥冲突：空句柄
    // 未触发额外 IPC 订阅
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handler.subscribe_count.load(), before);
}

// ---- 不同 owner 可共存 (CR-010 §11.2) ----
TEST_F(MqttClientRoutedTest, DifferentOwnersCoexist) {
    auto sub1 = client->subscribeRoutedDownlink("tsp", [](const RoutedDownlinkEvent&) {});
    auto sub2 = client->subscribeRoutedDownlink("fota", [](const RoutedDownlinkEvent&) {});
    ASSERT_TRUE(sub1.isActive());
    ASSERT_TRUE(sub2.isActive());
}

// ---- cancel 后不再交付，句柄非活跃 (CR-010 §6) ----
TEST_F(MqttClientRoutedTest, CancelStopsDelivery) {
    std::atomic<int> received{0};
    auto sub = client->subscribeRoutedDownlink(
        "tsp", [&](const RoutedDownlinkEvent&) { received.fetch_add(1); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    sub.cancel();
    EXPECT_FALSE(sub.isActive());
    pushRouted(routedJson());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(received.load(), 0);
}

// ---- 参数非法返回空句柄 (CR-010 §2.2) ----
TEST_F(MqttClientRoutedTest, InvalidOwnerReturnsInactive) {
    auto sub1 = client->subscribeRoutedDownlink("", [](const RoutedDownlinkEvent&) {});
    EXPECT_FALSE(sub1.isActive());
    auto sub2 = client->subscribeRoutedDownlink("bad owner!", [](const RoutedDownlinkEvent&) {});
    EXPECT_FALSE(sub2.isActive());
}
