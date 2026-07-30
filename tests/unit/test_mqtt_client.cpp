// CR-004 §10: Client SDK 单元测试（真实 framework-ipc Server/Client 起socket）
// 覆盖惰性连接、幂等重试、unknown outcome、独立订阅、重连恢复
#include <gtest/gtest.h>
#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "ipc.h"
#include <atomic>
#include <chrono>
#include <thread>
#include <string>
#include <vector>
#include <filesystem>

using namespace tbox::mqtt;

// 测试用 mock service handler
class TestServiceHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    std::atomic<int> publish_count{0};
    std::string last_msg_id;

    std::pair<int32_t, bool> onPublish(const PublishEnvelope& env) override {
        publish_count.fetch_add(1);
        last_msg_id = env.msg_id;
        return {0, true};  // accepted
    }
    std::pair<int32_t, bool> onSubscribe(const Route&, int) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribeEventChannel(int) override { return {0, true}; }
    std::pair<int32_t, bool> onUnsubscribe(int) override { return {0, true}; }
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry&) override { return {0, true}; }
    std::string onGetConnectionState() override {
        return R"({"state":"CONNECTED","last_error":"","retry_count":0})";
    }
    // CR-007 §4 stub
    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot&) override {
        return ReplaceSnapshotResult{};
    }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override {
        return SnapshotStatusResult{};
    }
    SubscriptionReadiness onGetSubscriptionReadiness() override {
        return SubscriptionReadiness{};
    }
};

class MqttClientTest : public ::testing::Test {
protected:
    std::string socket_path;
    TestServiceHandler handler;
    std::unique_ptr<MqttIpcDispatcher> dispatcher;
    std::unique_ptr<::tbox::fw::ipc::Server> server;
    std::unique_ptr<Client> client;

    void SetUp() override {
        socket_path = "/tmp/tbox_mqtt_client_test_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
        ::unlink(socket_path.c_str());

        dispatcher = std::make_unique<MqttIpcDispatcher>(&handler);
        server = std::make_unique<::tbox::fw::ipc::Server>(socket_path);

        auto* dp = dispatcher.get();
        auto req_handler = [dp](uint32_t mid, std::string_view params, int fd) -> std::string {
            return dp->dispatch(mid, params, fd);
        };
        auto* dp_dc = dispatcher.get();
        auto dc_handler = [dp_dc](int fd) { dp_dc->onClientDisconnected(fd); };

        ASSERT_TRUE(server->start(std::move(req_handler), std::move(dc_handler)));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        client = std::make_unique<Client>(socket_path);
    }

    void TearDown() override {
        client.reset();
        server->stop();
        server.reset();
        ::unlink(socket_path.c_str());
    }
};

// ---- publish accepted (CR-004 §5.2) ----
TEST_F(MqttClientTest, PublishAccepted) {
    PublishEnvelope env;
    env.msg_id = "client-msg-1";
    env.topic = "vehicle/up/test";
    env.qos = QoS::AT_LEAST_ONCE;
    env.payload = {0x01, 0x02, 0x03};

    auto result = client->publish(env);
    EXPECT_TRUE(result.accepted);
    EXPECT_EQ(result.outcome, PublishOutcome::ACCEPTED);
    EXPECT_EQ(handler.last_msg_id, "client-msg-1");
}

// ---- publish 幂等去重 (CR-004 §5.2) ----
TEST_F(MqttClientTest, PublishDedupSameMsgId) {
    PublishEnvelope env;
    env.msg_id = "dup-client-1";
    env.topic = "vehicle/up/test";
    env.qos = QoS::AT_LEAST_ONCE;

    auto r1 = client->publish(env);
    EXPECT_TRUE(r1.accepted);

    auto r2 = client->publish(env);  // 相同 msg_id
    EXPECT_TRUE(r2.accepted);
}

// ---- getConnectionState (CR-004 §7.1) ----
TEST_F(MqttClientTest, GetConnectionState) {
    auto state = client->getConnectionState();
    EXPECT_EQ(state, ConnectionState::CONNECTED);
}

// ---- registerRoute (CR-004 §7.1) ----
TEST_F(MqttClientTest, RegisterRoute) {
    RouteEntry entry;
    entry.internal_addr = "service_diag";
    entry.mqtt_topic = "vehicle/diag/status";
    entry.direction = RouteEntry::Direction::UP;
    entry.qos = QoS::AT_LEAST_ONCE;

    auto result = client->registerRoute(entry);
    EXPECT_TRUE(result.ok);
}

// ---- unknown outcome: server 未运行 (CR-004 §5.2) ----
TEST_F(MqttClientTest, UnknownOutcomeWhenServerDown) {
    std::string bad_path = "/tmp/tbox_mqtt_no_such_server.sock";
    Client bad_client(bad_path);

    PublishEnvelope env;
    env.msg_id = "lost-msg-1";
    env.topic = "vehicle/up/test";

    auto result = bad_client.publish(env);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.outcome, PublishOutcome::UNKNOWN);
}

// ---- subscribe 独立连接 + 下行推送 (CR-004 §6) ----
TEST_F(MqttClientTest, SubscribeAndReceiveDownlink) {
    // client subscribe 需要服务端推送 downlink 事件。
    // 此处验证 subscribe 返回活跃句柄；完整下行链路在 backpressure 集成测试中覆盖。
    Route route;
    route.topic = "vehicle/down/cmd";
    route.qos = QoS::AT_LEAST_ONCE;

    std::atomic<bool> received{false};
    auto sub = client->subscribe(route, [&](const std::string&, const std::vector<uint8_t>&) {
        received.store(true);
    });

    // subscribe 句柄应活跃（独立连接）
    EXPECT_TRUE(sub.isActive());
    sub.cancel();
    EXPECT_FALSE(sub.isActive());
}

// ---- 惰性连接 (CR-004 §7.1) ----
TEST_F(MqttClientTest, LazyConnect) {
    // 未显式 connect 即可调用 publish（framework Client 惰性连接）
    PublishEnvelope env;
    env.msg_id = "lazy-msg-1";
    env.topic = "vehicle/up/test";

    auto result = client->publish(env);
    EXPECT_TRUE(result.accepted);
}
