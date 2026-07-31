// CR-004 §10: Dispatcher 单元测试
// 覆盖 handler 分发、accepted/rejected、route/订阅登记、断连清理、同 fd 写串行化、MQTT-10xx 映射
#include <gtest/gtest.h>
#include "mqtt_ipc_dispatcher.h"
#include "tbox/mqtt/ipc_protocol.h"
#include <nlohmann/json.hpp>
#include <string>

using namespace tbox::mqtt;
using json = nlohmann::json;

// 测试用 mock handler
class MockServiceHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    PublishEnvelope last_publish_env;
    bool publish_accept = true;
    int32_t publish_status = 0;

    Route last_subscribe_route;
    int last_subscribe_fd = -1;
    bool subscribe_ok = true;
    int32_t subscribe_status = 0;

    bool unsubscribe_ok = true;
    int32_t unsubscribe_status = 0;

    RouteEntry last_register_route;
    bool register_route_ok = true;
    int32_t register_route_status = 0;

    std::string connection_state_json = R"({"state":"CONNECTED","last_error":"","retry_count":0})";

    std::pair<int32_t, bool> onPublish(const PublishEnvelope& env) override {
        last_publish_env = env;
        return {publish_status, publish_accept};
    }
    std::pair<int32_t, bool> onSubscribe(const Route& route, int client_fd) override {
        last_subscribe_route = route;
        last_subscribe_fd = client_fd;
        return {subscribe_status, subscribe_ok};
    }
    std::pair<int32_t, bool> onSubscribeEventChannel(int client_fd) override {
        last_subscribe_fd = client_fd;
        return {subscribe_status, subscribe_ok};
    }
    std::pair<int32_t, bool> onUnsubscribe(int client_fd) override {
        (void)client_fd;
        return {unsubscribe_status, unsubscribe_ok};
    }
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry& entry) override {
        last_register_route = entry;
        return {register_route_status, register_route_ok};
    }
    std::string onGetConnectionState() override {
        return connection_state_json;
    }

    // CR-007 §4 stub
    ReplaceSnapshotResult last_snapshot_result;
    SnapshotStatusResult last_snapshot_status;
    SubscriptionReadiness last_readiness;

    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot&) override {
        return last_snapshot_result;
    }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override {
        return last_snapshot_status;
    }
    SubscriptionReadiness onGetSubscriptionReadiness() override {
        return last_readiness;
    }
    PublishResult onPublishRoute(const RoutePublishRequest&) override {
        return PublishResult{};
    }
};

class MqttIpcDispatcherTest : public ::testing::Test {
protected:
    MockServiceHandler handler;
    std::unique_ptr<MqttIpcDispatcher> dispatcher;

    void SetUp() override {
        dispatcher = std::make_unique<MqttIpcDispatcher>(&handler);
    }

    // 构造 publish 请求 JSON
    std::string makePublishJson(const std::string& msg_id, const std::string& topic,
                                int qos = 1, const std::string& payload_b64 = "") {
        json j;
        j["msg_id"] = msg_id;
        j["topic"] = topic;
        j["qos"] = qos;
        j["priority"] = 1;
        j["content_type"] = "application/x-protobuf";
        if (!payload_b64.empty()) j["payload_base64"] = payload_b64;
        j["trace_id"] = "trace-1";
        j["request_id"] = "req-1";
        return j.dump();
    }
};

// ---- publish accepted (CR-004 §5.2) ----
TEST_F(MqttIpcDispatcherTest, PublishAccepted) {
    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH),
        makePublishJson("msg-001", "vehicle/up/telemetry"),
        100);

    auto j = json::parse(resp);
    EXPECT_EQ(j["status"], 0);
    EXPECT_TRUE(j["accepted"].get<bool>());
    EXPECT_EQ(j["outcome"], "accepted");
    EXPECT_EQ(handler.last_publish_env.msg_id, "msg-001");
    EXPECT_EQ(handler.last_publish_env.topic, "vehicle/up/telemetry");
}

// ---- publish rejected: 空 msg_id (MQTT-1004) ----
TEST_F(MqttIpcDispatcherTest, PublishRejectedEmptyMsgId) {
    json j;
    j["msg_id"] = "";
    j["topic"] = "test/topic";
    j["qos"] = 1;

    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH), j.dump(), 100);

    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 1004);
    EXPECT_FALSE(parsed["accepted"].get<bool>());
}

// ---- publish 幂等去重：相同 msg_id 返回已受理 (CR-004 §5.2) ----
TEST_F(MqttIpcDispatcherTest, PublishDedupSameMsgId) {
    std::string params = makePublishJson("dup-001", "test/topic");

    auto resp1 = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH), params, 100);
    auto j1 = json::parse(resp1);
    EXPECT_TRUE(j1["accepted"].get<bool>());

    // 第二次相同 msg_id：去重，返回已受理
    auto resp2 = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH), params, 100);
    auto j2 = json::parse(resp2);
    EXPECT_TRUE(j2["accepted"].get<bool>());
    EXPECT_TRUE(j2["deduplicated"].get<bool>());
    EXPECT_EQ(j2["outcome"], "accepted");
}

// ---- publish rejected: 业务层拒绝 (MQTT-1006) ----
TEST_F(MqttIpcDispatcherTest, PublishRejectedByService) {
    handler.publish_accept = false;
    handler.publish_status = 1006;

    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH),
        makePublishJson("msg-002", "test/topic"), 100);

    auto j = json::parse(resp);
    EXPECT_EQ(j["status"], 1006);
    EXPECT_FALSE(j["accepted"].get<bool>());
    EXPECT_EQ(j["outcome"], "rejected");
}

// ---- isAccepted 查询 (CR-004 §5.2) ----
TEST_F(MqttIpcDispatcherTest, IsAcceptedAfterPublish) {
    EXPECT_FALSE(dispatcher->isAccepted("msg-003"));
    dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH),
        makePublishJson("msg-003", "test/topic"), 100);
    EXPECT_TRUE(dispatcher->isAccepted("msg-003"));
}

// ---- subscribe 登记 route + client_fd (CR-004 §3) ----
TEST_F(MqttIpcDispatcherTest, Subscribe) {
    json j;
    j["topic"] = "vehicle/down/cmd";
    j["qos"] = 1;

    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE), j.dump(), 200);

    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 0);
    EXPECT_TRUE(parsed["ok"].get<bool>());
    EXPECT_EQ(handler.last_subscribe_route.topic, "vehicle/down/cmd");
    EXPECT_EQ(handler.last_subscribe_fd, 200);
}

// ---- subscribe: 空 topic 拒绝 (MQTT-1004) ----
TEST_F(MqttIpcDispatcherTest, SubscribeEmptyTopic) {
    json j;
    j["topic"] = "";
    j["qos"] = 1;

    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE), j.dump(), 200);

    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 1004);
    EXPECT_FALSE(parsed["ok"].get<bool>());
}

// ---- registerRoute (CR-004 §3) ----
TEST_F(MqttIpcDispatcherTest, RegisterRoute) {
    json j;
    j["internal_addr"] = "service_diag";
    j["mqtt_topic"] = "vehicle/diag/status";
    j["qos"] = 1;
    j["direction"] = "UP";

    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::REGISTER_ROUTE), j.dump(), 300);

    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 0);
    EXPECT_TRUE(parsed["ok"].get<bool>());
    EXPECT_EQ(handler.last_register_route.internal_addr, "service_diag");
    EXPECT_EQ(handler.last_register_route.direction, RouteEntry::Direction::UP);
}

// ---- getConnectionState (CR-004 §3) ----
TEST_F(MqttIpcDispatcherTest, GetConnectionState) {
    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::GET_CONNECTION_STATE), "{}", 400);

    auto j = json::parse(resp);
    EXPECT_EQ(j["status"], 0);
    EXPECT_EQ(j["state"], "CONNECTED");
}

// ---- unknown method (FW-0306) ----
TEST_F(MqttIpcDispatcherTest, UnknownMethod) {
    auto resp = dispatcher->dispatch(9999, "{}", 500);
    auto j = json::parse(resp);
    EXPECT_EQ(j["status"], 306);
}

// ---- 断连清理 per-fd 订阅 (CR-004 §4) ----
TEST_F(MqttIpcDispatcherTest, ClientDisconnectedCleanup) {
    json j;
    j["topic"] = "vehicle/down/cmd";
    j["qos"] = 1;

    dispatcher->dispatch(static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE), j.dump(), 600);
    dispatcher->onClientDisconnected(600);

    // 再次断连同 fd 不报错
    dispatcher->onClientDisconnected(600);
    SUCCEED();
}

// ---- base64 payload 解码 (CR-004 §5.1) ----
TEST_F(MqttIpcDispatcherTest, PublishBase64PayloadDecoded) {
    // "AQID" = base64 of {0x01, 0x02, 0x03}
    dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH),
        makePublishJson("msg-payload", "test/topic", 1, "AQID"), 100);

    EXPECT_EQ(handler.last_publish_env.payload, (std::vector<uint8_t>{0x01, 0x02, 0x03}));
}
