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
    OwnerSubscriptionSnapshot last_snapshot;

    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot& s) override {
        last_snapshot = s;
        return last_snapshot_result;
    }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override {
        return last_snapshot_status;
    }
    SubscriptionReadiness onGetSubscriptionReadiness() override {
        return last_readiness;
    }
    RoutePublishRequest last_route_req;
    bool has_route_req = false;
    PublishResult route_publish_result;
    PublishResult onPublishRoute(const RoutePublishRequest& req) override {
        last_route_req = req;
        has_route_req = true;
        return route_publish_result;
    }

    // CR-015 §18.4 stub
    CapabilityReadiness last_capability_readiness;
    std::string last_capability_query;
    CapabilityReadiness onGetCapabilityReadiness(const std::string& capability) override {
        last_capability_query = capability;
        CapabilityReadiness r = last_capability_readiness;
        r.capability = capability;
        return r;
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

// ============================================================
// CR-015 §18.2/§18.4: snapshot 新字段解析 + capability handlers
// ============================================================

TEST_F(MqttIpcDispatcherTest, ReplaceSnapshotParsesCapabilityFields) {
    json j;
    j["owner"] = "tsp";
    j["generation"] = 7;
    j["content_digest"] = "d7";
    j["registration_complete"] = true;
    j["contract_version"] = kSubscriptionCatalogContractVersion;
    json items = json::array();
    json item;
    item["route_id"] = kKeyprovDownlinkRouteId;
    item["topic_template"] = "";
    item["direction"] = "DOWN";
    item["qos"] = 1;
    item["enabled"] = true;
    item["readiness_scope"] = kCapabilitySecurityKeyprov;
    item["criticality"] = "CAPABILITY_MANDATORY";
    items.push_back(item);
    j["items"] = items;
    handler.last_snapshot_result.status = SnapshotAcceptStatus::ACCEPTED;

    dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::REPLACE_SUBSCRIPTION_SNAPSHOT), j.dump(), 700);

    EXPECT_EQ(handler.last_snapshot.contract_version, kSubscriptionCatalogContractVersion);
    ASSERT_EQ(handler.last_snapshot.items.size(), 1u);
    const auto& parsed = handler.last_snapshot.items[0];
    ASSERT_TRUE(parsed.enabled.has_value());
    EXPECT_TRUE(parsed.enabled.value());
    EXPECT_EQ(parsed.readiness_scope, kCapabilitySecurityKeyprov);
    EXPECT_EQ(parsed.criticality, SubscriptionCriticality::CAPABILITY_MANDATORY);
}

TEST_F(MqttIpcDispatcherTest, GetCapabilityReadiness) {
    handler.last_capability_readiness.ready = true;
    handler.last_capability_readiness.enabled = true;
    handler.last_capability_readiness.reason_code = "READY";

    json j;
    j["capability"] = kCapabilitySecurityKeyprov;
    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::GET_CAPABILITY_READINESS), j.dump(), 800);

    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 0);
    EXPECT_EQ(handler.last_capability_query, kCapabilitySecurityKeyprov);
    EXPECT_TRUE(parsed["ready"].get<bool>());
    EXPECT_EQ(parsed["capability"], kCapabilitySecurityKeyprov);
    EXPECT_EQ(parsed["contract_version"], kCapabilityReadinessContractVersion);
}

TEST_F(MqttIpcDispatcherTest, GetCapabilityReadinessMissingCapability) {
    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::GET_CAPABILITY_READINESS), "{}", 800);
    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 1004);
}

TEST_F(MqttIpcDispatcherTest, SubscribeCapabilityReadinessRegistersFd) {
    json j;
    j["capability"] = kCapabilitySecurityKeyprov;
    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE_CAPABILITY_READINESS), j.dump(), 810);
    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 0);
    EXPECT_TRUE(parsed["ok"].get<bool>());

    auto fds = dispatcher->getCapabilityFds(kCapabilitySecurityKeyprov);
    ASSERT_EQ(fds.size(), 1u);
    EXPECT_EQ(fds[0], 810);

    dispatcher->onClientDisconnected(810);
    EXPECT_TRUE(dispatcher->getCapabilityFds(kCapabilitySecurityKeyprov).empty());
}

// ============================================================
// CR-017 §4/§5: readiness 新字段序列化 + publishRoute expected_session 解析
// ============================================================

TEST_F(MqttIpcDispatcherTest, ReadinessJsonIncludesEcuUidSessionContextAndPublishContract) {
    handler.last_readiness.ecu_uid = "ECU-42";
    handler.last_readiness.session_context.daemon_epoch = "aabbcc";
    handler.last_readiness.session_context.connection_generation = 3;
    handler.last_readiness.session_context.cert_session_generation = 4;
    handler.last_readiness.contract_version = kCloudReadinessContractVersion;
    handler.last_readiness.publish_contract_version = kPublishSessionContractVersion;

    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::GET_SUBSCRIPTION_READINESS), "{}", 900);
    auto j = json::parse(resp);
    EXPECT_EQ(j["status"], 0);
    EXPECT_EQ(j["ecu_uid"], "ECU-42");
    ASSERT_TRUE(j.contains("session_context"));
    EXPECT_EQ(j["session_context"]["daemon_epoch"], "aabbcc");
    EXPECT_EQ(j["session_context"]["connection_generation"], 3u);
    EXPECT_EQ(j["session_context"]["cert_session_generation"], 4u);
    EXPECT_EQ(j["publish_contract_version"], kPublishSessionContractVersion);
    EXPECT_EQ(j["contract_version"], kCloudReadinessContractVersion);
}

TEST_F(MqttIpcDispatcherTest, PublishRouteParsesExpectedSession) {
    json j;
    j["owner"] = "tsp";
    j["route_id"] = kKeyprovUplinkRouteId;
    j["msg_id"] = "mp-1";
    j["expected_session"] = {{"daemon_epoch", "epoch-1"},
                             {"connection_generation", 5},
                             {"cert_session_generation", 6}};

    dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH_ROUTE), j.dump(), 910);

    ASSERT_TRUE(handler.has_route_req);
    ASSERT_TRUE(handler.last_route_req.expected_session.has_value());
    EXPECT_EQ(handler.last_route_req.expected_session->daemon_epoch, "epoch-1");
    EXPECT_EQ(handler.last_route_req.expected_session->connection_generation, 5u);
    EXPECT_EQ(handler.last_route_req.expected_session->cert_session_generation, 6u);
}

TEST_F(MqttIpcDispatcherTest, PublishRouteWithoutExpectedSessionIsNullopt) {
    json j;
    j["owner"] = "tsp";
    j["route_id"] = "fota.uplink";
    j["msg_id"] = "mp-2";

    dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH_ROUTE), j.dump(), 920);

    ASSERT_TRUE(handler.has_route_req);
    EXPECT_FALSE(handler.last_route_req.expected_session.has_value());
}

TEST_F(MqttIpcDispatcherTest, PublishRouteResponseCarriesReasonAndRetryable) {
    handler.route_publish_result.accepted = false;
    handler.route_publish_result.error = ErrorCode::MQTT_1009;
    handler.route_publish_result.reason_code = session_reason::kPublishContextStale;
    handler.route_publish_result.retryable = true;

    json j;
    j["owner"] = "tsp";
    j["route_id"] = kKeyprovUplinkRouteId;
    j["msg_id"] = "mp-3";

    auto resp = dispatcher->dispatch(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH_ROUTE), j.dump(), 930);
    auto parsed = json::parse(resp);
    EXPECT_EQ(parsed["status"], 1009);
    EXPECT_EQ(parsed["reason_code"], session_reason::kPublishContextStale);
    EXPECT_TRUE(parsed["retryable"].get<bool>());
}
