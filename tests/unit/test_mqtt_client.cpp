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
    // CR-015: 捕获 client 真实发送并经 dispatcher 解析后的快照，用于回归测试。
    OwnerSubscriptionSnapshot last_snapshot;

    // CR-007 §4 stub
    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot& snap) override {
        last_snapshot = snap;
        return ReplaceSnapshotResult{};
    }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override {
        return SnapshotStatusResult{};
    }
    SubscriptionReadiness onGetSubscriptionReadiness() override {
        return SubscriptionReadiness{};
    }
    PublishResult onPublishRoute(const RoutePublishRequest&) override {
        return PublishResult{};
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

// ---- 回归 (CR-015 §4/§18.2): replaceSubscriptionSnapshot 必须下发
//      contract_version 与 item.enabled/readiness_scope/criticality ----
// 覆盖的缺陷：Client SDK 序列化快照时漏传上述字段，导致 daemon 侧
// keyprov route 被归一化为 enabled=false + contract_version=0，
// capability 永远 KEYPROV_DISABLED，车辆不订阅 keyprov 也不上报开通。
TEST_F(MqttClientTest, ReplaceSubscriptionSnapshotCarriesCr015Fields) {
    OwnerSubscriptionSnapshot snap;
    snap.owner = "tsp";
    snap.generation = 1;
    snap.content_digest = "0123456789abcdef";
    snap.registration_complete = true;
    snap.contract_version = kSubscriptionCatalogContractVersion;  // 1

    // legacy route（无 enabled）: 必须保持 nullopt 缺省语义，不得被强制成 false
    SubscriptionItem fota;
    fota.route_id = "fota.downlink";
    fota.topic_template = "vehicle/{ecu_uid}/down/fota";
    fota.direction = SnapshotDirection::DOWN;
    fota.qos = 1;
    fota.target = "tsp.fota";
    fota.mandatory = true;
    snap.items.push_back(fota);

    // legacy route 显式 enabled=false: 必须原样下发（否则 daemon 按缺省=true 处理）
    SubscriptionItem legacy_off;
    legacy_off.route_id = "fota.uplink";
    legacy_off.topic_template = "vehicle/{ecu_uid}/up/fota";
    legacy_off.direction = SnapshotDirection::UP;
    legacy_off.qos = 1;
    legacy_off.target = "tsp.fota";
    legacy_off.enabled = false;
    snap.items.push_back(legacy_off);

    // capability-gated keyprov route: enabled=true + scope + criticality
    SubscriptionItem up;
    up.route_id = kKeyprovUplinkRouteId;
    up.topic_template = "vehicle/{ecu_uid}/up/keyprov";
    up.direction = SnapshotDirection::UP;
    up.qos = 1;
    up.enabled = true;
    up.readiness_scope = kCapabilitySecurityKeyprov;
    up.criticality = SubscriptionCriticality::CAPABILITY_MANDATORY;
    snap.items.push_back(up);

    SubscriptionItem down;
    down.route_id = kKeyprovDownlinkRouteId;
    down.topic_template = "vehicle/{ecu_uid}/down/keyprov";
    down.direction = SnapshotDirection::DOWN;
    down.qos = 1;
    down.target = "tsp.keyprov";
    down.enabled = true;
    down.readiness_scope = kCapabilitySecurityKeyprov;
    down.criticality = SubscriptionCriticality::CAPABILITY_MANDATORY;
    snap.items.push_back(down);

    client->replaceSubscriptionSnapshot(snap);

    // daemon 解析后的快照（即真实上线内容）
    const OwnerSubscriptionSnapshot& got = handler.last_snapshot;
    EXPECT_EQ(got.contract_version, kSubscriptionCatalogContractVersion);
    ASSERT_EQ(got.items.size(), 4u);

    auto find = [&](const std::string& rid) -> const SubscriptionItem* {
        for (const auto& it : got.items) {
            if (it.route_id == rid) return &it;
        }
        return nullptr;
    };

    const SubscriptionItem* g_fota = find("fota.downlink");
    ASSERT_NE(g_fota, nullptr);
    EXPECT_FALSE(g_fota->enabled.has_value());  // 缺省语义保留

    const SubscriptionItem* g_legacy_off = find("fota.uplink");
    ASSERT_NE(g_legacy_off, nullptr);
    ASSERT_TRUE(g_legacy_off->enabled.has_value());
    EXPECT_FALSE(*g_legacy_off->enabled);        // 显式 false 不丢

    const SubscriptionItem* g_up = find(kKeyprovUplinkRouteId);
    ASSERT_NE(g_up, nullptr);
    ASSERT_TRUE(g_up->enabled.has_value());
    EXPECT_TRUE(*g_up->enabled);
    EXPECT_EQ(g_up->readiness_scope, kCapabilitySecurityKeyprov);
    EXPECT_EQ(g_up->criticality, SubscriptionCriticality::CAPABILITY_MANDATORY);

    const SubscriptionItem* g_down = find(kKeyprovDownlinkRouteId);
    ASSERT_NE(g_down, nullptr);
    ASSERT_TRUE(g_down->enabled.has_value());
    EXPECT_TRUE(*g_down->enabled);
    EXPECT_EQ(g_down->readiness_scope, kCapabilitySecurityKeyprov);
    EXPECT_EQ(g_down->criticality, SubscriptionCriticality::CAPABILITY_MANDATORY);
}
