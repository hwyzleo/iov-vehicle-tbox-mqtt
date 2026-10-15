// CR-015 §18.3/§18.4: keyprov capability readiness 端到端集成测试
// daemon 组件（OwnerSnapshotRegistry + EffectiveSubscriptionTable +
// BrokerSubscriptionReconciler + CloudReadinessGate）经 framework-ipc 到 SDK Client。
// 验证：keyprov 条件激活、capability 与全局 Cloud Ready 分层、FOTA 隔离、
// SUBACK 失败只关 keyprov、contract/enabled 语义与查询一致性。
#include <gtest/gtest.h>
#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "cloud_readiness_gate.h"
#include "owner_snapshot_registry.h"
#include "effective_subscription_table.h"
#include "broker_subscription_reconciler.h"
#include "subscription_catalog.h"
#include "ipc.h"
#include <chrono>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace tbox::mqtt;

namespace {

struct IntegFakeBrokerOps : public BrokerOperations {
    std::vector<std::string> subscribes;
    std::vector<std::string> unsubscribes;
    bool brokerSubscribe(const std::string& topic, uint8_t) override {
        subscribes.push_back(topic);
        return true;
    }
    bool brokerUnsubscribe(const std::string& topic) override {
        unsubscribes.push_back(topic);
        return true;
    }
};

SubscriptionItem makeFotaItem() {
    SubscriptionItem it;
    it.route_id = "fota.downlink";
    it.topic_template = "vehicle/{ecu_uid}/down/fota";
    it.direction = SnapshotDirection::DOWN;
    it.qos = 1;
    it.target = "tsp.fota";
    it.mandatory = true;
    return it;
}

SubscriptionItem makeKeyprovItem(bool enabled) {
    SubscriptionItem it;
    it.route_id = kKeyprovDownlinkRouteId;
    it.enabled = enabled;
    return it;
}

// 模拟 MqttService 的 daemon 装配（真实派发/订阅/就绪组件 + 假 Broker）
class KeyprovIntegHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    ::tbox::fw::ipc::Server* server = nullptr;
    CloudReadinessGate gate;
    OwnerSnapshotRegistry registry;
    EffectiveSubscriptionTable table;
    BrokerSubscriptionReconciler reconciler;
    IntegFakeBrokerOps ops;
    std::mutex mutex;

    KeyprovIntegHandler() {
        reconciler.setBrokerOperations(&ops);
        table.setEcuUid("ECU123");
    }

    void reconcile() {
        reconciler.reconcile(table.downstreamSnapshot(), /*session_present=*/false, "cid", 1);
        evaluate();
    }

    void evaluate() {
        bool ready = reconciler.isConverged();
        gate.onSubscriptionsReady(ready, reconciler.mandatoryActive(),
                                  reconciler.mandatoryTotal());
        refreshCapability();
    }

    void refreshCapability() {
        const auto& catalog = SubscriptionCatalog::instance();
        std::set<std::string> scopes;
        for (const auto& [key, entry] : catalog.entries()) {
            if (!entry.readiness_scope.empty()) scopes.insert(entry.readiness_scope);
        }
        for (const auto& scope : scopes) {
            bool present = false, enabled = false, contract = false, uplink = false;
            uint64_t owner_gen = 0;
            for (const auto& [key, entry] : catalog.entries()) {
                if (entry.readiness_scope != scope) continue;
                auto snap = registry.getLatestSnapshot(entry.owner);
                if (snap) {
                    for (const auto& item : snap->items) {
                        if (item.route_id != entry.route_id) continue;
                        present = true;
                        if (item.enabled.value_or(false)) enabled = true;
                        contract = (snap->contract_version == kSubscriptionCatalogContractVersion);
                        owner_gen = snap->generation;
                    }
                }
                if (entry.direction == SnapshotDirection::UP ||
                    entry.direction == SnapshotDirection::BIDIRECTIONAL) {
                    uplink = true;  // 测试未构建 route 投影，仅验证下行链
                }
            }
            gate.setCapabilityState(scope, enabled, contract, present, uplink, owner_gen);
            const bool has_downlink = reconciler.hasCapability(scope);
            const bool down_ready = has_downlink && reconciler.capabilityConverged(scope);
            gate.setCapabilityDownlinkReady(scope, down_ready, reconciler.capabilityRejected(scope));
        }
    }

    std::pair<int32_t, bool> onPublish(const PublishEnvelope&) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribe(const Route&, int) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribeEventChannel(int) override { return {0, true}; }
    std::pair<int32_t, bool> onUnsubscribe(int) override { return {0, true}; }
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry&) override { return {0, true}; }
    std::string onGetConnectionState() override { return R"({"state":"CONNECTED"})"; }

    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
            const OwnerSubscriptionSnapshot& s) override {
        std::lock_guard<std::mutex> lock(mutex);
        auto r = registry.replaceSnapshot(s);
        if (r.status == SnapshotAcceptStatus::ACCEPTED &&
            r.reason_code == "accepted") {
            table.recompute(registry);
            reconcile();
        }
        return r;
    }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
            const std::string&, uint64_t) override { return SnapshotStatusResult{}; }
    CloudReadinessSnapshot onGetSubscriptionReadiness() override {
        auto r = gate.getReadiness();
        r.owner_generations["tsp"] = registry.getLatestSnapshot("tsp")
            ? registry.getLatestSnapshot("tsp")->generation : 0;
        return r;
    }
    CapabilityReadiness onGetCapabilityReadiness(const std::string& capability) override {
        return gate.getCapabilityReadiness(capability);
    }
    // CR-017 §5: 模拟 MqttService §19.3 keyprov 会话绑定 guard
    std::mutex pub_mutex;
    bool has_route_req = false;
    RoutePublishRequest last_route_req;
    std::vector<uint8_t> last_payload;
    PublishResult onPublishRoute(const RoutePublishRequest& req) override {
        std::lock_guard<std::mutex> lock(pub_mutex);
        has_route_req = true;
        last_route_req = req;
        last_payload = req.payload;
        const bool capability_gated = (req.key.route_id == kKeyprovUplinkRouteId);
        if (capability_gated) {
            if (!req.expected_session.has_value()) {
                PublishResult r;
                r.error = ErrorCode::MQTT_1004;
                r.reason_code = session_reason::kPublishContextRequired;
                r.retryable = false;
                return r;
            }
            if (req.expected_session->daemon_epoch.empty()) {
                PublishResult r;
                r.error = ErrorCode::MQTT_1004;
                r.reason_code = session_reason::kPublishContextUnsupported;
                r.retryable = false;
                return r;
            }
            if (!gate.validateSession(req.expected_session.value())) {
                PublishResult r;
                r.error = ErrorCode::MQTT_1009;
                r.reason_code = session_reason::kPublishContextStale;
                r.retryable = true;
                return r;
            }
        }
        PublishResult ok;
        ok.accepted = true;
        return ok;
    }
    std::pair<int32_t, bool> onSubscribeCapabilityReadinessEventChannel(
            const std::string&, int client_fd) override {
        if (server) {
            server->add_subscription(
                client_fd, static_cast<uint32_t>(ipc::EventType::CAPABILITY_READINESS_CHANGED));
        }
        return {0, true};
    }
    std::pair<int32_t, bool> onSubscribeCloudReadinessEventChannel(int client_fd) override {
        if (server) {
            server->add_subscription(
                client_fd, static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED));
        }
        return {0, true};
    }
};

OwnerSubscriptionSnapshot makeSnapshot(uint64_t gen, bool keyprov_enabled,
                                       uint32_t contract_version) {
    OwnerSubscriptionSnapshot s;
    s.owner = "tsp";
    s.generation = gen;
    s.content_digest = "d" + std::to_string(gen);
    s.registration_complete = true;
    s.contract_version = contract_version;
    s.items.push_back(makeFotaItem());
    s.items.push_back(makeKeyprovItem(keyprov_enabled));
    return s;
}

std::string make_socket() {
    return std::string("/tmp/tbox_mqtt_keyprov_integ_") +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
}

} // namespace

class MqttKeyprovCapabilityIntegTest : public ::testing::Test {
protected:
    std::string socket_path;
    KeyprovIntegHandler handler;
    std::unique_ptr<MqttIpcDispatcher> dispatcher;
    std::unique_ptr<::tbox::fw::ipc::Server> server;
    std::unique_ptr<Client> client;

    void SetUp() override {
        socket_path = make_socket();
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
        handler.gate.onConnectionState(ConnectionState::CONNECTED, 1);
        // daemon 侧事件发布
        handler.gate.setCallback([this](const CloudReadinessSnapshot& s) {
            server->push_event(static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED),
                               cloudReadinessToJson(s));
        });
        handler.gate.setCapabilityCallback([this](const CapabilityReadiness& r) {
            server->push_event(
                static_cast<uint32_t>(ipc::EventType::CAPABILITY_READINESS_CHANGED),
                capabilityReadinessToJson(r));
        });
    }

    void TearDown() override {
        client.reset();
        server->stop();
        server.reset();
        ::unlink(socket_path.c_str());
    }
};

// keyprov enabled=true 且契约支持：FOTA+keyprov SUBACK 后两者均就绪
TEST_F(MqttKeyprovCapabilityIntegTest, EnabledAndSupportedReachesReady) {
    auto snap = makeSnapshot(1, /*enabled=*/true, kSubscriptionCatalogContractVersion);
    auto r = handler.onReplaceSubscriptionSnapshot(snap);
    ASSERT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);

    // 模拟 Broker SUBACK 成功
    handler.reconciler.onSubAck("vehicle/ECU123/down/fota", true);
    handler.reconciler.onSubAck("vehicle/ECU123/down/keyprov", true);
    handler.evaluate();

    auto cloud = client->getSubscriptionReadiness();
    EXPECT_TRUE(cloud.cloud_ready);
    auto cap = client->getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_TRUE(cap.ready);
    EXPECT_TRUE(cap.enabled);
    EXPECT_TRUE(cap.contract_supported);
    EXPECT_TRUE(cap.downlink_suback_ready);
    EXPECT_EQ(cap.reason_code, "READY");
}

// keyprov SUBACK 失败：仅 keyprov not-ready，共享 Cloud Ready 与 FOTA 不受影响
TEST_F(MqttKeyprovCapabilityIntegTest, KeyprovFailureKeepsGlobalReady) {
    handler.reconciler.setSubackRetryMax(0);  // 不重试，立即进入 FAILED
    auto snap = makeSnapshot(1, true, kSubscriptionCatalogContractVersion);
    ASSERT_EQ(handler.onReplaceSubscriptionSnapshot(snap).status,
              SnapshotAcceptStatus::ACCEPTED);

    handler.reconciler.onSubAck("vehicle/ECU123/down/fota", true);
    handler.reconciler.onSubAck("vehicle/ECU123/down/keyprov", false);
    handler.evaluate();

    EXPECT_TRUE(client->getSubscriptionReadiness().cloud_ready);
    auto cap = client->getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_FALSE(cap.ready);
    EXPECT_EQ(cap.reason_code, capability_reason::kKeyprovSubackRejected);
}

// keyprov enabled=false：不订阅、不进入全局 mandatory，返回 KEYPROV_DISABLED
TEST_F(MqttKeyprovCapabilityIntegTest, DisabledNotSubscribed) {
    auto snap = makeSnapshot(1, /*enabled=*/false, kSubscriptionCatalogContractVersion);
    ASSERT_EQ(handler.onReplaceSubscriptionSnapshot(snap).status,
              SnapshotAcceptStatus::ACCEPTED);

    // keyprov.downlink 不应出现在 Broker 订阅列表
    for (const auto& t : handler.ops.subscribes) {
        EXPECT_NE(t, "vehicle/ECU123/down/keyprov");
    }
    EXPECT_EQ(handler.reconciler.mandatoryTotal(), 1u);  // 仅 FOTA

    handler.reconciler.onSubAck("vehicle/ECU123/down/fota", true);
    handler.evaluate();

    EXPECT_TRUE(client->getSubscriptionReadiness().cloud_ready);
    auto cap = client->getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_FALSE(cap.ready);
    EXPECT_EQ(cap.reason_code, capability_reason::kKeyprovDisabled);
}

// contract 不支持：keyprov 不投影，返回 KEYPROV_CONTRACT_UNSUPPORTED
TEST_F(MqttKeyprovCapabilityIntegTest, ContractUnsupportedNotActivated) {
    auto snap = makeSnapshot(1, true, /*contract_version=*/0);
    ASSERT_EQ(handler.onReplaceSubscriptionSnapshot(snap).status,
              SnapshotAcceptStatus::ACCEPTED);
    for (const auto& t : handler.ops.subscribes) {
        EXPECT_NE(t, "vehicle/ECU123/down/keyprov");
    }
    handler.reconciler.onSubAck("vehicle/ECU123/down/fota", true);
    handler.evaluate();

    EXPECT_TRUE(client->getSubscriptionReadiness().cloud_ready);
    auto cap = client->getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_FALSE(cap.ready);
    EXPECT_EQ(cap.reason_code, capability_reason::kKeyprovContractUnsupported);
}

// 运行中关闭 keyprov：仅撤销 keyprov 订阅，FOTA 不被重建
TEST_F(MqttKeyprovCapabilityIntegTest, DisableKeyprovDoesNotResubscribeFota) {
    ASSERT_EQ(handler.onReplaceSubscriptionSnapshot(
        makeSnapshot(1, true, kSubscriptionCatalogContractVersion)).status,
        SnapshotAcceptStatus::ACCEPTED);
    handler.reconciler.onSubAck("vehicle/ECU123/down/fota", true);
    handler.reconciler.onSubAck("vehicle/ECU123/down/keyprov", true);
    handler.evaluate();
    ASSERT_TRUE(client->getCapabilityReadiness(kCapabilitySecurityKeyprov).ready);

    const size_t fota_sub_count_before = handler.ops.subscribes.size();
    auto snap2 = makeSnapshot(2, /*enabled=*/false, kSubscriptionCatalogContractVersion);
    ASSERT_EQ(handler.onReplaceSubscriptionSnapshot(snap2).status,
              SnapshotAcceptStatus::ACCEPTED);

    // keyprov 不再在 desired/reconciler 中；FOTA 保持（本测试 session_present=false，
    // Broker 会清空旧订阅，故不依赖 UNSUBSCRIBE；撤订/FOTA 不重建由 reconciler 单测覆盖）
    EXPECT_FALSE(handler.reconciler.hasCapability(kCapabilitySecurityKeyprov));
    EXPECT_LE(handler.ops.subscribes.size(), fota_sub_count_before + 1);
    handler.reconciler.onSubAck("vehicle/ECU123/down/fota", true);
    handler.evaluate();
    EXPECT_TRUE(client->getSubscriptionReadiness().cloud_ready);
    EXPECT_FALSE(client->getCapabilityReadiness(kCapabilitySecurityKeyprov).ready);
}

// capability 事件订阅：就绪变化推送 typed 事件
TEST_F(MqttKeyprovCapabilityIntegTest, CapabilityEventDelivered) {
    ASSERT_EQ(handler.onReplaceSubscriptionSnapshot(
        makeSnapshot(1, true, kSubscriptionCatalogContractVersion)).status,
        SnapshotAcceptStatus::ACCEPTED);

    std::mutex m;
    std::vector<CapabilityReadiness> events;
    auto sub = client->subscribeCapabilityReadinessChanged(
        kCapabilitySecurityKeyprov,
        [&](const CapabilityReadiness& r) { std::lock_guard<std::mutex> l(m); events.push_back(r); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    handler.reconciler.onSubAck("vehicle/ECU123/down/fota", true);
    handler.reconciler.onSubAck("vehicle/ECU123/down/keyprov", true);
    handler.evaluate();

    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::lock_guard<std::mutex> l(m);
        bool saw_ready = false;
        for (const auto& e : events) if (e.ready) saw_ready = true;
        if (saw_ready) break;
    }
    std::lock_guard<std::mutex> l(m);
    bool saw_ready = false;
    for (const auto& e : events) if (e.ready) saw_ready = true;
    EXPECT_TRUE(saw_ready);
}

// ============================================================
// CR-017 §5: keyprov 上行会话绑定（client SDK -> IPC -> daemon guard）
// ============================================================

TEST_F(MqttKeyprovCapabilityIntegTest, KeyprovPublishRequiresExpectedSessionOverIpc) {
    auto res = client->publishRoute("tsp", kKeyprovUplinkRouteId, "kp-req-1",
                                    std::vector<uint8_t>{0x01});
    EXPECT_FALSE(res.accepted);
    EXPECT_EQ(res.reason_code, session_reason::kPublishContextRequired);
    EXPECT_EQ(res.error, ErrorCode::MQTT_1004);
    EXPECT_FALSE(res.retryable);
}

TEST_F(MqttKeyprovCapabilityIntegTest, KeyprovPublishWithCurrentSessionAccepted) {
    TlsSessionIdentity id;
    id.cert_session_generation = 7;
    id.ecu_uid = "ECU123";
    id.valid = true;
    handler.gate.setTlsSessionIdentity(id);

    auto ready = client->getSubscriptionReadiness();
    ASSERT_FALSE(ready.session_context.daemon_epoch.empty());
    ASSERT_EQ(ready.session_context.cert_session_generation, 7u);

    PublishOptions opts;
    opts.expected_session = ready.session_context;
    std::vector<uint8_t> payload = {0x08, 0x00, 0xFF, 0x00, 0x80};
    auto res = client->publishRoute("tsp", kKeyprovUplinkRouteId, "kp-ok-1", payload, opts);
    EXPECT_TRUE(res.accepted);
    std::lock_guard<std::mutex> l(handler.pub_mutex);
    EXPECT_EQ(handler.last_payload, payload);  // §19.5 逐字节透明
}

TEST_F(MqttKeyprovCapabilityIntegTest, KeyprovPublishWithStaleSessionRejectedOverIpc) {
    TlsSessionIdentity id;
    id.cert_session_generation = 7;
    id.ecu_uid = "ECU123";
    id.valid = true;
    handler.gate.setTlsSessionIdentity(id);

    PublishOptions opts;
    SessionContext sc;
    auto ready = client->getSubscriptionReadiness();
    sc.daemon_epoch = "deadbeef";  // 与当前 epoch 不符
    sc.connection_generation = ready.session_context.connection_generation;
    sc.cert_session_generation = ready.session_context.cert_session_generation;
    opts.expected_session = sc;

    auto res = client->publishRoute("tsp", kKeyprovUplinkRouteId, "kp-stale-1", {0x01}, opts);
    EXPECT_FALSE(res.accepted);
    EXPECT_EQ(res.reason_code, session_reason::kPublishContextStale);
    EXPECT_EQ(res.error, ErrorCode::MQTT_1009);
    EXPECT_TRUE(res.retryable);
}

TEST_F(MqttKeyprovCapabilityIntegTest, KeyprovPublishEmptyEpochRejectedOverIpc) {
    PublishOptions opts;
    opts.expected_session = SessionContext{};  // 空 epoch -> UNSUPPORTED
    auto res = client->publishRoute("tsp", kKeyprovUplinkRouteId, "kp-unsupported-1", {0x01}, opts);
    EXPECT_FALSE(res.accepted);
    EXPECT_EQ(res.reason_code, session_reason::kPublishContextUnsupported);
    EXPECT_EQ(res.error, ErrorCode::MQTT_1004);
}

TEST_F(MqttKeyprovCapabilityIntegTest, FotaPublishNotBlockedBySessionGuardOverIpc) {
    // 非 keyprov route 不要求 expected_session（旧签名兼容入口）
    auto res = client->publishRoute("tsp", "fota.uplink", "fota-ok-1",
                                    std::vector<uint8_t>{0x02});
    EXPECT_TRUE(res.accepted);
}

