// CR-009 §13.1: RoutePublisher 单元测试
#include <gtest/gtest.h>
#include "route_publisher.h"
#include "effective_route_table.h"
#include "topic_projection_builder.h"
#include "owner_snapshot_registry.h"
#include "topic_template_resolver.h"

using namespace tbox::mqtt;

namespace {
SubscriptionItem makeItem(const std::string& id, const std::string& tmpl,
                          SnapshotDirection dir, uint8_t qos,
                          const std::string& target, bool mand) {
    SubscriptionItem it;
    it.route_id = id; it.topic_template = tmpl; it.direction = dir;
    it.qos = qos; it.target = target; it.mandatory = mand;
    return it;
}
OwnerSubscriptionSnapshot makeSnap(const std::string& owner, uint64_t gen,
        const std::string& digest, bool complete, std::vector<SubscriptionItem> items) {
    OwnerSubscriptionSnapshot s;
    s.owner = owner; s.generation = gen; s.content_digest = digest;
    s.registration_complete = complete; s.items = std::move(items);
    return s;
}

struct RoutePublisherTestEnv {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    TopicProjectionBuilder builder;
    std::shared_ptr<const EffectiveRouteTable> table;
    RoutePublisher publisher;

    bool broker_called = false;
    std::string broker_topic;
    std::vector<uint8_t> broker_payload;
    QoS broker_qos = QoS::AT_MOST_ONCE;

    bool enqueue_called = false;
    OutboundMessage enqueued_msg;

    bool is_connected = false;

    RoutePublisherTestEnv() {
        publisher.setBrokerPublishFn(
            [this](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos) -> bool {
                broker_called = true;
                broker_topic = topic;
                broker_payload = payload;
                broker_qos = qos;
                return true;
            });
        publisher.setQueueEnqueueFn(
            [this](const OutboundMessage& msg) -> bool {
                enqueue_called = true;
                enqueued_msg = msg;
                return true;
            });
        publisher.setIsConnectedFn(
            [this]() -> bool { return is_connected; });
    }

    void setupRoutes(const std::string& ecu_uid, uint64_t gen) {
        resolver.setIdentity(ecu_uid, gen);
        reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
            makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true),
            makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
        }));
        table = builder.build(reg, resolver);
    }
};
}

TEST(RoutePublisherTest, PublishRouteOnlineSuccess) {
    RoutePublisherTestEnv env;
    env.setupRoutes("ECU123", 1);
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "msg_001";
    req.payload = {0x01, 0x02};

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_TRUE(result.accepted);
    EXPECT_TRUE(env.broker_called);
    EXPECT_EQ(env.broker_topic, "vehicle/ECU123/up/fota");
    EXPECT_EQ(env.broker_payload, (std::vector<uint8_t>{0x01, 0x02}));
}

TEST(RoutePublisherTest, PublishRouteOfflineEnqueue) {
    RoutePublisherTestEnv env;
    env.setupRoutes("ECU123", 1);
    env.is_connected = false;

    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "msg_002";
    req.payload = {0x03};

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_TRUE(result.accepted);
    EXPECT_FALSE(env.broker_called);
    EXPECT_TRUE(env.enqueue_called);
    EXPECT_EQ(env.enqueued_msg.route.owner, "tsp");
    EXPECT_EQ(env.enqueued_msg.route.route_id, "fota.up");
    EXPECT_TRUE(env.enqueued_msg.isRouteBased());
    EXPECT_EQ(env.enqueued_msg.identity_generation, 1u);
}

TEST(RoutePublisherTest, PublishRouteNotFound) {
    RoutePublisherTestEnv env;
    env.setupRoutes("ECU123", 1);
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", "nonexistent"};
    req.msg_id = "msg_003";

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1004);
}

TEST(RoutePublisherTest, PublishRouteWrongDirection) {
    RoutePublisherTestEnv env;
    env.setupRoutes("ECU123", 1);
    env.is_connected = true;

    // fota.down 是 DOWN 方向，不能用于上行发布
    RoutePublishRequest req;
    req.key = {"tsp", "fota.down"};
    req.msg_id = "msg_004";

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1004);
}

TEST(RoutePublisherTest, PublishRouteStaleGeneration) {
    RoutePublisherTestEnv env;
    env.setupRoutes("ECU123", 1);
    env.is_connected = true;

    // 身份变化后 resolver 的 generation 变了，但 table 还是旧的
    env.resolver.setIdentity("ECU456", 2);

    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "msg_005";

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1009);
}

TEST(RoutePublisherTest, PublishRouteQosOverride) {
    RoutePublisherTestEnv env;
    env.setupRoutes("ECU123", 1);
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "msg_006";
    req.qos = 0;  // 覆盖 route 定义的 QoS 1

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_TRUE(result.accepted);
    EXPECT_EQ(env.broker_qos, QoS::AT_MOST_ONCE);
}

TEST(RoutePublisherTest, PublishRouteEmptyMsgId) {
    RoutePublisherTestEnv env;
    env.setupRoutes("ECU123", 1);

    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "";  // 空 msg_id

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1004);
}

TEST(RoutePublisherTest, PublishRouteUnresolvedState) {
    RoutePublisherTestEnv env;
    // 不设置身份 -> 路由 UNRESOLVED
    env.reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    }));
    env.table = env.builder.build(env.reg, env.resolver);
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "msg_007";

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);

    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1009);
}

// ============================================================
// CR-015 §18.6: keyprov payload 上限、非持久化与默认关闭
// ============================================================

namespace {
void setupKeyprov(RoutePublisherTestEnv& env, bool enabled, uint32_t contract) {
    env.resolver.setIdentity("ECU123", 1);
    SubscriptionItem up;
    up.route_id = kKeyprovUplinkRouteId;
    up.enabled = enabled;
    auto snap = makeSnap("tsp", 1, "d1", true, {up});
    snap.contract_version = contract;
    env.reg.replaceSnapshot(snap);
    env.table = env.builder.build(env.reg, env.resolver);
}
}

TEST(RoutePublisherTest, KeyprovPayloadTooLargeRejected) {
    RoutePublisherTestEnv env;
    setupKeyprov(env, true, kSubscriptionCatalogContractVersion);
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", kKeyprovUplinkRouteId};
    req.msg_id = "kp_large";
    req.payload.assign(kKeyprovMaxPayloadBytes + 1, 0xAB);

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1004);
    EXPECT_EQ(result.reason_code, capability_reason::kKeyprovPayloadTooLarge);
    EXPECT_FALSE(result.retryable);
    EXPECT_FALSE(env.broker_called);
}

TEST(RoutePublisherTest, KeyprovPayloadAtLimitAccepted) {
    RoutePublisherTestEnv env;
    setupKeyprov(env, true, kSubscriptionCatalogContractVersion);
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", kKeyprovUplinkRouteId};
    req.msg_id = "kp_limit";
    req.payload.assign(kKeyprovMaxPayloadBytes, 0x01);

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);
    EXPECT_TRUE(result.accepted);
    EXPECT_TRUE(env.broker_called);
    EXPECT_EQ(env.broker_topic, "vehicle/ECU123/up/keyprov");
}

TEST(RoutePublisherTest, KeyprovOfflineNotPersisted) {
    RoutePublisherTestEnv env;
    setupKeyprov(env, true, kSubscriptionCatalogContractVersion);
    env.is_connected = false;

    RoutePublishRequest req;
    req.key = {"tsp", kKeyprovUplinkRouteId};
    req.msg_id = "kp_offline";
    req.payload = {0x01};

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);
    EXPECT_FALSE(result.accepted);
    EXPECT_FALSE(env.enqueue_called);  // 不写离线队列
    EXPECT_EQ(result.reason_code, capability_reason::kKeyprovSubscribePending);
}

TEST(RoutePublisherTest, KeyprovDisabledReason) {
    RoutePublisherTestEnv env;
    setupKeyprov(env, false, kSubscriptionCatalogContractVersion);  // enabled=false -> 未投影
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", kKeyprovUplinkRouteId};
    req.msg_id = "kp_disabled";

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.reason_code, capability_reason::kKeyprovDisabled);
}

TEST(RoutePublisherTest, KeyprovContractUnsupportedNotProjected) {
    RoutePublisherTestEnv env;
    setupKeyprov(env, true, 0);  // 契约不支持 -> 未投影
    env.is_connected = true;

    RoutePublishRequest req;
    req.key = {"tsp", kKeyprovUplinkRouteId};
    req.msg_id = "kp_contract";

    auto result = env.publisher.publishRoute(req, env.table, env.resolver);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.reason_code, capability_reason::kKeyprovDisabled);
}
