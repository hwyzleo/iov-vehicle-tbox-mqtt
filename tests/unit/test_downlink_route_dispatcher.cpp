// CR-009 §13.1: DownlinkRouteDispatcher + LegacyTopicGuard 单元测试
#include <gtest/gtest.h>
#include "downlink_route_dispatcher.h"
#include "legacy_topic_guard.h"
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
}

// ---- DownlinkRouteDispatcher 测试 ----

TEST(DownlinkRouteDispatcherTest, DispatchSingleOwner) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    }));
    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    std::vector<RoutedDownlinkEnvelope> events;
    DownlinkRouteDispatcher dispatcher;
    dispatcher.setDispatchFn([&events](const RoutedDownlinkEnvelope& e) {
        events.push_back(e);
    });

    size_t matched = dispatcher.dispatch("vehicle/ECU123/down/fota", {0x01, 0x02}, table, 1);

    EXPECT_EQ(matched, 1u);
    ASSERT_EQ(events.size(), 1u);
    EXPECT_EQ(events[0].key.owner, "tsp");
    EXPECT_EQ(events[0].key.route_id, "fota.down");
    EXPECT_EQ(events[0].target, "tsp.fota");
    EXPECT_EQ(events[0].payload, (std::vector<uint8_t>{0x01, 0x02}));
}

TEST(DownlinkRouteDispatcherTest, DispatchMultiOwner) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("shared", "vehicle/{ecu_uid}/down/shared", SnapshotDirection::DOWN, 1, "tsp.h", false)
    }));
    reg.replaceSnapshot(makeSnap("diag", 1, "d2", true, {
        makeItem("shared", "vehicle/{ecu_uid}/down/shared", SnapshotDirection::DOWN, 2, "diag.h", false)
    }));
    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    std::vector<RoutedDownlinkEnvelope> events;
    DownlinkRouteDispatcher dispatcher;
    dispatcher.setDispatchFn([&events](const RoutedDownlinkEnvelope& e) {
        events.push_back(e);
    });

    size_t matched = dispatcher.dispatch("vehicle/ECU123/down/shared", {0x00}, table, 1);

    EXPECT_EQ(matched, 2u);
    EXPECT_EQ(events.size(), 2u);
}

TEST(DownlinkRouteDispatcherTest, DispatchNoMatch) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    }));
    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    DownlinkRouteDispatcher dispatcher;
    dispatcher.setDispatchFn([](const RoutedDownlinkEnvelope&) {});

    size_t matched = dispatcher.dispatch("vehicle/ECU123/down/nonexistent", {}, table, 1);
    EXPECT_EQ(matched, 0u);
}

TEST(DownlinkRouteDispatcherTest, DispatchStaleGeneration) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    }));
    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    int call_count = 0;
    DownlinkRouteDispatcher dispatcher;
    dispatcher.setDispatchFn([&call_count](const RoutedDownlinkEnvelope&) {
        ++call_count;
    });

    // 当前身份 generation 是 2，但 table 是 generation 1 的 -> 旧 callback 隔离
    size_t matched = dispatcher.dispatch("vehicle/ECU123/down/fota", {}, table, 2);
    EXPECT_EQ(matched, 0u);
    EXPECT_EQ(call_count, 0);
}

TEST(DownlinkRouteDispatcherTest, DispatchNullTable) {
    DownlinkRouteDispatcher dispatcher;
    dispatcher.setDispatchFn([](const RoutedDownlinkEnvelope&) {});

    size_t matched = dispatcher.dispatch("any/topic", {}, nullptr, 1);
    EXPECT_EQ(matched, 0u);
}

// ---- CR-015 §18.6: capability 下行 payload 上限（DOWNLINK_VALIDATE）----

TEST(DownlinkRouteDispatcherTest, KeyprovDownlinkWithinLimitDispatched) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);
    SubscriptionItem it;
    it.route_id = kKeyprovDownlinkRouteId;
    it.enabled = true;
    auto snap = makeSnap("tsp", 1, "d1", true, {it});
    snap.contract_version = kSubscriptionCatalogContractVersion;
    reg.replaceSnapshot(snap);
    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    std::vector<RoutedDownlinkEnvelope> events;
    DownlinkRouteDispatcher dispatcher;
    dispatcher.setDispatchFn([&events](const RoutedDownlinkEnvelope& e) {
        events.push_back(e);
    });

    std::vector<uint8_t> payload(kKeyprovMaxPayloadBytes, 0x01);
    size_t matched = dispatcher.dispatch(
        "vehicle/ECU123/down/keyprov", payload, table, 1);
    EXPECT_EQ(matched, 1u);
    ASSERT_EQ(events.size(), 1u);
    EXPECT_EQ(events[0].target, "tsp.keyprov");
    EXPECT_EQ(events[0].readiness_scope, kCapabilitySecurityKeyprov);
}

TEST(DownlinkRouteDispatcherTest, KeyprovDownlinkTooLargeRejected) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);
    SubscriptionItem it;
    it.route_id = kKeyprovDownlinkRouteId;
    it.enabled = true;
    auto snap = makeSnap("tsp", 1, "d1", true, {it});
    snap.contract_version = kSubscriptionCatalogContractVersion;
    reg.replaceSnapshot(snap);
    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    int call_count = 0;
    DownlinkRouteDispatcher dispatcher;
    dispatcher.setDispatchFn([&call_count](const RoutedDownlinkEnvelope&) {
        ++call_count;
    });

    std::vector<uint8_t> payload(kKeyprovMaxPayloadBytes + 1, 0x01);
    size_t matched = dispatcher.dispatch(
        "vehicle/ECU123/down/keyprov", payload, table, 1);
    EXPECT_EQ(matched, 0u);
    EXPECT_EQ(call_count, 0);
}

// ---- LegacyTopicGuard 测试 ----

TEST(LegacyTopicGuardTest, ValidateValidDeviceTopic) {
    LegacyTopicGuard guard;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);

    EXPECT_TRUE(guard.validate("vehicle/ECU123/up/fota", resolver));
}

TEST(LegacyTopicGuardTest, RejectWrongDeviceUid) {
    LegacyTopicGuard guard;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);

    // 设备层是 ECU456，不匹配当前 ECU123
    EXPECT_FALSE(guard.validate("vehicle/ECU456/up/fota", resolver));
}

TEST(LegacyTopicGuardTest, RejectInvalidIdentity) {
    LegacyTopicGuard guard;
    TopicTemplateResolver resolver;
    // 不设置身份

    EXPECT_FALSE(guard.validate("vehicle/ECU123/up/fota", resolver));
}

TEST(LegacyTopicGuardTest, RejectEmptyIdentity) {
    LegacyTopicGuard guard;
    TopicTemplateResolver resolver;
    resolver.setIdentity("", 1);

    EXPECT_FALSE(guard.validate("vehicle/ECU123/up/fota", resolver));
}

TEST(LegacyTopicGuardTest, IsRouteApiEnabled) {
    LegacyTopicGuard guard;
    OwnerSnapshotRegistry reg;

    // 无快照 -> route API 未启用
    EXPECT_FALSE(guard.isRouteApiEnabled("tsp", reg));

    // 提交快照后 -> route API 已启用
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    }));
    EXPECT_TRUE(guard.isRouteApiEnabled("tsp", reg));
    EXPECT_FALSE(guard.isRouteApiEnabled("diag", reg));
}
