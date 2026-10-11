// CR-007 §13: EffectiveSubscriptionTable 单元测试
#include <gtest/gtest.h>
#include "effective_subscription_table.h"
#include "owner_snapshot_registry.h"

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

TEST(EffectiveSubscriptionTableTest, ExpandEcuUidTopic) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU123");
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    }));
    table.recompute(reg);
    auto down = table.downstreamSnapshot();
    ASSERT_EQ(down->size(), 1u);
    EXPECT_EQ(down->begin()->second.expanded_topic, "vehicle/ECU123/down/fota");
}

TEST(EffectiveSubscriptionTableTest, MultiOwnerQosMaxAndMandatoryOr) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    // 两个 owner 同 topic，不同 qos/mandatory
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/x", SnapshotDirection::DOWN, 1, "tsp.x", false)
    }));
    reg.replaceSnapshot(makeSnap("diag", 1, "d2", true, {
        makeItem("b", "vehicle/{ecu_uid}/down/x", SnapshotDirection::DOWN, 2, "diag.x", true)
    }));
    table.recompute(reg);
    auto down = table.downstreamSnapshot();
    ASSERT_EQ(down->size(), 1u);
    const auto& eff = down->begin()->second;
    EXPECT_EQ(eff.effective_qos, 2u);  // max
    EXPECT_TRUE(eff.mandatory);         // OR
    EXPECT_EQ(eff.owners.size(), 2u);
    EXPECT_EQ(eff.targets.size(), 2u);
}

TEST(EffectiveSubscriptionTableTest, SingleOwnerDeleteLeadsToUnsubscribe) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/x", SnapshotDirection::DOWN, 1, "tsp.x", true),
        makeItem("b", "vehicle/{ecu_uid}/down/y", SnapshotDirection::DOWN, 1, "tsp.y", false)
    }));
    reg.replaceSnapshot(makeSnap("diag", 1, "d2", true, {
        makeItem("c", "vehicle/{ecu_uid}/down/y", SnapshotDirection::DOWN, 1, "diag.y", false)
    }));
    table.recompute(reg);
    EXPECT_EQ(table.downstreamSnapshot()->size(), 2u);

    // tsp 删除 b，diag 仍引用 y -> y 保留，仅移除 tsp 引用
    reg.replaceSnapshot(makeSnap("tsp", 2, "d3", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/x", SnapshotDirection::DOWN, 1, "tsp.x", true)
    }));
    table.recompute(reg);
    auto down = table.downstreamSnapshot();
    ASSERT_EQ(down->size(), 2u);  // x(tsp) + y(diag)
    // 最后一个引用消失：diag 也删 y
    reg.replaceSnapshot(makeSnap("diag", 2, "d4", true, {}));
    table.recompute(reg);
    EXPECT_EQ(table.downstreamSnapshot()->size(), 1u);  // 只剩 x
}

TEST(EffectiveSubscriptionTableTest, BidirectionalUpDown) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/bidi", SnapshotDirection::BIDIRECTIONAL, 1, "tsp.x", false)
    }));
    table.recompute(reg);
    EXPECT_EQ(table.downstreamSnapshot()->size(), 1u);  // 下行订阅
    EXPECT_EQ(table.upstreamRoutes().size(), 1u);        // 上行路由
}

TEST(EffectiveSubscriptionTableTest, UpOnlyNoDownstream) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/up/x", SnapshotDirection::UP, 1, "tsp.x", false)
    }));
    table.recompute(reg);
    EXPECT_EQ(table.downstreamSnapshot()->size(), 0u);  // 不产生下行订阅
    EXPECT_EQ(table.upstreamRoutes().size(), 1u);
    EXPECT_EQ(table.mandatoryCount(), 0u);
}

TEST(EffectiveSubscriptionTableTest, MandatoryCount) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/x", SnapshotDirection::DOWN, 1, "tsp.x", true),
        makeItem("b", "vehicle/{ecu_uid}/down/y", SnapshotDirection::DOWN, 1, "tsp.y", false)
    }));
    table.recompute(reg);
    EXPECT_EQ(table.mandatoryCount(), 1u);
}

// ============================================================
// CR-015: capability-gated route 与全局 mandatory 隔离
// ============================================================

namespace {
SubscriptionItem makeKeyprovDown(bool enabled) {
    SubscriptionItem it;
    it.route_id = kKeyprovDownlinkRouteId;
    it.enabled = enabled;
    return it;
}
}

TEST(EffectiveSubscriptionTableTest, CapabilityRouteNotInGlobalMandatory) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    auto snap = makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true),
        makeKeyprovDown(true)
    });
    snap.contract_version = kSubscriptionCatalogContractVersion;
    ASSERT_EQ(reg.replaceSnapshot(snap).status, SnapshotAcceptStatus::ACCEPTED);
    table.recompute(reg);
    EXPECT_EQ(table.downstreamSnapshot()->size(), 2u);  // FOTA + keyprov 均订阅
    EXPECT_EQ(table.mandatoryCount(), 1u);              // 仅 FOTA 计入全局
    EXPECT_EQ(table.capabilityCount(), 1u);             // keyprov 计入 capability
}

TEST(EffectiveSubscriptionTableTest, CapabilityRouteExcludedWhenContractUnsupported) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    auto snap = makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true),
        makeKeyprovDown(true)
    });
    snap.contract_version = 0;  // 旧/未知契约
    ASSERT_EQ(reg.replaceSnapshot(snap).status, SnapshotAcceptStatus::ACCEPTED);
    table.recompute(reg);
    EXPECT_EQ(table.downstreamSnapshot()->size(), 1u);  // 仅 FOTA
    EXPECT_EQ(table.capabilityCount(), 0u);
    EXPECT_EQ(table.mandatoryCount(), 1u);
}

TEST(EffectiveSubscriptionTableTest, DisabledCapabilityRouteNotProjected) {
    OwnerSnapshotRegistry reg;
    EffectiveSubscriptionTable table;
    table.setEcuUid("ECU");
    auto snap = makeSnap("tsp", 1, "d1", true, {
        makeKeyprovDown(false)
    });
    snap.contract_version = kSubscriptionCatalogContractVersion;
    ASSERT_EQ(reg.replaceSnapshot(snap).status, SnapshotAcceptStatus::ACCEPTED);
    table.recompute(reg);
    EXPECT_EQ(table.downstreamSnapshot()->size(), 0u);
    EXPECT_EQ(table.capabilityCount(), 0u);
}
