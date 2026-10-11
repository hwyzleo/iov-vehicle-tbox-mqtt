// CR-007 §13: OwnerSnapshotRegistry 单元测试
#include <gtest/gtest.h>
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

TEST(OwnerSnapshotRegistryTest, AcceptValidSnapshot) {
    OwnerSnapshotRegistry reg;
    auto r = reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    }));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r.accepted_generation, 1u);
    EXPECT_EQ(reg.ownerCount(), 1u);
}

TEST(OwnerSnapshotRegistryTest, IdempotentSameGenerationDigest) {
    OwnerSnapshotRegistry reg;
    auto snap = makeSnap("tsp", 1, "d1", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false)});
    reg.replaceSnapshot(snap);
    auto r = reg.replaceSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r.reason_code, "idempotent");
}

TEST(OwnerSnapshotRegistryTest, RejectDigestConflict) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false)}));
    auto r = reg.replaceSnapshot(makeSnap("tsp", 1, "d2", true,
        {makeItem("a", "t2", SnapshotDirection::UP, 1, "tgt", false)}));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::REJECTED);
    EXPECT_EQ(r.reason_code, "digest_conflict");
}

TEST(OwnerSnapshotRegistryTest, StaleGenerationNotOverwrite) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 5, "d5", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false)}));
    auto r = reg.replaceSnapshot(makeSnap("tsp", 3, "d3", true,
        {makeItem("a", "t3", SnapshotDirection::UP, 1, "tgt", false)}));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r.accepted_generation, 5u);
    EXPECT_EQ(r.reason_code, "stale_generation");
    auto snap = reg.getLatestSnapshot("tsp");
    ASSERT_TRUE(snap != nullptr);
    EXPECT_EQ(snap->generation, 5u);
}

TEST(OwnerSnapshotRegistryTest, HigherGenerationReplaces) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false)}));
    auto r = reg.replaceSnapshot(makeSnap("tsp", 2, "d2", true,
        {makeItem("b", "t2", SnapshotDirection::UP, 1, "tgt", false)}));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r.accepted_generation, 2u);
    auto snap = reg.getLatestSnapshot("tsp");
    ASSERT_TRUE(snap != nullptr);
    EXPECT_EQ(snap->items[0].route_id, "b");
}

TEST(OwnerSnapshotRegistryTest, RejectInvalidSchema) {
    OwnerSnapshotRegistry reg;
    EXPECT_EQ(reg.replaceSnapshot(makeSnap("", 1, "d", true, {})).reason_code, "invalid_owner");
    EXPECT_EQ(reg.replaceSnapshot(makeSnap("tsp", 0, "d", true, {})).reason_code, "invalid_generation");
    EXPECT_EQ(reg.replaceSnapshot(makeSnap("tsp", 1, "", true, {})).reason_code, "invalid_digest");
    EXPECT_EQ(reg.replaceSnapshot(makeSnap("tsp", 1, "d", true,
        {makeItem("a", "t", SnapshotDirection::UP, 5, "tgt", false)})).reason_code, "invalid_qos");
    EXPECT_EQ(reg.replaceSnapshot(makeSnap("tsp", 1, "d", true,
        {makeItem("", "t", SnapshotDirection::UP, 1, "tgt", false)})).reason_code, "invalid_route_id");
    EXPECT_EQ(reg.replaceSnapshot(makeSnap("tsp", 1, "d", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "", false)})).reason_code, "invalid_target");
    EXPECT_EQ(reg.replaceSnapshot(makeSnap("tsp", 1, "d", true, {
        makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false),
        makeItem("a", "t2", SnapshotDirection::UP, 1, "tgt", false)})).reason_code, "duplicate_route_id");
}

TEST(OwnerSnapshotRegistryTest, MultipleOwners) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 1, "dt", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false)}));
    reg.replaceSnapshot(makeSnap("diag", 1, "dd", true,
        {makeItem("x", "t", SnapshotDirection::UP, 1, "tgt", false)}));
    EXPECT_EQ(reg.ownerCount(), 2u);
    EXPECT_TRUE(reg.hasRegistrationComplete("tsp"));
    EXPECT_TRUE(reg.hasRegistrationComplete("diag"));
    EXPECT_EQ(reg.getAllLatestSnapshots().size(), 2u);
}

TEST(OwnerSnapshotRegistryTest, QueryStatus) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false)}));
    auto st = reg.getSnapshotStatus("tsp", 1);
    EXPECT_EQ(st.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(st.generation, 1u);
    EXPECT_EQ(st.content_digest, "d1");
    EXPECT_TRUE(st.registration_complete);
    EXPECT_EQ(reg.getSnapshotStatus("none", 1).status, SnapshotAcceptStatus::UNKNOWN);
}

TEST(OwnerSnapshotRegistryTest, Clear) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true,
        {makeItem("a", "t", SnapshotDirection::UP, 1, "tgt", false)}));
    reg.clear();
    EXPECT_EQ(reg.ownerCount(), 0u);
    EXPECT_FALSE(reg.hasRegistrationComplete("tsp"));
}

// ============================================================
// CR-015: catalog-governed route / enabled / contract 语义
// ============================================================

namespace {
SubscriptionItem makeCatalogItem(const std::string& route_id) {
    SubscriptionItem it;  // 只引用 route_id：模板/方向/QoS/target 留空
    it.route_id = route_id;
    return it;
}
}

TEST(OwnerSnapshotRegistryTest, CatalogKeyprovFieldsFrozen) {
    OwnerSnapshotRegistry reg;
    auto snap = makeSnap("tsp", 1, "d1", true,
        {makeCatalogItem(kKeyprovDownlinkRouteId)});
    snap.contract_version = kSubscriptionCatalogContractVersion;
    auto r = reg.replaceSnapshot(snap);
    ASSERT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);

    auto stored = reg.getLatestSnapshot("tsp");
    ASSERT_TRUE(stored != nullptr);
    ASSERT_EQ(stored->items.size(), 1u);
    const auto& item = stored->items[0];
    EXPECT_EQ(item.topic_template, "vehicle/{ecu_uid}/down/keyprov");
    EXPECT_EQ(item.direction, SnapshotDirection::DOWN);
    EXPECT_EQ(item.qos, 1u);
    EXPECT_EQ(item.target, "tsp.keyprov");
    EXPECT_EQ(item.readiness_scope, kCapabilitySecurityKeyprov);
    EXPECT_EQ(item.criticality, SubscriptionCriticality::CAPABILITY_MANDATORY);
    // capability-gated route 未显式 enabled -> false
    ASSERT_TRUE(item.enabled.has_value());
    EXPECT_FALSE(item.enabled.value());
}

TEST(OwnerSnapshotRegistryTest, CatalogUplinkEmptyTargetAccepted) {
    OwnerSnapshotRegistry reg;
    auto snap = makeSnap("tsp", 1, "d1", true,
        {makeCatalogItem(kKeyprovUplinkRouteId)});
    snap.contract_version = kSubscriptionCatalogContractVersion;
    auto r = reg.replaceSnapshot(snap);
    ASSERT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    auto stored = reg.getLatestSnapshot("tsp");
    EXPECT_EQ(stored->items[0].target, "");
    EXPECT_EQ(stored->items[0].direction, SnapshotDirection::UP);
}

TEST(OwnerSnapshotRegistryTest, CatalogRouteRejectsArbitraryTemplate) {
    OwnerSnapshotRegistry reg;
    SubscriptionItem it = makeCatalogItem(kKeyprovDownlinkRouteId);
    it.topic_template = "vehicle/{ecu_uid}/down/evil";
    auto snap = makeSnap("tsp", 1, "d1", true, {it});
    snap.contract_version = kSubscriptionCatalogContractVersion;
    auto r = reg.replaceSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::REJECTED);
    EXPECT_EQ(r.reason_code, "catalog_template_mismatch");
}

TEST(OwnerSnapshotRegistryTest, CatalogRouteRejectsScopeOverride) {
    OwnerSnapshotRegistry reg;
    SubscriptionItem it = makeCatalogItem(kKeyprovDownlinkRouteId);
    it.readiness_scope = "security.other";
    auto snap = makeSnap("tsp", 1, "d1", true, {it});
    auto r = reg.replaceSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::REJECTED);
    EXPECT_EQ(r.reason_code, "catalog_scope_mismatch");
}

TEST(OwnerSnapshotRegistryTest, CatalogEnabledExplicitTrue) {
    OwnerSnapshotRegistry reg;
    SubscriptionItem it = makeCatalogItem(kKeyprovDownlinkRouteId);
    it.enabled = true;
    auto snap = makeSnap("tsp", 1, "d1", true, {it});
    snap.contract_version = kSubscriptionCatalogContractVersion;
    ASSERT_EQ(reg.replaceSnapshot(snap).status, SnapshotAcceptStatus::ACCEPTED);
    auto stored = reg.getLatestSnapshot("tsp");
    ASSERT_TRUE(stored->items[0].enabled.has_value());
    EXPECT_TRUE(stored->items[0].enabled.value());
    EXPECT_EQ(stored->contract_version, kSubscriptionCatalogContractVersion);
}

TEST(OwnerSnapshotRegistryTest, UnknownScopedRouteRejected) {
    OwnerSnapshotRegistry reg;
    SubscriptionItem it = makeItem("custom.route", "t", SnapshotDirection::UP, 1, "tgt", false);
    it.readiness_scope = "security.custom";
    auto r = reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {it}));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::REJECTED);
    EXPECT_EQ(r.reason_code, "unknown_capability_route");
}

TEST(OwnerSnapshotRegistryTest, LegacyEnabledDefaultsTrue) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true,
        {makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)}));
    auto stored = reg.getLatestSnapshot("tsp");
    ASSERT_TRUE(stored->items[0].enabled.has_value());
    EXPECT_TRUE(stored->items[0].enabled.value());
    EXPECT_EQ(stored->items[0].criticality, SubscriptionCriticality::GLOBAL_MANDATORY);
    EXPECT_TRUE(stored->items[0].readiness_scope.empty());
}
