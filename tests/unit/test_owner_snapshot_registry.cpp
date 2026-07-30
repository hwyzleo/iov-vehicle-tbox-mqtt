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
