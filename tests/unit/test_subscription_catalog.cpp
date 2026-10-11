// CR-015 §18.2: SubscriptionCatalog 单元测试
#include <gtest/gtest.h>
#include "subscription_catalog.h"

using namespace tbox::mqtt;

TEST(SubscriptionCatalogTest, KeyprovUplinkFrozen) {
    const auto& cat = SubscriptionCatalog::instance();
    const CatalogEntry* e = cat.lookup("tsp", kKeyprovUplinkRouteId);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->owner, "tsp");
    EXPECT_EQ(e->direction, SnapshotDirection::UP);
    EXPECT_EQ(e->topic_template, "vehicle/{ecu_uid}/up/keyprov");
    EXPECT_EQ(e->qos, 1);
    EXPECT_TRUE(e->target.empty());
    EXPECT_EQ(e->readiness_scope, kCapabilitySecurityKeyprov);
    EXPECT_EQ(e->criticality, SubscriptionCriticality::CAPABILITY_MANDATORY);
    EXPECT_FALSE(e->persist_offline);  // keyprov 上行不持久化
    EXPECT_TRUE(e->capabilityGated());
}

TEST(SubscriptionCatalogTest, KeyprovDownlinkFrozen) {
    const auto& cat = SubscriptionCatalog::instance();
    const CatalogEntry* e = cat.lookup("tsp", kKeyprovDownlinkRouteId);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->direction, SnapshotDirection::DOWN);
    EXPECT_EQ(e->topic_template, "vehicle/{ecu_uid}/down/keyprov");
    EXPECT_EQ(e->qos, 1);
    EXPECT_EQ(e->target, "tsp.keyprov");
    EXPECT_EQ(e->readiness_scope, kCapabilitySecurityKeyprov);
    EXPECT_EQ(e->criticality, SubscriptionCriticality::CAPABILITY_MANDATORY);
    EXPECT_FALSE(e->persist_offline);
}

TEST(SubscriptionCatalogTest, UnknownRouteNotCatalogGoverned) {
    const auto& cat = SubscriptionCatalog::instance();
    EXPECT_FALSE(cat.isCatalogRoute("tsp", "fota.downlink"));
    EXPECT_EQ(cat.lookup("tsp", "fota.downlink"), nullptr);
    EXPECT_EQ(cat.lookup("diag", kKeyprovUplinkRouteId), nullptr);
}

TEST(SubscriptionCatalogTest, CapabilityForScope) {
    EXPECT_EQ(SubscriptionCatalog::capabilityForScope(kCapabilitySecurityKeyprov),
              kCapabilitySecurityKeyprov);
}
