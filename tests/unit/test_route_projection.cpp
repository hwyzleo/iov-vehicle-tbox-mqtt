// CR-009 §13.1: EffectiveRouteTable + TopicProjectionBuilder 单元测试
#include <gtest/gtest.h>
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

std::shared_ptr<const EffectiveRouteTable> buildTable(
        const std::string& ecu_uid, uint64_t gen,
        const std::vector<SubscriptionItem>& items,
        const std::string& owner = "tsp") {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    if (!ecu_uid.empty()) resolver.setIdentity(ecu_uid, gen);
    OwnerSubscriptionSnapshot snap;
    snap.owner = owner; snap.generation = 1; snap.content_digest = "d1";
    snap.registration_complete = true; snap.items = items;
    reg.replaceSnapshot(snap);
    TopicProjectionBuilder builder;
    return builder.build(reg, resolver);
}
}

// ---- EffectiveRouteTable 基本测试（通过 builder 构建）----

TEST(EffectiveRouteTableTest, EmptyTable) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);
    EXPECT_TRUE(table->isEmpty());
    EXPECT_EQ(table->totalRoutes(), 0u);
    EXPECT_FALSE(table->lookupRoute({"tsp", "fota.up"}).has_value());
}

TEST(EffectiveRouteTableTest, LookupRoute) {
    auto table = buildTable("ECU123", 1, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    });

    auto found = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->expanded_topic, "vehicle/ECU123/up/fota");
    EXPECT_EQ(found->state, RouteState::ACTIVE);
}

TEST(EffectiveRouteTableTest, LookupRouteNotFound) {
    auto table = buildTable("ECU123", 1, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    });
    auto found = table->lookupRoute({"tsp", "nonexistent"});
    EXPECT_FALSE(found.has_value());
}

TEST(EffectiveRouteTableTest, LookupDownlinkSingleOwner) {
    auto table = buildTable("ECU123", 1, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    });

    auto refs = table->lookupDownlink("vehicle/ECU123/down/fota");
    ASSERT_EQ(refs.size(), 1u);
    EXPECT_EQ(refs[0].key.owner, "tsp");
    EXPECT_EQ(refs[0].key.route_id, "fota.down");
    EXPECT_EQ(refs[0].target, "tsp.fota");
}

TEST(EffectiveRouteTableTest, LookupDownlinkMultiOwner) {
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

    auto refs = table->lookupDownlink("vehicle/ECU123/down/shared");
    ASSERT_EQ(refs.size(), 2u);
}

TEST(EffectiveRouteTableTest, LookupDownlinkNoMatch) {
    auto table = buildTable("ECU123", 1, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    });
    auto refs = table->lookupDownlink("vehicle/ECU123/nonexistent");
    EXPECT_TRUE(refs.empty());
}

TEST(EffectiveRouteTableTest, UnresolvedRouteNotInReverseIndex) {
    // 身份无效 -> 路由 UNRESOLVED
    auto table = buildTable("", 0, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    });

    auto refs = table->lookupDownlink("");
    EXPECT_TRUE(refs.empty());
    auto found = table->lookupRoute({"tsp", "fota.down"});
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->state, RouteState::UNRESOLVED);
}

TEST(EffectiveRouteTableTest, UpstreamAndDownstreamRoutes) {
    auto table = buildTable("ECU123", 1, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true),
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true),
        makeItem("fota.bidi", "vehicle/{ecu_uid}/bidi/fota", SnapshotDirection::BIDIRECTIONAL, 1, "tsp.fota", true)
    });

    auto upstream = table->upstreamRoutes();
    EXPECT_EQ(upstream.size(), 2u);

    auto downstream = table->downstreamRoutes();
    EXPECT_EQ(downstream.size(), 2u);

    auto topics = table->downlinkTopics();
    EXPECT_EQ(topics.size(), 2u);
}

TEST(EffectiveRouteTableTest, StateCounts) {
    auto table_active = buildTable("ECU123", 1, {
        makeItem("a", "vehicle/{ecu_uid}/up/a", SnapshotDirection::UP, 1, "tsp.a", true),
        makeItem("b", "vehicle/{ecu_uid}/up/b", SnapshotDirection::UP, 1, "tsp.b", true)
    });
    EXPECT_EQ(table_active->totalRoutes(), 2u);
    EXPECT_EQ(table_active->activeRoutes(), 2u);
    EXPECT_EQ(table_active->unresolvedRoutes(), 0u);

    auto table_unresolved = buildTable("", 0, {
        makeItem("a", "vehicle/{ecu_uid}/up/a", SnapshotDirection::UP, 1, "tsp.a", true)
    });
    EXPECT_EQ(table_unresolved->unresolvedRoutes(), 1u);
    EXPECT_EQ(table_unresolved->activeRoutes(), 0u);
}

// ---- TopicProjectionBuilder 测试 ----

TEST(TopicProjectionBuilderTest, BuildWithValidIdentity) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);

    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true),
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    }));

    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    EXPECT_EQ(table->totalRoutes(), 2u);
    EXPECT_EQ(table->activeRoutes(), 2u);
    EXPECT_EQ(table->identityGeneration(), 1u);

    auto up = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(up.has_value());
    EXPECT_EQ(up->expanded_topic, "vehicle/ECU123/up/fota");
    EXPECT_EQ(up->state, RouteState::ACTIVE);

    auto down = table->lookupRoute({"tsp", "fota.down"});
    ASSERT_TRUE(down.has_value());
    EXPECT_EQ(down->expanded_topic, "vehicle/ECU123/down/fota");
}

TEST(TopicProjectionBuilderTest, BuildWithInvalidIdentityUnresolved) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;

    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    }));

    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    EXPECT_EQ(table->totalRoutes(), 1u);
    EXPECT_EQ(table->unresolvedRoutes(), 1u);
    EXPECT_EQ(table->activeRoutes(), 0u);

    auto route = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->state, RouteState::UNRESOLVED);
    EXPECT_TRUE(route->expanded_topic.empty());
}

TEST(TopicProjectionBuilderTest, DifferentIdentityGenerationsDifferentTopics) {
    OwnerSnapshotRegistry reg;
    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    }));

    TopicProjectionBuilder builder;

    TopicTemplateResolver r1;
    r1.setIdentity("ECU_OLD", 1);
    auto table1 = builder.build(reg, r1);
    auto route1 = table1->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(route1.has_value());
    EXPECT_EQ(route1->expanded_topic, "vehicle/ECU_OLD/up/fota");
    EXPECT_EQ(route1->identity_generation, 1u);

    TopicTemplateResolver r2;
    r2.setIdentity("ECU_NEW", 2);
    auto table2 = builder.build(reg, r2);
    auto route2 = table2->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(route2.has_value());
    EXPECT_EQ(route2->expanded_topic, "vehicle/ECU_NEW/up/fota");
    EXPECT_EQ(route2->identity_generation, 2u);

    EXPECT_NE(route1->expanded_topic, route2->expanded_topic);
}

TEST(TopicProjectionBuilderTest, InvalidTemplateMarkedInvalid) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU123", 1);

    std::map<std::string, OwnerSubscriptionSnapshot> snaps;
    snaps["tsp"] = makeSnap("tsp", 1, "d1", true, {
        makeItem("bad", "vehicle/{vin}/up", SnapshotDirection::UP, 1, "tsp.bad", false)
    });

    TopicProjectionBuilder builder;
    auto table = builder.build(snaps, resolver);

    auto route = table->lookupRoute({"tsp", "bad"});
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->state, RouteState::INVALID);
    EXPECT_TRUE(route->expanded_topic.empty());
}

TEST(TopicProjectionBuilderTest, MultiOwnerMultiRoute) {
    OwnerSnapshotRegistry reg;
    TopicTemplateResolver resolver;
    resolver.setIdentity("ECU", 1);

    reg.replaceSnapshot(makeSnap("tsp", 1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true),
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    }));
    reg.replaceSnapshot(makeSnap("diag", 1, "d2", true, {
        makeItem("diag.down", "vehicle/{ecu_uid}/down/diag", SnapshotDirection::DOWN, 1, "diag.handler", false)
    }));

    TopicProjectionBuilder builder;
    auto table = builder.build(reg, resolver);

    EXPECT_EQ(table->totalRoutes(), 3u);
    EXPECT_EQ(table->activeRoutes(), 3u);

    auto topics = table->downlinkTopics();
    EXPECT_EQ(topics.size(), 2u);
}
