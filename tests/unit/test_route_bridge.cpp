#include <gtest/gtest.h>
#include "route_bridge.h"

using namespace tbox::mqtt;

TEST(RouteBridgeTest, RegisterRoute) {
    RouteBridge bridge;

    RouteEntry entry;
    entry.internal_addr = "service_diag";
    entry.mqtt_topic = "vehicle/diag/status";
    entry.direction = RouteEntry::Direction::UP;
    entry.qos = QoS::AT_LEAST_ONCE;

    EXPECT_TRUE(bridge.registerRoute(entry));
}

TEST(RouteBridgeTest, FindRouteByInternalAddr) {
    RouteBridge bridge;

    RouteEntry entry;
    entry.internal_addr = "service_diag";
    entry.mqtt_topic = "vehicle/diag/status";
    entry.direction = RouteEntry::Direction::UP;

    bridge.registerRoute(entry);

    auto found = bridge.findRouteByInternalAddr("service_diag");
    EXPECT_TRUE(found.has_value());
    EXPECT_EQ(found->mqtt_topic, "vehicle/diag/status");
}

TEST(RouteBridgeTest, FindRouteByMqttTopic) {
    RouteBridge bridge;

    RouteEntry entry;
    entry.internal_addr = "service_diag";
    entry.mqtt_topic = "vehicle/diag/status";
    entry.direction = RouteEntry::Direction::DOWN;

    bridge.registerRoute(entry);

    auto found = bridge.findRouteByMqttTopic("vehicle/diag/status");
    EXPECT_TRUE(found.has_value());
    EXPECT_EQ(found->internal_addr, "service_diag");
}

TEST(RouteBridgeTest, GetUpstreamRoutes) {
    RouteBridge bridge;

    RouteEntry up1;
    up1.internal_addr = "service_diag";
    up1.mqtt_topic = "vehicle/diag/status";
    up1.direction = RouteEntry::Direction::UP;

    RouteEntry down1;
    down1.internal_addr = "service_control";
    down1.mqtt_topic = "vehicle/control/cmd";
    down1.direction = RouteEntry::Direction::DOWN;

    bridge.registerRoute(up1);
    bridge.registerRoute(down1);

    auto upstream = bridge.getUpstreamRoutes();
    EXPECT_EQ(upstream.size(), 1);
    EXPECT_EQ(upstream[0].internal_addr, "service_diag");
}

TEST(RouteBridgeTest, GetDownstreamRoutes) {
    RouteBridge bridge;

    RouteEntry up1;
    up1.internal_addr = "service_diag";
    up1.mqtt_topic = "vehicle/diag/status";
    up1.direction = RouteEntry::Direction::UP;

    RouteEntry down1;
    down1.internal_addr = "service_control";
    down1.mqtt_topic = "vehicle/control/cmd";
    down1.direction = RouteEntry::Direction::DOWN;

    bridge.registerRoute(up1);
    bridge.registerRoute(down1);

    auto downstream = bridge.getDownstreamRoutes();
    EXPECT_EQ(downstream.size(), 1);
    EXPECT_EQ(downstream[0].mqtt_topic, "vehicle/control/cmd");
}

TEST(RouteBridgeTest, RemoveRoute) {
    RouteBridge bridge;

    RouteEntry entry;
    entry.internal_addr = "service_diag";
    entry.mqtt_topic = "vehicle/diag/status";
    entry.direction = RouteEntry::Direction::UP;

    bridge.registerRoute(entry);
    EXPECT_TRUE(bridge.removeRoute("service_diag"));

    auto found = bridge.findRouteByInternalAddr("service_diag");
    EXPECT_FALSE(found.has_value());
}
