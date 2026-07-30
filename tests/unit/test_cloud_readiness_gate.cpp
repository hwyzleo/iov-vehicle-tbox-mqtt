// CR-007 §13: CloudReadinessGate 单元测试
#include <gtest/gtest.h>
#include "cloud_readiness_gate.h"

using namespace tbox::mqtt;

TEST(CloudReadinessGateTest, NotReadyInitially) {
    CloudReadinessGate gate;
    EXPECT_FALSE(gate.isCloudReady());
    auto r = gate.getReadiness();
    EXPECT_FALSE(r.cloud_ready);
}

TEST(CloudReadinessGateTest, ReadyWhenConnectedAndSubscriptionsReady) {
    CloudReadinessGate gate;
    int call_count = 0;
    bool last_ready = false;
    gate.setCallback([&](bool ready, uint32_t, uint32_t, const std::string&) {
        ++call_count; last_ready = ready;
    });
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 2, 2);
    EXPECT_TRUE(gate.isCloudReady());
    EXPECT_EQ(call_count, 1);
    EXPECT_TRUE(last_ready);
}

TEST(CloudReadinessGateTest, NotReadyWhenSubscriptionsNotReady) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(false, 1, 2);
    EXPECT_FALSE(gate.isCloudReady());
}

TEST(CloudReadinessGateTest, LoseReadyOnDisconnect) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 2, 2);
    EXPECT_TRUE(gate.isCloudReady());
    gate.onConnectionState(ConnectionState::RECONNECTING, 1);
    EXPECT_FALSE(gate.isCloudReady());
}

TEST(CloudReadinessGateTest, DegradedBlocksReady) {
    CloudReadinessGate gate;
    gate.setDegraded(true, "gate_timeout");
    gate.onConnectionState(ConnectionState::CONNECTED_DEGRADED, 1);
    gate.onSubscriptionsReady(true, 2, 2);
    EXPECT_FALSE(gate.isCloudReady());
}

TEST(CloudReadinessGateTest, MandatoryFailedLosesReady) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::SUBSCRIBING, 1);
    gate.onSubscriptionsReady(true, 2, 2);
    EXPECT_TRUE(gate.isCloudReady());
    gate.onMandatoryFailed("suback_failed");
    EXPECT_FALSE(gate.isCloudReady());
}

TEST(CloudReadinessGateTest, ReadinessSnapshotFields) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CLOUD_READY, 7);
    std::map<std::string, uint64_t> gens = {{"tsp", 3}};
    gate.setOwnerGenerations(gens);
    gate.onSubscriptionsReady(true, 2, 2);
    auto r = gate.getReadiness();
    EXPECT_TRUE(r.cloud_ready);
    EXPECT_TRUE(r.connected);
    EXPECT_EQ(r.connection_generation, 7u);
    EXPECT_EQ(r.owner_generations["tsp"], 3u);
    EXPECT_EQ(r.mandatory_count, 2u);
    EXPECT_EQ(r.mandatory_active, 2u);
}
