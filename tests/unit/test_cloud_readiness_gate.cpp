// CR-007 §13 / CR-014 §3, §4, §5: CloudReadinessGate 单元测试
#include <gtest/gtest.h>
#include "cloud_readiness_gate.h"

using namespace tbox::mqtt;

namespace {
TlsSessionIdentity makeIdentity(uint64_t cert_gen, const std::string& fp = "sha256:aa11bb22") {
    TlsSessionIdentity id;
    id.cert_session_generation = cert_gen;
    id.credential_id = "cred-1";
    id.credential_version = cert_gen;
    id.certificate_ref = fp;
    id.device_identity_digest = fp;
    id.valid = true;
    return id;
}
} // namespace

TEST(CloudReadinessGateTest, NotReadyInitially) {
    CloudReadinessGate gate;
    EXPECT_FALSE(gate.isCloudReady());
    auto r = gate.getReadiness();
    EXPECT_FALSE(r.cloud_ready);
    EXPECT_EQ(r.event_sequence, 0u);
}

TEST(CloudReadinessGateTest, ReadyWhenConnectedAndSubscriptionsReady) {
    CloudReadinessGate gate;
    int call_count = 0;
    bool last_ready = false;
    gate.setCallback([&](const CloudReadinessSnapshot& snap) {
        ++call_count; last_ready = snap.cloud_ready;
    });
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 2, 2);
    EXPECT_TRUE(gate.isCloudReady());
    // CR-014：快照在 connection 变化与 ready 变化时各提交一次事件
    EXPECT_EQ(call_count, 2);
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

// ---- CR-014 §3/§4: 快照与事件、TLS 会话身份绑定 ----

TEST(CloudReadinessGateTest, SnapshotCarriesTlsSessionIdentity) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 4);
    gate.setTlsSessionIdentity(makeIdentity(11, "sha256:deadbeef"));
    gate.onSubscriptionsReady(true, 1, 1);

    auto r = gate.getReadiness();
    EXPECT_TRUE(r.cloud_ready);
    EXPECT_EQ(r.connection_generation, 4u);
    EXPECT_EQ(r.cert_session_generation, 11u);
    EXPECT_EQ(r.credential_id, "cred-1");
    EXPECT_EQ(r.credential_version, 11u);
    EXPECT_EQ(r.certificate_ref, "sha256:deadbeef");
    EXPECT_EQ(r.device_identity_digest, "sha256:deadbeef");  // 初始同摘要
    EXPECT_EQ(r.contract_version, kCloudReadinessContractVersion);
    EXPECT_TRUE(r.contract_supported);
}

TEST(CloudReadinessGateTest, EventSequenceMonotonicAndQueryConsistent) {
    CloudReadinessGate gate;
    std::vector<CloudReadinessSnapshot> events;
    gate.setCallback([&](const CloudReadinessSnapshot& s) { events.push_back(s); });

    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.setTlsSessionIdentity(makeIdentity(1));
    gate.onSubscriptionsReady(true, 1, 1);

    ASSERT_FALSE(events.empty());
    uint64_t prev = 0;
    for (const auto& e : events) {
        EXPECT_GT(e.event_sequence, prev);
        prev = e.event_sequence;
    }
    // 查询与最后一次事件读同一份快照
    auto r = gate.getReadiness();
    EXPECT_EQ(r.event_sequence, events.back().event_sequence);
    EXPECT_EQ(r.certificate_ref, events.back().certificate_ref);
}

TEST(CloudReadinessGateTest, RevokeSessionCarriesOldIdentityAndFalse) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 2);
    gate.setTlsSessionIdentity(makeIdentity(5));
    gate.onSubscriptionsReady(true, 1, 1);
    ASSERT_TRUE(gate.isCloudReady());

    CloudReadinessSnapshot revoked;
    gate.setCallback([&](const CloudReadinessSnapshot& s) { revoked = s; });
    gate.revokeSession("credential_changed");

    EXPECT_FALSE(gate.isCloudReady());
    EXPECT_FALSE(revoked.cloud_ready);
    // cloud_ready=false 仍携带被撤销 generation 的旧身份 (CR-014 §4.2)
    EXPECT_EQ(revoked.cert_session_generation, 5u);
    EXPECT_EQ(revoked.certificate_ref, "sha256:aa11bb22");
    EXPECT_EQ(revoked.reason_code, "credential_changed");
}

TEST(CloudReadinessGateTest, NewSessionMustReconvergeBeforeReady) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.setTlsSessionIdentity(makeIdentity(1, "sha256:old"));
    gate.onSubscriptionsReady(true, 1, 1);
    ASSERT_TRUE(gate.isCloudReady());

    // 新 TLS 会话：先退出 ready，且不得复用旧 cert_session_generation
    gate.setTlsSessionIdentity(makeIdentity(2, "sha256:new"));
    EXPECT_FALSE(gate.isCloudReady());
    auto r = gate.getReadiness();
    EXPECT_EQ(r.cert_session_generation, 2u);
    EXPECT_EQ(r.certificate_ref, "sha256:new");

    // 新会话重新收敛后才恢复
    gate.onSubscriptionsReady(true, 1, 1);
    EXPECT_TRUE(gate.isCloudReady());
    EXPECT_EQ(gate.getReadiness().cert_session_generation, 2u);
}
