// CR-007 §13 / CR-014 §3, §4, §5: CloudReadinessGate 单元测试
#include <gtest/gtest.h>
#include "cloud_readiness_gate.h"

using namespace tbox::mqtt;

namespace {
TlsSessionIdentity makeIdentity(uint64_t cert_gen, const std::string& fp = "sha256:aa11bb22",
                               const std::string& uid = "HWYZ-ABC123") {
    TlsSessionIdentity id;
    id.cert_session_generation = cert_gen;
    id.credential_id = "cred-1";
    id.credential_version = cert_gen;
    id.certificate_ref = fp;
    id.device_identity_digest = fp;
    id.ecu_uid = uid;
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

// ============================================================
// CR-015 §18.3: capability-scoped readiness
// ============================================================

TEST(CloudReadinessGateTest, CapabilityReadyIndependentOfGlobal) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 1, 1);          // 全局 GLOBAL_MANDATORY 就绪
    gate.setCapabilityState(kCapabilitySecurityKeyprov,
                            /*enabled=*/true, /*contract=*/true,
                            /*snapshot=*/true, /*uplink=*/true, /*owner_gen=*/3);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, true);
    auto c = gate.getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_TRUE(c.ready);
    EXPECT_EQ(c.owner_generation, 3u);
    EXPECT_EQ(c.connection_generation, 1u);
    EXPECT_EQ(c.reason_code, "READY");
    EXPECT_TRUE(c.contract_supported);
}

TEST(CloudReadinessGateTest, CapabilityEnabledButNoSubackNotReady) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 1, 1);
    gate.setCapabilityState(kCapabilitySecurityKeyprov, true, true, true, true, 3);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, false);
    auto c = gate.getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_FALSE(c.ready);
    EXPECT_FALSE(c.downlink_suback_ready);
    EXPECT_EQ(c.reason_code, capability_reason::kKeyprovSubscribePending);
}

TEST(CloudReadinessGateTest, CapabilityDisabledReason) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 1, 1);
    gate.setCapabilityState(kCapabilitySecurityKeyprov, false, true, false, false, 0);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, false);
    auto c = gate.getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_FALSE(c.ready);
    EXPECT_EQ(c.reason_code, capability_reason::kKeyprovDisabled);
}

TEST(CloudReadinessGateTest, CapabilityContractUnsupportedReason) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 1, 1);
    gate.setCapabilityState(kCapabilitySecurityKeyprov, true, false, true, true, 5);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, true);
    auto c = gate.getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_FALSE(c.ready);
    EXPECT_EQ(c.reason_code, capability_reason::kKeyprovContractUnsupported);
}

TEST(CloudReadinessGateTest, KeyprovFailureDoesNotDropGlobalReady) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 1, 1);
    gate.setCapabilityState(kCapabilitySecurityKeyprov, true, true, true, true, 2);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, false);
    // 全局 Cloud Ready 不受 keyprov capability 失败影响 (DSN-CR-015 §18.3)
    EXPECT_TRUE(gate.isCloudReady());
    EXPECT_FALSE(gate.getCapabilityReadiness(kCapabilitySecurityKeyprov).ready);
}

TEST(CloudReadinessGateTest, CapabilityEventSequenceAndSnapshotMap) {
    CloudReadinessGate gate;
    std::vector<CapabilityReadiness> events;
    gate.setCapabilityCallback([&](const CapabilityReadiness& c) { events.push_back(c); });
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 1, 1);
    gate.setCapabilityState(kCapabilitySecurityKeyprov, true, true, true, true, 4);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, true);

    ASSERT_FALSE(events.empty());
    uint64_t prev = 0;
    for (const auto& e : events) {
        EXPECT_GT(e.event_sequence, prev);
        prev = e.event_sequence;
    }
    // 全局快照包含 capability map，且与独立查询一致
    auto snap = gate.getReadiness();
    ASSERT_EQ(snap.capabilities.count(kCapabilitySecurityKeyprov), 1u);
    EXPECT_EQ(snap.capabilities.at(kCapabilitySecurityKeyprov).event_sequence,
              gate.getCapabilityReadiness(kCapabilitySecurityKeyprov).event_sequence);
}

TEST(CloudReadinessGateTest, CapabilityLosesReadyOnDisconnect) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.onSubscriptionsReady(true, 1, 1);
    gate.setCapabilityState(kCapabilitySecurityKeyprov, true, true, true, true, 2);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, true);
    ASSERT_TRUE(gate.getCapabilityReadiness(kCapabilitySecurityKeyprov).ready);

    gate.onConnectionState(ConnectionState::RECONNECTING, 1);
    EXPECT_FALSE(gate.isCloudReady());
    EXPECT_FALSE(gate.getCapabilityReadiness(kCapabilitySecurityKeyprov).ready);
}

// ============================================================
// CR-017 §3, §4: ecu_uid 会话投影与 daemon epoch
// ============================================================

TEST(CloudReadinessGateTest, SnapshotCarriesEcuUidAndSessionContext) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 9);
    gate.setTlsSessionIdentity(makeIdentity(12, "sha256:beef", "ECU-123"));
    gate.onSubscriptionsReady(true, 1, 1);

    auto r = gate.getReadiness();
    EXPECT_TRUE(r.cloud_ready);
    EXPECT_EQ(r.ecu_uid, "ECU-123");
    EXPECT_FALSE(r.session_context.daemon_epoch.empty());
    EXPECT_EQ(r.session_context.connection_generation, 9u);
    EXPECT_EQ(r.session_context.cert_session_generation, 12u);
    EXPECT_EQ(r.publish_contract_version, kPublishSessionContractVersion);
    EXPECT_EQ(r.contract_version, kCloudReadinessContractVersion);
}

TEST(CloudReadinessGateTest, NoIdentityHasEmptyEcuUid) {
    CloudReadinessGate gate;
    auto r = gate.getReadiness();
    EXPECT_TRUE(r.ecu_uid.empty());
    EXPECT_FALSE(r.cloud_ready);
}

TEST(CloudReadinessGateTest, DaemonEpochStableAcrossGateInstances) {
    // CR-017 §3: epoch 为进程级，Gate 重建不改变
    CloudReadinessGate gate1;
    CloudReadinessGate gate2;
    EXPECT_EQ(gate1.getDaemonEpoch(), gate2.getDaemonEpoch());
    EXPECT_FALSE(gate1.getDaemonEpoch().empty());
    EXPECT_EQ(gate1.getDaemonEpoch().size(), 32u);  // 128-bit hex
}

TEST(CloudReadinessGateTest, ValidateSessionMatchesCurrentContext) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 9);
    gate.setTlsSessionIdentity(makeIdentity(12));

    SessionContext ok;
    ok.daemon_epoch = gate.getDaemonEpoch();
    ok.connection_generation = 9;
    ok.cert_session_generation = 12;
    EXPECT_TRUE(gate.validateSession(ok));

    SessionContext wrong_epoch = ok;
    wrong_epoch.daemon_epoch = "deadbeef";
    EXPECT_FALSE(gate.validateSession(wrong_epoch));

    SessionContext wrong_conn = ok;
    wrong_conn.connection_generation = 8;
    EXPECT_FALSE(gate.validateSession(wrong_conn));

    SessionContext wrong_cert = ok;
    wrong_cert.cert_session_generation = 11;
    EXPECT_FALSE(gate.validateSession(wrong_cert));
}

TEST(CloudReadinessGateTest, ValidateSessionFalseWithoutValidIdentity) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 9);
    SessionContext ctx;
    ctx.daemon_epoch = gate.getDaemonEpoch();
    ctx.connection_generation = 9;
    ctx.cert_session_generation = 0;
    EXPECT_FALSE(gate.validateSession(ctx));  // 无有效 identity
}

TEST(CloudReadinessGateTest, RevokeKeepsEcuUidInFalseSnapshot) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 2);
    gate.setTlsSessionIdentity(makeIdentity(5, "sha256:aa11bb22", "ECU-REVOKED"));
    gate.onSubscriptionsReady(true, 1, 1);
    CloudReadinessSnapshot revoked;
    gate.setCallback([&](const CloudReadinessSnapshot& s) { revoked = s; });
    gate.revokeSession("credential_changed");
    EXPECT_FALSE(revoked.cloud_ready);
    EXPECT_EQ(revoked.ecu_uid, "ECU-REVOKED");
    EXPECT_EQ(revoked.cert_session_generation, 5u);
}

// ============================================================
// CR-017 §3 / §19: connection generation 非零与一致性不变量
// ============================================================

TEST(CloudReadinessGateTest, ZeroConnectionGenerationBlocksReady) {
    // generation==0 表示尚未提交有效连接上下文：即使连接/订阅就绪也不得 Cloud Ready
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 0);
    gate.setTlsSessionIdentity(makeIdentity(1));
    gate.onSubscriptionsReady(true, 1, 1);
    EXPECT_FALSE(gate.isCloudReady());
    auto r = gate.getReadiness();
    EXPECT_FALSE(r.cloud_ready);
    EXPECT_EQ(r.reason_code, session_reason::kSessionIdentityInvalid);
}

TEST(CloudReadinessGateTest, ReadyImpliesNonzeroGeneration) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 3);
    gate.setTlsSessionIdentity(makeIdentity(2));
    gate.onSubscriptionsReady(true, 1, 1);
    auto r = gate.getReadiness();
    EXPECT_TRUE(r.cloud_ready);
    EXPECT_NE(r.connection_generation, 0u);
    // 顶层 generation 与 session_context 必须来自同一份不可变快照
    EXPECT_EQ(r.connection_generation, r.session_context.connection_generation);
}

TEST(CloudReadinessGateTest, OldGenerationSessionIsStaleAfterContextReplacement) {
    // 重连后上下文替换：旧 expected_session 必须判 stale
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CONNECTED, 1);
    gate.setTlsSessionIdentity(makeIdentity(1));
    gate.onSubscriptionsReady(true, 1, 1);

    SessionContext old_ctx;
    old_ctx.daemon_epoch = gate.getDaemonEpoch();
    old_ctx.connection_generation = 1;
    old_ctx.cert_session_generation = 1;
    EXPECT_TRUE(gate.validateSession(old_ctx));

    // 第二段连接：generation 1 -> 2，cert session 1 -> 2
    gate.setTlsSessionIdentity(makeIdentity(2));
    gate.revokeSession("connection_lost");
    gate.onConnectionState(ConnectionState::CONNECTED, 2);
    gate.onSubscriptionsReady(true, 1, 1);
    EXPECT_FALSE(gate.validateSession(old_ctx));

    SessionContext new_ctx = old_ctx;
    new_ctx.connection_generation = 2;
    new_ctx.cert_session_generation = 2;
    EXPECT_TRUE(gate.validateSession(new_ctx));
}

// ============================================================
// DSN-CR-018 §8: 可信时间门禁字段与共享失效
// ============================================================

namespace {
TimeGateStatus makeTimeStatus(bool ready, bool require = true,
                              const char* reason = "time_untrusted") {
    TimeGateStatus st;
    st.require_trusted_time = require;
    st.bypassed = !require;
    st.time_gate_ready = ready;
    st.trust_state = ready ? TimeTrustState::Trusted : TimeTrustState::Untrusted;
    st.source = TimeSource::PlatformSync;
    st.reason = ready ? TimeGateReason::None : TimeGateReason::Untrusted;
    st.source_epoch = 1;
    st.lease_remaining = std::chrono::milliseconds(500);
    st.time_gate_generation = ready ? 1 : 2;
    return st;
}
}

// cloud_ready = 既有连接订阅条件 AND effective_time_condition（§8）
TEST(CloudReadinessGateTimeTest, TimeGateBlocksReadyWhenEnforcedNotReady) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CLOUD_READY, 1);
    gate.setTlsSessionIdentity(makeIdentity(1));
    gate.onSubscriptionsReady(true, 2, 2);

    // 时间失信（强制）→ cloud_ready 必须为 false
    gate.onTimeGateState(makeTimeStatus(false));
    EXPECT_FALSE(gate.isCloudReady());
    auto snap = gate.getReadiness();
    EXPECT_TRUE(snap.require_trusted_time);
    EXPECT_FALSE(snap.time_gate_ready);
    EXPECT_EQ(snap.time_trust_state, "Untrusted");
    EXPECT_EQ(snap.time_reason, "time_untrusted");
    EXPECT_EQ(snap.time_gate_generation, 2u);

    // 时间恢复 → cloud_ready 恢复
    gate.onTimeGateState(makeTimeStatus(true));
    EXPECT_TRUE(gate.isCloudReady());
    EXPECT_TRUE(gate.getReadiness().time_gate_ready);
    EXPECT_EQ(gate.getReadiness().time_reason, "");
}

// 时间失信：所有 enabled capability ready=false，reason 指向时间；
// disabled 保留 disabled 主因（§8）
TEST(CloudReadinessGateTimeTest, TimeFailureRevokesEnabledCapabilities) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CLOUD_READY, 1);
    gate.setTlsSessionIdentity(makeIdentity(1));
    gate.onSubscriptionsReady(true, 2, 2);
    gate.setCapabilityState(kCapabilitySecurityKeyprov, /*enabled=*/true,
                            /*contract=*/true, /*snapshot=*/true,
                            /*uplink=*/true, /*owner_gen=*/1);
    gate.setCapabilityDownlinkReady(kCapabilitySecurityKeyprov, true);
    EXPECT_TRUE(gate.getCapabilityReadiness(kCapabilitySecurityKeyprov).ready);

    // 时间失信 → capability ready=false，reason 指向时间
    gate.onTimeGateState(makeTimeStatus(false));
    auto cap = gate.getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_FALSE(cap.ready);
    EXPECT_EQ(cap.reason_code, "time_untrusted");

    // 时间恢复 → capability 恢复
    gate.onTimeGateState(makeTimeStatus(true));
    EXPECT_TRUE(gate.getCapabilityReadiness(kCapabilitySecurityKeyprov).ready);
}

// 旁路（require=false）：不拦截 cloud_ready，但 time_gate_bypassed 标记
TEST(CloudReadinessGateTimeTest, BypassDoesNotGateReady) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CLOUD_READY, 1);
    gate.setTlsSessionIdentity(makeIdentity(1));
    gate.onSubscriptionsReady(true, 2, 2);

    gate.onTimeGateState(makeTimeStatus(false, /*require=*/false));
    EXPECT_TRUE(gate.isCloudReady());  // 旁路不改变旧结果
    auto snap = gate.getReadiness();
    EXPECT_TRUE(snap.time_gate_bypassed);
    EXPECT_FALSE(snap.require_trusted_time);
}

// 事件 sequence 与查询同一版本（原子提交 → 递增 → 投递）
TEST(CloudReadinessGateTimeTest, TimeEventSequenceConsistent) {
    CloudReadinessGate gate;
    gate.onConnectionState(ConnectionState::CLOUD_READY, 1);
    gate.setTlsSessionIdentity(makeIdentity(1));
    gate.onSubscriptionsReady(true, 1, 1);

    const uint64_t seq_before = gate.getReadiness().event_sequence;
    CloudReadinessSnapshot cb_snap;
    bool cb_fired = false;
    gate.setCallback([&](const CloudReadinessSnapshot& s) {
        cb_snap = s;
        cb_fired = true;
    });

    gate.onTimeGateState(makeTimeStatus(false));
    EXPECT_TRUE(cb_fired);
    auto snap = gate.getReadiness();
    EXPECT_GT(snap.event_sequence, seq_before);
    // 事件携带的快照与查询读取同一版本（原子提交 → 递增 sequence → 投递）
    EXPECT_EQ(cb_snap.event_sequence, snap.event_sequence);
    EXPECT_EQ(cb_snap.cloud_ready, snap.cloud_ready);
    EXPECT_FALSE(cb_snap.cloud_ready);
}
