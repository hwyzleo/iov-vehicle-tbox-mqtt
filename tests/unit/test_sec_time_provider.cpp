//
// TBOX-SEC-DSN-CR-024：SEC 配对可信时间 provider 单元测试。
//
// 覆盖：
//   - mapEnvelope：trusted/untrusted/unknown 映射、字段透传、invalid → valid=false
//     （缺关键字段不按 Trusted 默认，§3.2）；
//   - query：SEC 不可达 → ProviderUnavailable（fail-closed → TIME_WAIT）；
//   - info：provider_epoch 为 SEC daemon epoch、is_test_fake=false。
//

#include <gtest/gtest.h>

#include "sec_time_provider.h"
#include "tbox/sec/time.h"

using namespace tbox::mqtt;

namespace {

tbox::sec::TrustedTimeEnvelope make_env(const std::string& trust_state,
                                        bool contract_complete = true,
                                        bool valid = true,
                                        uint64_t sec_epoch = 42) {
    tbox::sec::TrustedTimeEnvelope e;
    e.contract_version = 1;
    e.capabilities = "snapshot,v1";
    e.contract_complete = contract_complete;
    e.sec_daemon_epoch = sec_epoch;
    e.sec_time_event_sequence = 7;
    e.time_daemon_epoch = 1;
    e.time_event_sequence = 1;
    e.source_id = "ntp-verified-1";
    e.source_kind = "ntp_network";
    e.source_epoch = 1;
    e.policy_version = "1";
    e.trust_state = trust_state;
    e.source = "platform_sync";
    e.reason = "platform_synced";
    e.utc_valid = true;
    e.utc_epoch_ms = 1'700'000'000'000;
    e.freshness_age_ms = 250;
    e.uncertainty_valid = true;
    e.uncertainty_ms = 60;
    e.remaining_validity_ms = 900;
    e.drift_bound_ppm = 100;
    e.system_clock_aligned = true;
    e.alignment_error_ms = 0;
    e.auth_status = "verified";
    e.evidence_ref = "ev:1";
    e.valid = valid;
    e.error_class = "";
    return e;
}

}  // namespace

TEST(SecTimeProviderTest, MapTrustedEnvelope) {
    const auto s = SecTimeProvider::mapEnvelope(make_env("trusted"));
    EXPECT_EQ(s.trust_state, TimeTrustState::Trusted);
    EXPECT_EQ(s.source, TimeSource::PlatformSync);
    EXPECT_EQ(s.reason, TimeGateReason::None);
    EXPECT_EQ(s.source_epoch, 1u);
    EXPECT_EQ(s.sample_sequence, 7u);
    EXPECT_EQ(s.provider_epoch, "42");
    EXPECT_EQ(s.remaining_validity, std::chrono::milliseconds(900));
    EXPECT_EQ(s.freshness_age, std::chrono::milliseconds(250));
    EXPECT_EQ(s.uncertainty, std::chrono::milliseconds(60));
    EXPECT_EQ(s.clock_aligned, true);
    EXPECT_EQ(s.contract_version, 1u);
    EXPECT_EQ(s.valid, true);
}

// 未认证 NTP → SEC untrusted → MQTT 样本 untrusted（TIME_WAIT 依据）
TEST(SecTimeProviderTest, MapUntrustedEnvelope) {
    const auto s = SecTimeProvider::mapEnvelope(
        make_env("untrusted"));
    EXPECT_EQ(s.trust_state, TimeTrustState::Untrusted);
    EXPECT_EQ(s.reason, TimeGateReason::Untrusted);
    EXPECT_EQ(s.valid, true);  // 可查询业务状态，非畸形
}

TEST(SecTimeProviderTest, MapUnknownEnvelope) {
    const auto s = SecTimeProvider::mapEnvelope(
        make_env("unknown", /*contract_complete=*/false));
    EXPECT_EQ(s.trust_state, TimeTrustState::Unknown);
    EXPECT_EQ(s.reason, TimeGateReason::Unknown);
}

// §3.2：envelope invalid / 缺关键字段 → sample.valid=false（不按 Trusted 默认）
TEST(SecTimeProviderTest, MapInvalidEnvelopeNotValid) {
    const auto s = SecTimeProvider::mapEnvelope(
        make_env("trusted", /*contract_complete=*/true, /*valid=*/false));
    EXPECT_EQ(s.valid, false);

    const auto s2 = SecTimeProvider::mapEnvelope(
        make_env("trusted", /*contract_complete=*/false, /*valid=*/true));
    EXPECT_EQ(s2.valid, false);
}

// 传输错误分类独立于业务状态：不把不可达伪装成 untrusted/unknown
TEST(SecTimeProviderTest, MapErrorClassNotValid) {
    auto e = make_env("unknown");
    e.error_class = "stale";
    const auto s = SecTimeProvider::mapEnvelope(e);
    EXPECT_EQ(s.valid, false);
}

// SEC 不可达 → ProviderUnavailable（fail-closed → TIME_WAIT）
TEST(SecTimeProviderTest, QueryWhenSecAbsentProviderUnavailable) {
    auto provider = createSecTimeProvider(
        "/tmp/tbox-sec-time-provider-test-nonexistent.sock");
    const auto r = provider->query();
    EXPECT_EQ(r.ok, false);
    EXPECT_EQ(r.reason, TimeGateReason::ProviderUnavailable);

    const auto info = provider->info();
    EXPECT_EQ(info.is_test_fake, false);
}
