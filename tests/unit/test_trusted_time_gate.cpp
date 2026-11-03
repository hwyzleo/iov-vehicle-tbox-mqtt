#include <gtest/gtest.h>
#include "trusted_time_gate.h"
#include <thread>
#include <chrono>

using namespace tbox::mqtt;

namespace {

// 构造一个可信样本（测试辅助）
TrustedTimeSample makeTrustedSample(std::chrono::milliseconds remaining =
                                        std::chrono::milliseconds(2000),
                                    TimeSource src = TimeSource::PlatformSync,
                                    uint64_t epoch = 1,
                                    uint64_t seq = 1,
                                    bool aligned = true,
                                    uint32_t contract = 1) {
    TrustedTimeSample s;
    s.utc_now = std::chrono::system_clock::now();
    s.trust_state = TimeTrustState::Trusted;
    s.source = src;
    s.freshness_age = std::chrono::milliseconds(100);
    s.uncertainty = std::chrono::milliseconds(2000);
    s.reason = TimeGateReason::None;
    s.source_epoch = epoch;
    s.sample_sequence = seq;
    s.provider_epoch = "sec-epoch-1";
    s.remaining_validity = remaining;
    s.clock_aligned = aligned;
    s.contract_version = contract;
    s.valid = true;
    return s;
}

// 等待条件（带超时）
bool waitUntil(std::function<bool()> pred, int timeout_ms = 2000) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// 脚本化 provider：按序列返回结果，可模拟查询阻塞（租约到期路径）
class ScriptedProvider final : public ITrustedTimeProvider {
public:
    std::vector<TimeQueryResult> results;
    std::vector<std::chrono::milliseconds> delays;
    size_t idx = 0;

    TimeQueryResult query() override {
        size_t i = std::min(idx, results.empty() ? 0u : results.size() - 1);
        if (i < delays.size() && delays[i].count() > 0) {
            std::this_thread::sleep_for(delays[i]);
        }
        if (!results.empty()) idx++;
        return results.empty() ? TimeQueryResult{} : results[i];
    }
    TimeProviderInfo info() const override { return {"scripted", false}; }
};

// 构造不可用结果
TimeQueryResult makeUnavailable(TimeGateReason r) {
    TimeQueryResult q;
    q.ok = false;
    q.reason = r;
    return q;
}

} // namespace

// ============================================================
// 配置纯校验（AT-10 基础）
// ============================================================

TEST(TrustedTimeGateTest, DefaultConfigIsValid) {
    TrustedTimeConfig cfg;
    std::string err;
    EXPECT_TRUE(TrustedTimeConfig::validate(cfg, &err));
    EXPECT_TRUE(cfg.require_trusted_time);  // 安全缺省 true
}

TEST(TrustedTimeGateTest, InvalidConfigRejected) {
    TrustedTimeConfig cfg;
    cfg.query_timeout_ms = std::chrono::milliseconds(1);  // < 50
    std::string err;
    EXPECT_FALSE(TrustedTimeConfig::validate(cfg, &err));
    EXPECT_FALSE(err.empty());

    cfg = TrustedTimeConfig{};
    cfg.retry_initial_backoff_ms = std::chrono::milliseconds(50000);  // > 10000
    EXPECT_FALSE(TrustedTimeConfig::validate(cfg, &err));

    cfg = TrustedTimeConfig{};
    cfg.retry_max_backoff_ms = std::chrono::milliseconds(10);  // < initial
    EXPECT_FALSE(TrustedTimeConfig::validate(cfg, &err));

    cfg = TrustedTimeConfig{};
    cfg.retry_multiplier = 10.0;  // > 4
    EXPECT_FALSE(TrustedTimeConfig::validate(cfg, &err));
}

// ============================================================
// 强制模式 fail-closed：默认（无 SEC 契约）provider
// ============================================================

TEST(TrustedTimeGateTest, EnforcedWithDefaultProviderFailsClosed) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    ASSERT_TRUE(gate.configure(cfg));
    gate.setProvider(createDefaultTimeProvider());  // ContractUnsupported
    gate.start();

    EXPECT_TRUE(gate.isEnforced());
    EXPECT_FALSE(gate.isBypassed());
    // ContractUnsupported → 永不 ready；等待首次查询完成
    EXPECT_TRUE(waitUntil([&] {
        return gate.getStatus().reason == TimeGateReason::ContractUnsupported;
    }));
    EXPECT_FALSE(gate.isReady());
    // 受理拒绝
    auto ad = gate.checkAdmission();
    EXPECT_FALSE(ad.allowed);
    EXPECT_FALSE(ad.bypassed);

    // 默认 provider 不是 fake
    EXPECT_FALSE(gate.getStatus().provider_is_test_fake);
    gate.stop();
}

// ============================================================
// 旁路模式（require=false，dev/test）
// ============================================================

TEST(TrustedTimeGateTest, BypassKeepsOldBehaviorWithFlag) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    cfg.require_trusted_time = false;
    ASSERT_TRUE(gate.configure(cfg));
    gate.setProvider(createDefaultTimeProvider());
    gate.start();

    EXPECT_FALSE(gate.isEnforced());
    EXPECT_TRUE(gate.isBypassed());
    // 旁路：门禁恒放行（旧行为），但 time_gate_ready 不伪造为 true
    auto ad = gate.checkAdmission();
    EXPECT_TRUE(ad.allowed);
    EXPECT_TRUE(ad.bypassed);
    auto st = gate.getStatus();
    EXPECT_TRUE(st.bypassed);
    gate.stop();
}

// ============================================================
// Fake 可信样本 → ready；租约管理
// ============================================================

TEST(TrustedTimeGateTest, FakeTrustedSampleOpensGate) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    cfg.cache_max_age_ms = std::chrono::milliseconds(1000);
    ASSERT_TRUE(gate.configure(cfg));
    gate.setProvider(createFakeTimeProvider(makeTrustedSample(
        std::chrono::milliseconds(2000))));
    gate.start();

    EXPECT_TRUE(waitUntil([&] { return gate.isReady(); }));
    EXPECT_TRUE(gate.getStatus().provider_is_test_fake);
    auto ad = gate.checkAdmission();
    EXPECT_TRUE(ad.allowed);
    EXPECT_EQ(ad.time_gate_generation, gate.getGeneration());
    gate.stop();
}

TEST(TrustedTimeGateTest, UntrustedSampleKeepsClosed) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    ASSERT_TRUE(gate.configure(cfg));
    auto s = makeTrustedSample();
    s.trust_state = TimeTrustState::Untrusted;
    s.reason = TimeGateReason::Untrusted;
    gate.setProvider(createFakeTimeProvider(s));
    gate.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_FALSE(gate.isReady());
    EXPECT_FALSE(gate.checkAdmission().allowed);
    gate.stop();
}

TEST(TrustedTimeGateTest, TrustedButClockNotAlignedKeepsClosed) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    ASSERT_TRUE(gate.configure(cfg));
    gate.setProvider(createFakeTimeProvider(
        makeTrustedSample(std::chrono::milliseconds(2000),
                          TimeSource::PlatformSync, 1, 1, /*aligned=*/false)));
    gate.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_FALSE(gate.isReady());
    EXPECT_EQ(gate.getStatus().reason, TimeGateReason::ClockNotAligned);
    gate.stop();
}

// ============================================================
// 租约到期撤销 + generation 递增（§6.1/§6.2）
// ============================================================

TEST(TrustedTimeGateTest, LeaseExpiryRevokesAndBumpsGeneration) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    cfg.cache_max_age_ms = std::chrono::milliseconds(100);
    ASSERT_TRUE(gate.configure(cfg));

    // 脚本化：首次查询返回有效样本（usable lease = min(1000,100) = 100ms）；
    // 第二次查询阻塞 500ms（模拟慢 SEC），期间租约到期 → timerLoop 撤销。
    auto provider = std::make_shared<ScriptedProvider>();
    TimeQueryResult ok;
    ok.ok = true;
    ok.sample = makeTrustedSample(std::chrono::milliseconds(1000));
    provider->results = {ok, makeUnavailable(TimeGateReason::Untrusted)};
    provider->delays = {std::chrono::milliseconds(0),
                        std::chrono::milliseconds(500)};
    gate.setProvider(provider);
    gate.start();

    EXPECT_TRUE(waitUntil([&] { return gate.isReady(); }));
    const uint64_t gen_before = gate.getGeneration();

    // 租约 100ms 到期自动撤销（刷新查询仍在途阻塞）
    EXPECT_TRUE(waitUntil([&] { return !gate.isReady(); }, 3000));
    EXPECT_EQ(gate.getGeneration(), gen_before + 1);
    EXPECT_FALSE(gate.checkAdmission().allowed);
    gate.stop();
}

// ============================================================
// 运行期失信撤销（模拟）幂等
// ============================================================

TEST(TrustedTimeGateTest, RevokeIsIdempotent) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    ASSERT_TRUE(gate.configure(cfg));
    gate.setProvider(createFakeTimeProvider(makeTrustedSample()));
    gate.start();
    EXPECT_TRUE(waitUntil([&] { return gate.isReady(); }));

    const uint64_t gen = gate.getGeneration();
    gate.revoke(TimeGateReason::Untrusted);
    EXPECT_FALSE(gate.isReady());
    EXPECT_EQ(gate.getGeneration(), gen + 1);

    // 幂等：已撤销后再 revoke 不重复递增
    const uint64_t gen2 = gate.getGeneration();
    gate.revoke(TimeGateReason::Untrusted);
    EXPECT_EQ(gate.getGeneration(), gen2);
    gate.stop();
}

// ============================================================
// 来源/epoch 变化失效旧缓存（§3.3）
// ============================================================

TEST(TrustedTimeGateTest, SourceEpochChangeInvalidatesCache) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    ASSERT_TRUE(gate.configure(cfg));
    gate.setProvider(createFakeTimeProvider(
        makeTrustedSample(std::chrono::milliseconds(5000),
                          TimeSource::PlatformSync, 1, 1)));
    gate.start();
    EXPECT_TRUE(waitUntil([&] { return gate.isReady(); }));
    const uint64_t gen = gate.getGeneration();

    // 来源切换（RTC, epoch 不同）→ 旧缓存失效；新样本 Trusted 可重建
    gate.simulateSample(makeTrustedSample(
        std::chrono::milliseconds(5000),
        TimeSource::HardwareRtc, 2, 1));
    EXPECT_TRUE(gate.isReady());
    EXPECT_EQ(gate.getStatus().source_epoch, 2u);

    // 序列缺口/乱序：旧序列样本不得覆盖新查询结果（不授信旧事件）
    gate.simulateSample(makeTrustedSample(
        std::chrono::milliseconds(5000),
        TimeSource::HardwareRtc, 2, 0));  // seq 0 < 1 → 视为旧
    EXPECT_EQ(gate.getStatus().source_epoch, 2u);
    EXPECT_TRUE(gate.isReady());
    EXPECT_EQ(gen, gate.getGeneration());
    gate.stop();
}

// ============================================================
// 样本损坏/契约缺失拒绝（§3.2/§5）
// ============================================================

TEST(TrustedTimeGateTest, MalformedAndNoContractRejected) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    ASSERT_TRUE(gate.configure(cfg));

    // contract_version = 0 → ContractUnsupported
    gate.setProvider(createFakeTimeProvider(
        makeTrustedSample(std::chrono::milliseconds(5000),
                          TimeSource::PlatformSync, 1, 1, true, /*contract=*/0)));
    gate.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_FALSE(gate.isReady());
    EXPECT_EQ(gate.getStatus().reason, TimeGateReason::ContractUnsupported);

    // 负 remaining_validity → 不可用
    gate.stop();
    gate.setProvider(createFakeTimeProvider(
        makeTrustedSample(std::chrono::milliseconds(-100))));
    gate.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_FALSE(gate.isReady());
    gate.stop();
}

// ============================================================
// 回调：ready 翻转事件（§8 事件语义）
// ============================================================

TEST(TrustedTimeGateTest, CallbackOnReadyFlip) {
    TrustedTimeGate gate;
    TrustedTimeConfig cfg;
    ASSERT_TRUE(gate.configure(cfg));

    int ready_true_events = 0;
    int ready_false_events = 0;
    gate.setChangeCallback([&](const TimeGateStatus& st, bool ready_changed) {
        if (!ready_changed) return;
        if (st.time_gate_ready) ready_true_events++;
        else ready_false_events++;
    });

    gate.setProvider(createFakeTimeProvider(makeTrustedSample()));
    gate.start();
    EXPECT_TRUE(waitUntil([&] { return gate.isReady(); }));
    EXPECT_GE(ready_true_events, 1);

    gate.revoke(TimeGateReason::Untrusted);
    EXPECT_GE(ready_false_events, 1);
    gate.stop();
}
