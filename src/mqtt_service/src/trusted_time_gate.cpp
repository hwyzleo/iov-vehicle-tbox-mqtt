#include "trusted_time_gate.h"
#include <algorithm>
#include <cmath>

namespace tbox {
namespace mqtt {

// ============================================================
// TrustedTimeConfig::validate（§9 纯校验）
// ============================================================

bool TrustedTimeConfig::validate(const TrustedTimeConfig& cfg, std::string* err) {
    auto fail = [&](const char* msg) {
        if (err) *err = msg;
        return false;
    };
    const auto q = cfg.query_timeout_ms.count();
    const auto r = cfg.refresh_interval_ms.count();
    const auto c = cfg.cache_max_age_ms.count();
    const auto bi = cfg.retry_initial_backoff_ms.count();
    const auto bm = cfg.retry_max_backoff_ms.count();
    if (q < 50 || q > 1000) return fail("trusted_time.query_timeout_ms must be in [50,1000]");
    if (r < 100 || r > 1000) return fail("trusted_time.refresh_interval_ms must be in [100,1000]");
    if (c < 100 || c > 1000) return fail("trusted_time.cache_max_age_ms must be in [100,1000]");
    if (bi < 100 || bi > 10000) return fail("trusted_time.retry.initial_backoff_ms must be in [100,10000]");
    if (bm < bi || bm > 60000) return fail("trusted_time.retry.max_backoff_ms must be in [initial,60000]");
    if (!std::isfinite(cfg.retry_multiplier) || cfg.retry_multiplier < 1.0 || cfg.retry_multiplier > 4.0)
        return fail("trusted_time.retry.multiplier must be finite in [1,4]");
    if (!std::isfinite(cfg.retry_jitter_ratio) || cfg.retry_jitter_ratio < 0.0 || cfg.retry_jitter_ratio > 0.5)
        return fail("trusted_time.retry.jitter_ratio must be finite in [0,0.5]");
    return true;
}

// ============================================================
// 默认/测试提供方
// ============================================================

namespace {

/// 生产默认提供方：SEC 配对契约（DEP-TIME-01）未落地前的占位。
/// 强制模式 fail-closed → 永远不可用；不伪造 SEC SDK 方法。
class ContractUnsupportedProvider final : public ITrustedTimeProvider {
public:
    TimeQueryResult query() override {
        TimeQueryResult r;
        r.ok = false;
        r.reason = TimeGateReason::ContractUnsupported;
        return r;
    }
    TimeProviderInfo info() const override {
        return TimeProviderInfo{"contract-unsupported", false};
    }
};

/// dev/test 专用 fake：仅测试 fixture 构造；production 工厂/schema 拒绝。
class FakeTimeProvider final : public ITrustedTimeProvider {
public:
    explicit FakeTimeProvider(TrustedTimeSample sample) : sample_(sample) {
        sample_.valid = true;
        sample_.provider_epoch = "fake";
    }
    TimeQueryResult query() override {
        TimeQueryResult r;
        std::lock_guard<std::mutex> lock(mu_);
        r.ok = sample_.valid;
        r.sample = sample_;
        r.reason = r.ok ? TimeGateReason::None : TimeGateReason::Unknown;
        return r;
    }
    void set_now(const TrustedTimeSample& s) {
        std::lock_guard<std::mutex> lock(mu_);
        sample_ = s;
        sample_.valid = true;
        sample_.provider_epoch = "fake";
    }
    TimeProviderInfo info() const override {
        return TimeProviderInfo{"fake", true};
    }
private:
    mutable std::mutex mu_;
    TrustedTimeSample sample_;
};

} // namespace

std::shared_ptr<ITrustedTimeProvider> createDefaultTimeProvider() {
    return std::make_shared<ContractUnsupportedProvider>();
}

std::shared_ptr<ITrustedTimeProvider> createFakeTimeProvider(
    const TrustedTimeSample& sample) {
    return std::make_shared<FakeTimeProvider>(sample);
}

// ============================================================
// TrustedTimeGate
// ============================================================

TrustedTimeGate::TrustedTimeGate() = default;

TrustedTimeGate::~TrustedTimeGate() {
    stop();
}

bool TrustedTimeGate::configure(const TrustedTimeConfig& cfg, std::string* err) {
    if (!TrustedTimeConfig::validate(cfg, err)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    config_ = cfg;
    bypass_ = !cfg.require_trusted_time;
    return true;
}

void TrustedTimeGate::setProvider(std::shared_ptr<ITrustedTimeProvider> provider) {
    std::lock_guard<std::mutex> lock(mutex_);
    provider_ = std::move(provider);
    // provider 变化 → 旧缓存立即失效（§3.3 缓存键含 provider 生命周期标识）
    has_sample_ = false;
    ready_ = false;
}

void TrustedTimeGate::setChangeCallback(ChangeCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(cb);
}

void TrustedTimeGate::setEventCallback(EventCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    event_callback_ = std::move(cb);
}

void TrustedTimeGate::start() {
    stop();
    EventCallback ev_cb;
    EventKind ev_kind = EventKind::kBypassChanged;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!provider_) {
            provider_ = createDefaultTimeProvider();
        }
        started_ = true;
        timer_running_ = true;
        // 强制模式：首次查询立即执行（不计 retry）。旁路模式：shadow 观测。
        query_inflight_ = false;
        next_query_at_ = std::chrono::steady_clock::now();
        timer_thread_ = std::thread(&TrustedTimeGate::timerLoop, this);
        if (bypass_) {
            ready_ = true;  // 开发旁路：保持旧行为，门禁不拦截（§10.1 P0）
            ev_cb = event_callback_;
            ev_kind = EventKind::kBypassChanged;
        }
    }
    timer_cv_.notify_all();
    if (ev_cb) ev_cb(ev_kind, buildStatusForCallbackLocked());
}

void TrustedTimeGate::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        timer_running_ = false;
        timer_cv_.notify_all();
    }
    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    started_ = false;
    query_inflight_ = false;
}

TimeGateStatus TrustedTimeGate::getStatus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    TimeGateStatus st;
    st.require_trusted_time = config_.require_trusted_time;
    st.bypassed = bypass_;
    st.time_gate_ready = ready_;
    st.trust_state = sample_.trust_state;
    st.source = sample_.source;
    st.reason = sample_.reason;
    st.source_epoch = sample_.source_epoch;
    st.clock_aligned = sample_.clock_aligned;
    st.freshness_ms = sample_.freshness_age;
    st.uncertainty_ms = sample_.uncertainty;
    st.time_gate_generation = gate_generation_;
    st.retry_count = retry_count_;
    st.backoff_ms = std::chrono::milliseconds(0);
    if (has_sample_ && ready_) {
        auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
            lease_deadline_ - std::chrono::steady_clock::now());
        st.lease_remaining = remain > std::chrono::milliseconds(0)
            ? remain : std::chrono::milliseconds(0);
    }
    if (provider_) {
        st.provider_is_test_fake = provider_->info().is_test_fake;
    }
    return st;
}

bool TrustedTimeGate::isReady() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return ready_;
}

bool TrustedTimeGate::isBypassed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return bypass_;
}

bool TrustedTimeGate::isEnforced() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_.require_trusted_time;
}

uint64_t TrustedTimeGate::getGeneration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return gate_generation_;
}

TrustedTimeConfig TrustedTimeGate::getConfig() const {
    return config_;
}

bool TrustedTimeGate::cacheUsableLocked() const {
    if (!has_sample_ || !ready_) return false;
    return lease_deadline_ > std::chrono::steady_clock::now();
}

TimeAdmission TrustedTimeGate::checkAdmission() {
    std::lock_guard<std::mutex> lock(mutex_);
    TimeAdmission ad;
    ad.bypassed = bypass_;
    if (bypass_) {
        // 开发旁路：保持旧行为；shadow 观测不拦截（§10.1）。
        // 受理 provenance 标记 BYPASS（恢复强制后不盲发，REQ-CR-019 §4）
        ad.allowed = true;
        ad.reason = TimeGateReason::Bypassed;
        ad.time_gate_generation = gate_generation_;
        ad.provenance = TimeProvenance::BYPASS;
        return ad;
    }
    if (cacheUsableLocked()) {
        ad.allowed = true;
        ad.reason = TimeGateReason::None;
        ad.time_gate_generation = gate_generation_;
        // §7.2: 受理 provenance 与保守 UTC 区间（样本 utc ± uncertainty）
        ad.provenance = TimeProvenance::TRUSTED;
        ad.source = timeSourceToString(sample_.source);
        ad.source_epoch = sample_.source_epoch;
        const auto utc_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            sample_.utc_now.time_since_epoch()).count();
        const int64_t unc = sample_.uncertainty.count();
        ad.utc_earliest_ms = utc_ms - unc;
        ad.utc_latest_ms = utc_ms + unc;
        return ad;
    }
    // 缓存无效：触发异步刷新并拒绝（§7.1 不在调用线程阻塞查询）
    if (started_ && !query_inflight_) {
        next_query_at_ = std::chrono::steady_clock::now();
        timer_cv_.notify_all();
    }
    ad.allowed = false;
    ad.provenance = TimeProvenance::UNKNOWN;
    if (has_sample_ && lease_deadline_ <= std::chrono::steady_clock::now()) {
        ad.reason = TimeGateReason::SampleExpired;
    } else {
        ad.reason = sample_.valid ? sample_.reason : TimeGateReason::Unknown;
    }
    ad.time_gate_generation = gate_generation_;
    return ad;
}

void TrustedTimeGate::revoke(const TimeGateReason& reason) {
    ChangeCallback cb;
    EventCallback ev_cb;
    EventKind ev_kind = EventKind::kTrustLost;
    TimeGateStatus st;
    bool ready_changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (bypass_) {
            // 旁路不执行运行期撤销（旧行为）；仅记录 reason 供 shadow 观测
            sample_.reason = reason;
            cb = callback_;
            st = buildStatusForCallbackLocked();
            ready_changed = false;
        } else if (ready_) {
            ready_ = false;
            has_sample_ = false;
            sample_ = TrustedTimeSample{};
            sample_.reason = reason;
            // §6.2：撤销递增 time_gate_generation，使所有已排队发送任务重新检查
            gate_generation_++;
            ready_changed = true;
            if (started_ && !query_inflight_) {
                next_query_at_ = std::chrono::steady_clock::now();
            }
            cb = callback_;
            ev_cb = event_callback_;
            ev_kind = EventKind::kTrustLost;
            st = buildStatusForCallbackLocked();
        } else {
            // 幂等：已处于不可用，不重复递增 generation
            sample_.reason = reason;
            cb = callback_;
            st = buildStatusForCallbackLocked();
        }
    }
    timer_cv_.notify_all();
    if (cb) cb(st, ready_changed);
    if (ev_cb) ev_cb(ev_kind, buildStatusForCallbackLocked());
}

TimeGateStatus TrustedTimeGate::buildStatusForCallbackLocked() const {
    // 与 getStatus 一致（持锁路径）
    TimeGateStatus st;
    st.require_trusted_time = config_.require_trusted_time;
    st.bypassed = bypass_;
    st.time_gate_ready = ready_;
    st.trust_state = sample_.trust_state;
    st.source = sample_.source;
    st.reason = sample_.reason;
    st.source_epoch = sample_.source_epoch;
    st.clock_aligned = sample_.clock_aligned;
    st.freshness_ms = sample_.freshness_age;
    st.uncertainty_ms = sample_.uncertainty;
    st.time_gate_generation = gate_generation_;
    st.retry_count = retry_count_;
    if (has_sample_ && ready_) {
        auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
            lease_deadline_ - std::chrono::steady_clock::now());
        st.lease_remaining = remain > std::chrono::milliseconds(0)
            ? remain : std::chrono::milliseconds(0);
    }
    if (provider_) {
        st.provider_is_test_fake = provider_->info().is_test_fake;
    }
    return st;
}

// ---- dev/test 注入 ----

void TrustedTimeGate::simulateSample(const TrustedTimeSample& sample) {
    ChangeCallback cb;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 绕过 provider，直接按契约语义提交（RTT≈0）
        TrustedTimeSample s = sample;
        s.valid = true;
        const int64_t rtt_ms = 0;
        const int64_t rem = s.remaining_validity.count() - rtt_ms;
        const int64_t cap = config_.cache_max_age_ms.count();
        const int64_t lease = std::min(rem, cap);
        if (lease <= 0) {
            has_sample_ = false;
            ready_ = false;
            sample_ = s;
            sample_.reason = TimeGateReason::SampleExpired;
        } else {
            // §3.3 缓存键：(provider_epoch, source, source_epoch, sample_sequence)
            const bool same_context =
                has_sample_ && sample_.provider_epoch == s.provider_epoch &&
                sample_.source == s.source && sample_.source_epoch == s.source_epoch &&
                s.sample_sequence >= sample_.sample_sequence;
            const bool prev_ready = ready_;
            sample_ = s;
            has_sample_ = true;
            sample_received_at_ = std::chrono::steady_clock::now();
            lease_deadline_ = sample_received_at_ + std::chrono::milliseconds(lease);
            if (config_.require_trusted_time) {
                ready_ = (s.trust_state == TimeTrustState::Trusted) && s.clock_aligned;
                if (ready_) {
                    resetRetryLocked();
                    scheduleRefreshLocked();
                } else {
                    // 来源/epoch 变动先失效（§3.3）；未对齐/不可信 → 不可用
                    sample_.reason = s.clock_aligned
                        ? s.reason : TimeGateReason::ClockNotAligned;
                    scheduleRetryLocked();
                }
            } else {
                ready_ = true;  // 旁路 shadow：门禁不拦截
            }
            changed = (prev_ready != ready_);
        }
        cb = callback_;
    }
    timer_cv_.notify_all();
    if (cb) cb(buildStatusForCallbackLocked(), changed);
}

void TrustedTimeGate::simulateUnavailable(TimeGateReason reason) {
    ChangeCallback cb;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const bool prev_ready = ready_;
        has_sample_ = false;
        sample_ = TrustedTimeSample{};
        sample_.reason = reason;
        if (!bypass_) {
            ready_ = false;
            changed = prev_ready;
            if (prev_ready) gate_generation_++;
            scheduleRetryLocked();
        }
        cb = callback_;
    }
    timer_cv_.notify_all();
    if (cb) cb(buildStatusForCallbackLocked(), changed);
}

// ---- 内部 ----

bool TrustedTimeGate::commitSampleLocked(const TrustedTimeSample& s,
                                          std::chrono::milliseconds rtt) {
    const bool prev_ready = ready_;
    // §3.2：溢出/越界/未知枚举整体拒绝，不按 Trusted 默认
    if (!s.valid) {
        sample_.reason = TimeGateReason::SampleMalformed;
        has_sample_ = false;
        ready_ = false;
        scheduleRetryLocked();
        return prev_ready != ready_;
    }
    if (s.contract_version == 0) {
        sample_.reason = TimeGateReason::ContractUnsupported;
        has_sample_ = false;
        ready_ = false;
        scheduleRetryLocked();
        return prev_ready != ready_;
    }
    if (s.freshness_age.count() < 0 || s.uncertainty.count() < 0 ||
        s.remaining_validity.count() < 0) {
        sample_.reason = TimeGateReason::SampleMalformed;
        has_sample_ = false;
        ready_ = false;
        scheduleRetryLocked();
        return prev_ready != ready_;
    }

    // 保守租约：usable_lease = min(remaining_validity - RTT, cache_max_age_ms)
    const int64_t rem = s.remaining_validity.count() - rtt.count();
    const int64_t cap = config_.cache_max_age_ms.count();
    const int64_t lease = std::min(rem, cap);
    if (lease <= 0) {
        // 已无剩余租约的回复不得缓存/用于后续操作（§3.2）
        sample_.reason = TimeGateReason::SampleExpired;
        has_sample_ = false;
        ready_ = false;
        scheduleRetryLocked();
        return prev_ready != ready_;
    }

    // §3.3 缓存键一致性：provider_epoch/source/source_epoch/序列
    const bool same_context =
        has_sample_ && sample_.provider_epoch == s.provider_epoch &&
        sample_.source == s.source && sample_.source_epoch == s.source_epoch &&
        s.sample_sequence >= sample_.sample_sequence;
    if (has_sample_ && !same_context) {
        // 来源/epoch 变动先失效并重新查询（§3.3）；不可凭数值递增认定连续信任
        has_sample_ = false;
    }

    sample_ = s;
    has_sample_ = true;
    sample_received_at_ = std::chrono::steady_clock::now();
    lease_deadline_ = sample_received_at_ + std::chrono::milliseconds(lease);

    if (config_.require_trusted_time) {
        const bool trust_ok = (s.trust_state == TimeTrustState::Trusted);
        const bool aligned = s.clock_aligned;
        ready_ = trust_ok && aligned;
        if (ready_) {
            resetRetryLocked();
            scheduleRefreshLocked();
        } else {
            sample_.reason = trust_ok ? TimeGateReason::ClockNotAligned : s.reason;
            scheduleRetryLocked();
        }
    } else {
        ready_ = true;  // 旁路 shadow：不拦截
    }
    return prev_ready != ready_;
}

void TrustedTimeGate::doQuery() {
    std::shared_ptr<ITrustedTimeProvider> provider;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started_ || query_inflight_) return;
        provider = provider_;
        query_inflight_ = true;
        query_started_at_ = std::chrono::steady_clock::now();
        total_attempts_++;
    }

    TimeQueryResult result;
    if (provider) {
        result = provider->query();
    } else {
        result.ok = false;
        result.reason = TimeGateReason::ProviderUnavailable;
    }

    const auto rtt = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - query_started_at_);

    ChangeCallback cb;
    EventCallback ev_cb;
    EventKind ev_kind = EventKind::kQueryFailed;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        query_inflight_ = false;
        if (!started_) return;  // stop 后丢弃迟到结果

        if (result.ok) {
            changed = commitSampleLocked(result.sample, rtt);
            if (changed && ready_) {
                ev_cb = event_callback_;
                ev_kind = EventKind::kReady;
            }
        } else {
            const bool prev_ready = ready_;
            has_sample_ = false;
            sample_ = TrustedTimeSample{};
            sample_.reason = result.reason;
            if (!bypass_) {
                ready_ = false;
                changed = prev_ready;
                if (prev_ready) gate_generation_++;
            }
            scheduleRetryLocked();
            // 旁路 shadow 观测不产生失败事件（避免每刷新周期刷 WARN）
            if (!bypass_) {
                ev_cb = event_callback_;
                ev_kind = prev_ready ? EventKind::kTrustLost : EventKind::kQueryFailed;
            }
        }
        cb = callback_;
    }
    timer_cv_.notify_all();
    if (cb) cb(buildStatusForCallbackLocked(), changed);
    if (ev_cb) {
        const TimeGateStatus st = buildStatusForCallbackLocked();
        ev_cb(ev_kind, st);
        // INFO/DEBUG：失败/失信后已调度下一次重试（§11 mqtt.time_gate.retry_scheduled）
        if (ev_kind == EventKind::kQueryFailed || ev_kind == EventKind::kTrustLost) {
            ev_cb(EventKind::kRetryScheduled, st);
        }
    }
}

std::chrono::milliseconds TrustedTimeGate::calculateBackoffLocked() const {
    double delay_ms = static_cast<double>(config_.retry_initial_backoff_ms.count());
    for (uint32_t i = 0; i < std::min<uint32_t>(retry_count_, 6u); ++i) {
        delay_ms *= config_.retry_multiplier;
    }
    auto delay = Duration(static_cast<int64_t>(delay_ms));
    delay = std::min(delay, config_.retry_max_backoff_ms);
    // jitter ±ratio，抖动后硬裁剪到有效范围
    const int64_t jitter_ms = static_cast<int64_t>(
        delay.count() * config_.retry_jitter_ratio);
    if (jitter_ms > 0) {
        std::uniform_int_distribution<int64_t> dist(-jitter_ms, jitter_ms);
        delay += Duration(dist(rng_));
    }
    if (delay.count() < 1) delay = Duration(1);
    delay = std::min(delay, config_.retry_max_backoff_ms);
    return delay;
}

void TrustedTimeGate::scheduleRetryLocked() {
    if (bypass_) {
        // 旁路 shadow 观测：按刷新间隔轮询，不计 retry（§10.1）
        next_query_at_ = std::chrono::steady_clock::now() + config_.refresh_interval_ms;
        timer_cv_.notify_all();
        return;
    }
    // 每次真正重试 attempt 开始时 retry_count、total_attempts 递增；
    // 初次立即查询不计 retry（首次失败时 total_attempts==1，不递增）
    if (total_attempts_ > 1) {
        retry_count_++;
    }
    const auto backoff = calculateBackoffLocked();
    next_query_at_ = std::chrono::steady_clock::now() + backoff;
    timer_cv_.notify_all();
}

void TrustedTimeGate::resetRetryLocked() {
    retry_count_ = 0;
}

void TrustedTimeGate::scheduleRefreshLocked() {
    // 成功态主动查询间隔 ≤ refresh_interval_ms；按剩余租约与查询超时预算提前刷新
    const auto now = std::chrono::steady_clock::now();
    auto refresh_at = now + config_.refresh_interval_ms;
    auto early_at = lease_deadline_ - config_.query_timeout_ms;
    if (early_at < refresh_at) refresh_at = early_at;
    if (refresh_at < now + Duration(1)) refresh_at = now + Duration(1);
    next_query_at_ = refresh_at;
    timer_cv_.notify_all();
}

void TrustedTimeGate::timerLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (timer_running_) {
        const auto now = std::chrono::steady_clock::now();
        // 租约到期撤销优先且幂等（§4.2/§6.1）
        if (!bypass_ && ready_ && has_sample_ && lease_deadline_ <= now) {
            lock.unlock();
            revoke(TimeGateReason::SampleExpired);
            lock.lock();
            continue;
        }
        // 到达查询时间（首次/刷新/重试）
        if (started_ && !query_inflight_ && next_query_at_ <= now) {
            lock.unlock();
            doQuery();
            lock.lock();
            continue;
        }

        // 计算下次唤醒
        auto wake = next_query_at_;
        if (!bypass_ && ready_ && has_sample_ && lease_deadline_ < wake) {
            wake = lease_deadline_;
        }
        if (!timer_running_) break;
        timer_cv_.wait_until(lock, wake, [this] {
            return !timer_running_;
        });
    }
}

} // namespace mqtt
} // namespace tbox
