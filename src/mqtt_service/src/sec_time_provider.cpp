#include "sec_time_provider.h"

#include "tbox/sec/client.h"
#include "tbox/sec/errors.h"
#include "tbox/sec/time.h"

#include <chrono>

namespace tbox {
namespace mqtt {

namespace {

TimeTrustState stringToTrustState(const std::string& s) {
    if (s == "trusted") return TimeTrustState::Trusted;
    if (s == "untrusted") return TimeTrustState::Untrusted;
    return TimeTrustState::Unknown;
}

TimeSource stringToSource(const std::string& s) {
    if (s == "platform_sync") return TimeSource::PlatformSync;
    if (s == "hardware_rtc") return TimeSource::HardwareRtc;
    if (s == "fake_test") return TimeSource::FakeTest;
    if (s == "contract_unsupported") return TimeSource::ContractUnsupported;
    return TimeSource::None;
}

/// 业务状态 → 门禁 reason（Unknown/Untrusted 是可查询业务状态，不是传输错误）。
TimeGateReason trustStateToReason(TimeTrustState st) {
    switch (st) {
        case TimeTrustState::Trusted: return TimeGateReason::None;
        case TimeTrustState::Untrusted: return TimeGateReason::Untrusted;
        case TimeTrustState::Unknown: return TimeGateReason::Unknown;
    }
    return TimeGateReason::Unknown;
}

}  // namespace

SecTimeProvider::SecTimeProvider(std::string sec_socket_path)
    : sec_socket_path_(std::move(sec_socket_path)),
      client_(std::make_shared<tbox::sec::SecClient>(sec_socket_path_)) {
    // 连接失败不阻塞：query() 映射为 ProviderUnavailable，门禁进入 TIME_WAIT；
    // SEC transport 恢复后由下一次查询自然重连。
    client_->connect();
}

SecTimeProvider::~SecTimeProvider() {
    client_->disconnect();
}

TrustedTimeSample SecTimeProvider::mapEnvelope(
    const tbox::sec::TrustedTimeEnvelope& e) {
    TrustedTimeSample s;
    s.trust_state = stringToTrustState(e.trust_state);
    s.source = stringToSource(e.source);
    s.reason = trustStateToReason(s.trust_state);
    if (e.utc_valid) {
        s.utc_now = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(e.utc_epoch_ms));
    }
    s.freshness_age = std::chrono::milliseconds(e.freshness_age_ms);
    s.uncertainty = std::chrono::milliseconds(e.uncertainty_ms);
    s.source_epoch = e.source_epoch;
    s.sample_sequence = e.sec_time_event_sequence;
    s.provider_epoch = std::to_string(e.sec_daemon_epoch);
    s.remaining_validity = std::chrono::milliseconds(e.remaining_validity_ms);
    s.clock_aligned = e.system_clock_aligned;
    s.contract_version = e.contract_version;
    // 结构完整可判定：envelope 有效 + 契约完整 + 无传输错误分类
    s.valid = e.valid && e.contract_complete && e.error_class.empty();
    return s;
}

TimeQueryResult SecTimeProvider::query() {
    TimeQueryResult r;
    tbox::sec::TrustedTimeEnvelope env;
    tbox::sec::ErrorCode rc;
    {
        std::lock_guard<std::mutex> lk(mu_);
        rc = client_->get_trusted_time(have_token_ ? last_token_ : 0, env);
    }
    if (rc == tbox::sec::ErrorCode::SUCCESS) {
        r.sample = mapEnvelope(env);
        if (!r.sample.valid) {
            // envelope 畸形/契约不完整：整体拒绝，不按 Trusted 默认（§3.2）
            r.ok = false;
            r.reason = TimeGateReason::SampleMalformed;
            return r;
        }
        r.ok = true;
        r.reason = TimeGateReason::None;
        std::lock_guard<std::mutex> lk(mu_);
        last_token_ = env.sec_daemon_epoch;
        have_token_ = true;
        return r;
    }

    // ---- 传输/契约层错误：独立表达，不伪装为业务状态 ----
    r.ok = false;
    switch (rc) {
        case tbox::sec::ErrorCode::TRUSTED_TIME_UNAVAILABLE:
        case tbox::sec::ErrorCode::CONNECTION_FAILED:
            r.reason = TimeGateReason::ProviderUnavailable;
            break;
        case tbox::sec::ErrorCode::TRUSTED_TIME_TIMEOUT:
            r.reason = TimeGateReason::QueryTimeout;
            break;
        case tbox::sec::ErrorCode::TRUSTED_TIME_UNSUPPORTED:
            r.reason = TimeGateReason::ContractUnsupported;
            break;
        case tbox::sec::ErrorCode::TRUSTED_TIME_MALFORMED:
            r.reason = TimeGateReason::SampleMalformed;
            break;
        default:
            r.reason = TimeGateReason::ProviderUnavailable;
            break;
    }
    // 迟到响应（SEC 重启/停机）：旧 token 失效，下次以 token=0 重新建立基线
    if (env.error_class == "stale") {
        std::lock_guard<std::mutex> lk(mu_);
        have_token_ = false;
    }
    return r;
}

TimeProviderInfo SecTimeProvider::info() const {
    TimeProviderInfo i;
    i.is_test_fake = false;
    std::lock_guard<std::mutex> lk(mu_);
    i.epoch = have_token_ ? std::to_string(last_token_)
                          : std::string("sec-unbound");
    return i;
}

std::shared_ptr<ITrustedTimeProvider> createSecTimeProvider(
    const std::string& sec_socket_path) {
    return std::make_shared<SecTimeProvider>(sec_socket_path);
}

}  // namespace mqtt
}  // namespace tbox
