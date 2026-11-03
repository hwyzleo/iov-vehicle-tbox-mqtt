#pragma once

// TBOX-MQTT-DSN-CR-018：可信时间门禁内部类型（daemon 内部）
//
// 边界（§3.1 路线 A）：
//   - MQTT 只消费 SEC 唯一 CompositeTrustedTimeProvider 的判定，不在 MQTT 复制
//     平台/RTC 信任算法；本文件类型仅用于承载、校验 SEC 契约结果和管理消费租约。
//   - 不 include SEC 私有 trusted_time.h；本文件镜像 SEC 语义，字段/wire 编码
//     待 SEC 配对 CR（DEP-TIME-01）冻结后对齐，当前不视为已存在接口。
//   - utc_now 为明确 epoch 毫秒；duration 有符号、非负，溢出/越界/未知枚举整体拒绝，
//     不按 Trusted 默认（§3.2）。

#include <chrono>
#include <cstdint>
#include <string>
#include <functional>

#include "mqtt_types.h"  // Duration

namespace tbox {
namespace mqtt {

// ============================================================
// 时间信任状态（MQTT 内部镜像，与 SEC wire 契约待配对 CR 冻结）
// ============================================================

/// 信任状态。强制模式门禁只接受 Trusted。
enum class TimeTrustState {
    Trusted,
    Untrusted,
    Unknown
};

/// 时间来源（非秘密）。
enum class TimeSource {
    PlatformSync,
    HardwareRtc,
    FakeTest,
    None,
    ContractUnsupported  // SEC 契约不可用（配对 CR 未落地）
};

/// 稳定门禁 reason（映射到 include/tbox/mqtt/types.h time_reason::* 常量）。
enum class TimeGateReason {
    None = 0,
    // MQTT-1020：时间信任或执行时钟不足
    Untrusted,
    Unknown,
    SampleExpired,
    ClockNotAligned,
    // MQTT-1021：时间依赖／契约不可用
    ProviderUnavailable,
    QueryTimeout,
    ContractUnsupported,
    SampleMalformed,
    // 开发旁路（不作为门禁通过）
    Bypassed
};

/// 一次可信时间采样（MQTT 内部承载/校验 SEC 契约结果，§3.2）
struct TrustedTimeSample {
    std::chrono::system_clock::time_point utc_now{};
    TimeTrustState trust_state = TimeTrustState::Unknown;
    TimeSource source = TimeSource::None;
    /// 距最近一次同步的龄期；必须由来源侧 steady_clock 计算并给出年龄上界。
    std::chrono::milliseconds freshness_age{0};
    std::chrono::milliseconds uncertainty{0};
    TimeGateReason reason = TimeGateReason::Unknown;
    uint64_t source_epoch = 0;        ///< 仅在同一 provider 生命周期及来源域内解释
    uint64_t sample_sequence = 0;     ///< 样本序列，用于乱序/缺口检测
    std::string provider_epoch;       ///< SEC daemon 生命周期标识（配对契约后为真实值）
    /// 来源给出的剩余可信有效时长（本样本剩余有效期）。
    std::chrono::milliseconds remaining_validity{0};
    /// 系统时钟已受可信源校准且仍在允许误差内（§3.4 执行时钟一致性）。
    bool clock_aligned = false;
    uint32_t contract_version = 0;    ///< 0 = 契约不可用/未知
    bool valid = false;               ///< 样本完整可判定（缺关键字段为 false）
};

/// 查询结果（§3.2：MQTT 不接收/比较跨进程 steady_clock 绝对 time_point）
struct TimeQueryResult {
    bool ok = false;                  ///< true=有样本；false=不可用（reason 说明）
    TrustedTimeSample sample;
    TimeGateReason reason = TimeGateReason::ProviderUnavailable;
};

/// 时间门禁配置（mqtt.trusted_time.*，§9 全部为新增提案键，缺省值见下表）
struct TrustedTimeConfig {
    bool require_trusted_time = true;          ///< production 必须 true；false 仅显式 dev/test
    Duration query_timeout_ms{500};            ///< 50..1000；超时即无新租约，不延长旧租约
    Duration refresh_interval_ms{500};         ///< 100..1000；查询间隔上限，按租约 deadline 提前刷新
    Duration cache_max_age_ms{1000};           ///< 100..1000；运行时实际可用年龄取配置上限与扣除 RTT 后 SEC 剩余租约的较小值
    Duration retry_initial_backoff_ms{1000};   ///< 100..10000，且 ≤ max_backoff_ms
    Duration retry_max_backoff_ms{30000};      ///< initial..60000；仅失败轮询，不延长授权租约
    double retry_multiplier = 2.0;             ///< 1..4，有限数
    double retry_jitter_ratio = 0.2;           ///< 0..0.5，抖动后仍硬限制 backoff

    /// 纯校验（§9：只校验类型/范围/跨字段关系，不连接 SEC/Broker、不读身份、不 bind socket）
    static bool validate(const TrustedTimeConfig& cfg, std::string* err = nullptr);
};

/// 门禁状态快照（readiness/日志/健康用，§8/§11）
struct TimeGateStatus {
    bool require_trusted_time = true;
    bool bypassed = false;                    ///< require=false（开发旁路，非门禁通过）
    bool time_gate_ready = false;             ///< 门禁放行（仅强制模式有意义）
    TimeTrustState trust_state = TimeTrustState::Unknown;
    TimeSource source = TimeSource::None;
    TimeGateReason reason = TimeGateReason::Unknown;
    uint64_t source_epoch = 0;
    std::chrono::milliseconds lease_remaining{0};
    std::chrono::milliseconds freshness_ms{0};
    std::chrono::milliseconds uncertainty_ms{0};
    bool clock_aligned = false;
    uint64_t time_gate_generation = 0;
    uint64_t retry_count = 0;
    std::chrono::milliseconds backoff_ms{0};
    bool provider_is_test_fake = false;       ///< 诊断：当前 provider 是否测试 fake
};

/// 受理/发送门禁判定结果（§7.1 双阶段；§7.2 携带受理 provenance）
struct TimeAdmission {
    bool allowed = false;
    TimeGateReason reason = TimeGateReason::Unknown;
    uint64_t time_gate_generation = 0;        ///< 通过时对应的 gate generation
    bool bypassed = false;
    // ---- §7.2 受理 provenance（allowed 时填充，供队列元数据）----
    TimeProvenance provenance = TimeProvenance::UNKNOWN;
    std::string source;                       ///< 非秘密来源标识
    uint64_t source_epoch = 0;
    int64_t utc_earliest_ms = 0;              ///< 保守 UTC 区间（样本 utc ± uncertainty）
    int64_t utc_latest_ms = 0;
};

/// 提供方标识与测试性（生产工厂拒绝 isTestFake）
struct TimeProviderInfo {
    std::string epoch;      ///< provider/SEC daemon 生命周期标识
    bool is_test_fake = false;
};

/// 稳定字符串化（日志/readiness reason 字段，映射到 time_reason::* 常量）
inline const char* timeGateReasonToString(TimeGateReason r) {
    switch (r) {
        case TimeGateReason::None:               return "";
        case TimeGateReason::Untrusted:          return "time_untrusted";
        case TimeGateReason::Unknown:            return "time_unknown";
        case TimeGateReason::SampleExpired:      return "time_sample_expired";
        case TimeGateReason::ClockNotAligned:    return "time_clock_not_aligned";
        case TimeGateReason::ProviderUnavailable: return "time_provider_unavailable";
        case TimeGateReason::QueryTimeout:       return "time_query_timeout";
        case TimeGateReason::ContractUnsupported: return "time_contract_unsupported";
        case TimeGateReason::SampleMalformed:    return "time_sample_malformed";
        case TimeGateReason::Bypassed:           return "time_gate_bypassed";
        default:                                 return "time_unknown";
    }
}

/// 稳定字符串化（readiness 快照 time_trust_state 字段）
inline const char* timeTrustStateToString(TimeTrustState s) {
    switch (s) {
        case TimeTrustState::Trusted:   return "Trusted";
        case TimeTrustState::Untrusted: return "Untrusted";
        case TimeTrustState::Unknown:   return "Unknown";
        default:                        return "Unknown";
    }
}

/// 稳定字符串化（readiness 快照 time_source 字段）
inline const char* timeSourceToString(TimeSource s) {
    switch (s) {
        case TimeSource::PlatformSync:        return "PlatformSync";
        case TimeSource::HardwareRtc:         return "HardwareRtc";
        case TimeSource::FakeTest:            return "FakeTest";
        case TimeSource::None:                return "None";
        case TimeSource::ContractUnsupported: return "ContractUnsupported";
        default:                              return "None";
    }
}

} // namespace mqtt
} // namespace tbox
