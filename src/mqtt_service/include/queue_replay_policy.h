#pragma once

// TBOX-MQTT-DSN-CR-018 §7.3 / REQ-CR-019 §5: 历史队列项恢复判定策略
//
// 纯决策函数（无副作用，可单测）：
//   - enforced=false（旁路模式）→ 保持旧行为，全部 kReplay；
//   - 已终结 → 不重放；
//   - 旧 schema / 旁路期 / 来源不可信 → kProvenanceUnknown（隔离保留，不自动重放）；
//   - TRUSTED 且外置截止 ≤ 可信现在保守最早时刻 → kExpiredTerminal（终结，不重放）；
//   - 截止位于当前不确定区间 → kQuarantined（隔离，不盲发）；
//   - TRUSTED 未过期 → kReplay（发送前再过门禁）。

#include "mqtt_types.h"
#include "trusted_time_types.h"

namespace tbox {
namespace mqtt {

enum class QueueReplayDecision : uint8_t {
    kReplay,             ///< 按原 msg_id/QoS 顺序重放
    kQuarantined,        ///< 隔离待处理，不盲发
    kExpiredTerminal,    ///< 传输期限终结，不重放
    kProvenanceUnknown   ///< 来源不可信终结（旧格式/旁路/无可信标记）
};

/// 判定矩阵（§7.3）。纯函数：不修改 msg、不落盘、不记录。
/// @param current_admission 发送时刻门禁判定（携带可信当前保守 UTC 区间）
inline QueueReplayDecision evaluateQueueReplay(
        const OutboundMessage& msg,
        const TimeAdmission& current_admission,
        bool enforced) {
    if (!enforced) {
        // 旁路模式：旧行为（REQ-CR-019 §5 的盲发隔离仅在强制模式适用）
        return QueueReplayDecision::kReplay;
    }
    // 旧 schema（< 当前版本）→ 来源不可信终结，不自动重放
    if (msg.queue_schema_version < kQueueSchemaVersionCurrent) {
        return QueueReplayDecision::kProvenanceUnknown;
    }
    // 旁路期/未知 provenance → 隔离保留，不自动重放
    if (msg.time_provenance != TimeProvenance::TRUSTED) {
        return QueueReplayDecision::kProvenanceUnknown;
    }
    // TRUSTED + 有外置绝对截止
    if (msg.external_deadline_ms > 0 && current_admission.utc_earliest_ms > 0) {
        const int64_t deadline = msg.external_deadline_ms;
        // 可信现在的保守最早时刻 ≥ 截止 → 终结 expired（expire_at 等于现在视为到期）
        if (current_admission.utc_earliest_ms >= deadline) {
            return QueueReplayDecision::kExpiredTerminal;
        }
        // 期限位于当前不确定区间 → 隔离，不盲发
        if (current_admission.utc_latest_ms >= deadline) {
            return QueueReplayDecision::kQuarantined;
        }
    }
    // 无 TTL（需显式路由策略）且可信受理/身份有效 → 重放
    return QueueReplayDecision::kReplay;
}

} // namespace mqtt
} // namespace tbox
