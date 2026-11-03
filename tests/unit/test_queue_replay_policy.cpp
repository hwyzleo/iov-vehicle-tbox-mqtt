#include <gtest/gtest.h>
#include "queue_replay_policy.h"

using namespace tbox::mqtt;

namespace {

OutboundMessage makeMsg(TimeProvenance p = TimeProvenance::TRUSTED,
                        uint32_t schema = kQueueSchemaVersionCurrent,
                        int64_t deadline = 0) {
    OutboundMessage m;
    m.msg_id = "m1";
    m.queue_schema_version = schema;
    m.time_provenance = p;
    m.external_deadline_ms = deadline;
    m.utc_earliest_ms = 1000000;
    m.utc_latest_ms = 1002000;
    return m;
}

TimeAdmission makeAdmission(int64_t earliest, int64_t latest) {
    TimeAdmission a;
    a.allowed = true;
    a.provenance = TimeProvenance::TRUSTED;
    a.utc_earliest_ms = earliest;
    a.utc_latest_ms = latest;
    return a;
}

} // namespace

// 旁路模式：全部重放（旧行为，REQ-CR-019 §5）
TEST(QueueReplayPolicyTest, BypassReplaysAll) {
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::BYPASS),
                                  makeAdmission(1000000, 1002000), false),
              QueueReplayDecision::kReplay);
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::UNKNOWN),
                                  makeAdmission(1000000, 1002000), false),
              QueueReplayDecision::kReplay);
}

// 旧 schema → 来源不可信终结，不自动重放（§7.3）
TEST(QueueReplayPolicyTest, OldSchemaQuarantined) {
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::TRUSTED, 0),
                                  makeAdmission(1000000, 1002000), true),
              QueueReplayDecision::kProvenanceUnknown);
}

// 旁路期受理项：强制模式隔离不盲发（REQ-CR-019 §4）
TEST(QueueReplayPolicyTest, BypassProvenanceQuarantinedInEnforced) {
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::BYPASS),
                                  makeAdmission(1000000, 1002000), true),
              QueueReplayDecision::kProvenanceUnknown);
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::UNKNOWN),
                                  makeAdmission(1000000, 1002000), true),
              QueueReplayDecision::kProvenanceUnknown);
}

// TRUSTED + 截止已过（可信现在保守最早时刻 ≥ 截止）→ 终结 expired
TEST(QueueReplayPolicyTest, TrustedExpiredTerminal) {
    // 受理截止 = 1000000；当前最早 = 1000000 → 等于视为到期
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::TRUSTED,
                                          kQueueSchemaVersionCurrent, 1000000),
                                  makeAdmission(1000000, 1003000), true),
              QueueReplayDecision::kExpiredTerminal);
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::TRUSTED,
                                          kQueueSchemaVersionCurrent, 999000),
                                  makeAdmission(1000000, 1003000), true),
              QueueReplayDecision::kExpiredTerminal);
}

// 截止位于当前不确定区间 → 隔离，不盲发
TEST(QueueReplayPolicyTest, DeadlineInUncertaintyQuarantined) {
    // 截止 1001000 在 [1000000, 1002000) 区间内
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::TRUSTED,
                                          kQueueSchemaVersionCurrent, 1001000),
                                  makeAdmission(1000000, 1002000), true),
              QueueReplayDecision::kQuarantined);
}

// TRUSTED 未过期 → 重放
TEST(QueueReplayPolicyTest, TrustedValidReplays) {
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::TRUSTED,
                                          kQueueSchemaVersionCurrent, 1003000),
                                  makeAdmission(1000000, 1002000), true),
              QueueReplayDecision::kReplay);
    // 无外置截止（显式路由策略）→ 重放
    EXPECT_EQ(evaluateQueueReplay(makeMsg(TimeProvenance::TRUSTED),
                                  makeAdmission(1000000, 1002000), true),
              QueueReplayDecision::kReplay);
}
