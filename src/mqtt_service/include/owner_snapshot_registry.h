#pragma once

// OwnerSnapshotRegistry (CR-007 §2, §4; 对齐 TSP DSN-CR-004 §5)
//
// 校验、原子接受、查询各 owner 的完整版本化订阅快照。业务 Topic 的事实来源仍是
// TSP 等 owner；本组件维护非权威运行时输入，不拥有业务 Topic 语义。
//
// 线程模型：不可变快照 + atomic shared_ptr 切换（单写多读，CR-007 §9）。
// 高 generation 先在影子副本完成全量校验，成功后单指针原子切换；禁止逐条半生效。
//
// 幂等规则 (CR-007 §4)：
// - 相同 owner+generation+digest 返回已有 ACCEPTED 结果。
// - 相同 owner/generation 但 digest 不同 -> REJECTED (MQTT-1010)。
// - 低于当前 generation -> stale，不覆盖；返回当前已接受结果。
// - 高 generation 校验通过 -> 原子替换。
//
// content_digest 由 owner 端规范化生成（如 TSP SnapshotBuilder），MQTT 侧只比对不重算。

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include "topic_template_validator.h"
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace tbox {
namespace mqtt {

class OwnerSnapshotRegistry {
public:
    OwnerSnapshotRegistry();
    ~OwnerSnapshotRegistry();

    /// 原子接受或拒绝完整快照 (CR-007 §4)
    /// ACCEPTED 仅表示本地投影已接受，不等于 Broker SUBACK 或 Cloud Ready。
    ReplaceSnapshotResult replaceSnapshot(const OwnerSubscriptionSnapshot& snapshot);

    /// 查询指定 owner/generation 的接受状态 (CR-007 §4)
    /// 用于响应丢失后的幂等收敛：相同 owner/generation 查询已接受状态。
    SnapshotStatusResult getSnapshotStatus(const std::string& owner,
                                           uint64_t generation) const;

    /// 获取某 owner 最新已接受快照（无则 nullptr）
    std::shared_ptr<const OwnerSubscriptionSnapshot> getLatestSnapshot(
        const std::string& owner) const;

    /// 获取所有 owner 的最新已接受快照（不可变快照，线程安全）
    std::map<std::string, OwnerSubscriptionSnapshot> getAllLatestSnapshots() const;

    /// 某 owner 是否已提交 registration_complete=true 的快照 (CR-007 §6 门闩)
    bool hasRegistrationComplete(const std::string& owner) const;

    /// 清空所有 owner 快照（进程重启语义：内存投影丢失，CR-007 §12.3）
    void clear();

    /// 已接受 owner 数量
    size_t ownerCount() const;

private:
    struct OwnerEntry {
        OwnerSubscriptionSnapshot snapshot;
        std::string reason_code;  // 最近接受原因（诊断用）
    };
    struct State {
        std::map<std::string, OwnerEntry> owners;  // owner -> latest accepted
    };

    struct ValidationResult {
        bool ok = false;
        std::string reason_code;
        std::vector<std::string> rejected_route_ids;
    };
    ValidationResult validate(const OwnerSubscriptionSnapshot& snapshot) const;

    // CR-009 §4: Topic 模板校验器
    TopicTemplateValidator template_validator_;

    mutable std::mutex mutex_;
    std::shared_ptr<const State> state_;
};

} // namespace mqtt
} // namespace tbox
