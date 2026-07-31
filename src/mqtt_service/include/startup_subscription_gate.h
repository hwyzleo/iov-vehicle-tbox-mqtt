#pragma once

// StartupSubscriptionGate (CR-007 §2, §6)
//
// 等待配置的 required owner 提交 registration_complete=true 快照；超时允许降级连接
// (CONNECTED_DEGRADED) 但不得发布 Cloud Ready。超时后继续接受迟到快照。
//
// 门闩满足条件 (CR-007 §6)：所有 required owner 存在最新 registration_complete=true 快照。
// 超时不阻塞本地 IPC 或进程生命周期 (REQ-CR-007 §4.2)。

#include "mqtt_types.h"
#include <chrono>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace tbox {
namespace mqtt {

class StartupSubscriptionGate {
public:
    using Clock = std::chrono::steady_clock;

    /// 配置 required owner 与超时 (CR-007 §6)
    void configure(const std::vector<std::string>& required_owners, Duration timeout);

    /// 开始等待计时（幂等：重复调用不重置 start_time_）
    void startWait();

    /// 停止等待并重置状态（gate 解决后调用，重连重入 SUBSCRIPTION_WAIT 时重新计时）
    void stopWait();

    /// 快照接受通知：更新 owner 完成状态 (CR-007 §6)
    /// @param registration_complete 该 owner 最新快照是否 registration_complete
    void onSnapshotAccepted(const std::string& owner, bool registration_complete);

    /// 所有 required owner 是否已提交 registration_complete 快照
    bool isSatisfied() const;

    /// 等待是否超时（未满足且超过 timeout）
    bool isTimedOut() const;

    /// 尚未完成的 required owner 列表
    std::vector<std::string> missingOwners() const;

    /// 已等待时长
    Duration elapsed() const;

    /// 重置（新连接周期）
    void reset();

private:
    mutable std::mutex mutex_;
    std::vector<std::string> required_owners_;
    Duration timeout_{5000};
    std::set<std::string> completed_owners_;
    Clock::time_point start_time_;
    bool waiting_ = false;
};

} // namespace mqtt
} // namespace tbox
