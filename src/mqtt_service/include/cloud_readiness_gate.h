#pragma once

// CloudReadinessGate (CR-007 §2, §10)
//
// 组合连接状态、owner generation 和 Mandatory Subscription 激活状态。仅在连接可用且
// 当前 Mandatory Subscription 全部 ACTIVE 后进入 READY (CR-007 §10)。
//
// CLOUD_READY = CONNECTED && SUBSCRIPTIONS_READY。
// Cloud Ready 退出条件 (CR-007 §10)：连接断开、required owner generation 变化、
// mandatory route 失效、SUBACK 失败、身份 generation 变化或 Broker Session 丢失。
//
// Ready 发布本身失败时保持本地状态但记录 outcome (CR-007 §10)。

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include <functional>
#include <map>
#include <mutex>
#include <string>

namespace tbox {
namespace mqtt {

class CloudReadinessGate {
public:
    using ChangeCallback = std::function<void(
        bool ready, uint32_t mandatory_active, uint32_t mandatory_total,
        const std::string& reason_code)>;

    void setCallback(ChangeCallback cb);

    /// 连接状态更新（含 SUBSCRIPTION_WAIT/SUBSCRIBING/CLOUD_READY/DEGRADED 等）
    void onConnectionState(ConnectionState state, uint64_t connection_generation);

    /// 设置 owner 最新已接受 generation（required owner 变化时触发重评估）
    void setOwnerGenerations(const std::map<std::string, uint64_t>& gens);

    /// Mandatory 订阅收敛状态更新
    /// @param ready 当前 mandatory 是否全部 ACTIVE
    /// @param mandatory_active 已 ACTIVE 的 mandatory 数
    /// @param mandatory_total mandatory 总数
    void onSubscriptionsReady(bool ready, uint32_t mandatory_active, uint32_t mandatory_total);

    /// Mandatory 失效（SUBACK 失败/超时/会话丢失）
    void onMandatoryFailed(const std::string& reason);

    /// 标记降级（门闩超时，连接在但不得 Cloud Ready）
    void setDegraded(bool degraded, const std::string& reason);

    bool isCloudReady() const;

    /// 组装诊断快照
    SubscriptionReadiness getReadiness() const;

private:
    void evaluate(const std::string& reason);

    static bool isConnectedState(ConnectionState s) {
        return s == ConnectionState::CONNECTED ||
               s == ConnectionState::SUBSCRIBING ||
               s == ConnectionState::CLOUD_READY ||
               s == ConnectionState::CONNECTED_DEGRADED;
    }

    mutable std::mutex mutex_;
    ChangeCallback callback_;
    ConnectionState connection_state_ = ConnectionState::INIT;
    uint64_t connection_generation_ = 0;
    std::map<std::string, uint64_t> owner_generations_;
    bool subscriptions_ready_ = false;
    uint32_t mandatory_active_ = 0;
    uint32_t mandatory_total_ = 0;
    bool degraded_ = false;
    bool cloud_ready_ = false;
    std::string last_reason_;
};

} // namespace mqtt
} // namespace tbox
