#pragma once

// CloudReadinessGate (CR-007 §2, §10; CR-014 §3, §4.2, §12.5)
//
// Cloud Ready 权威状态对象：组合连接状态、owner generation、Mandatory Subscription
// 激活状态与本次 TLS 会话身份。仅在连接可用且当前 Mandatory Subscription 全部 ACTIVE
// 后进入 READY (CR-007 §10)。
//
// CLOUD_READY = CONNECTED && SUBSCRIPTIONS_READY。
// Cloud Ready 退出条件 (CR-007 §10)：连接断开、required owner generation 变化、
// mandatory route 失效、SUBACK 失败、身份 generation 变化或 Broker Session 丢失。
//
// CR-014：本对象同时是 Cloud Readiness 快照与事件的唯一权威来源。
// - 查询 (getReadiness) 与事件 (callback) 读取同一份已提交快照；
// - 状态更新顺序固定为：原子提交新快照 -> 递增 event_sequence -> 投递事件；
// - TLS context 构建时经 setTlsSessionIdentity 固化不可复用的 cert_session_generation
//   与 leaf 证书身份；断链/凭据变更经 revokeSession 先提交并发布旧 generation 的
//   cloud_ready=false（仍携带旧身份），随后才销毁旧 TLS session/context。

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
    /// 变更回调：每次原子提交新快照（cloud_ready、connection/cert generation、
    /// credential、fingerprint、reason 任一变化）后投递完整快照。回调在锁外调用。
    using ChangeCallback = std::function<void(const CloudReadinessSnapshot&)>;

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

    /// 固化本次 TLS context 实际装载的 leaf 证书身份 (DSN-CR-014 §4.1)
    /// 新会话重置 subscriptions_ready，禁止旧 SUBACK 在未重新收敛前激活新状态。
    void setTlsSessionIdentity(const TlsSessionIdentity& identity);

    /// 撤销当前会话 (DSN-CR-014 §4.2)：先原子提交并发布旧 generation 的
    /// cloud_ready=false（快照仍携带被撤销的旧身份），供调用方精确失效。
    void revokeSession(const std::string& reason);

    bool isCloudReady() const;

    /// 返回与事件一致的最新已提交快照（含 identity 与 event_sequence）。
    CloudReadinessSnapshot getReadiness() const;

private:
    bool evaluateLocked(const std::string& reason, CloudReadinessSnapshot& out);
    CloudReadinessSnapshot buildSnapshotLocked(bool ready, const std::string& reason) const;
    static bool snapshotChanged(const CloudReadinessSnapshot& a,
                                const CloudReadinessSnapshot& b);

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
    // CR-014: 本次 TLS 会话固化的只读身份
    TlsSessionIdentity identity_;
    // CR-014: daemon 生命周期内单调递增事件序列
    uint64_t event_sequence_ = 0;
    bool has_committed_ = false;
    CloudReadinessSnapshot committed_;
};

} // namespace mqtt
} // namespace tbox
