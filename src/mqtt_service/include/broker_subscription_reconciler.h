#pragma once

// BrokerSubscriptionReconciler (CR-007 §2, §8)
//
// CONNACK 后根据 desired 投影、session_present 和 AppliedSubscriptionManifest 执行
// SUBSCRIBE/UNSUBSCRIBE 并跟踪 ACK。计算 desired 与 applied 差异，发送协议命令并跟踪
// SUBACK/UNSUBACK (CR-007 §8)。
//
// session_present=false：清空 active，全量 SUBSCRIBE。
// session_present=true：校验 manifest/session identity，diff 新增/删除/QoS 变化；
//   manifest 缺失或损坏时保守全量重发幂等 SUBSCRIBE (CR-007 §8.2)。
//
// 旧 generation 的迟到 ACK 不得激活新 generation (CR-007 §9)：每次 reconcile 递增
// generation，ACK 携带 topic，仅当 topic 属于当前 generation 时激活。

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include "effective_subscription_table.h"
#include "applied_subscription_manifest.h"
#include <chrono>
#include <map>
#include <mutex>
#include <set>
#include <string>

namespace tbox {
namespace mqtt {

/// Broker 协议操作接口（MqttClientManager 实现）
/// 内部维护 mid->topic 映射，在 SUBACK/UNSUBACK 时回调 reconciler。
class BrokerOperations {
public:
    virtual ~BrokerOperations() = default;
    /// 发起订阅，返回是否已提交协议层（不代表 SUBACK）
    virtual bool brokerSubscribe(const std::string& topic, uint8_t qos) = 0;
    /// 发起取消订阅
    virtual bool brokerUnsubscribe(const std::string& topic) = 0;
};

struct ReconcileResult {
    int subscribe_count = 0;
    int unsubscribe_count = 0;
    bool session_present = false;
    bool manifest_valid = false;  // identity 是否匹配
};

class BrokerSubscriptionReconciler {
public:
    BrokerSubscriptionReconciler();
    ~BrokerSubscriptionReconciler();

    void setBrokerOperations(BrokerOperations* ops);
    void setManifestStore(AppliedSubscriptionManifestStore* manifest);
    void setReconcileTimeout(Duration timeout);
    void setSubackRetryMax(uint32_t max);

    /// CONNACK 后收敛订阅 (CR-007 §8)
    /// @param desired 当前下行有效订阅投影（来自 EffectiveSubscriptionTable）
    /// @param session_present CONNACK 返回的 session_present
    /// @param client_id_hash 当前 client_id 摘要（manifest identity 校验）
    /// @param identity_generation 当前身份 generation
    ReconcileResult reconcile(
        std::shared_ptr<const std::map<std::string, EffectiveSubscription>> desired,
        bool session_present,
        const std::string& client_id_hash,
        uint64_t identity_generation);

    /// SUBACK 回调（topic 关联）
    void onSubAck(const std::string& topic, bool success);
    /// UNSUBACK 回调
    void onUnsubAck(const std::string& topic, bool success);

    /// 超时重试检查（由 MqttService 定时驱动）
    void tick();

    /// 当前 mandatory 全部 ACTIVE（SUBSCRIPTIONS_READY 条件之一）
    bool isConverged() const;

    uint32_t mandatoryActive() const;
    uint32_t mandatoryTotal() const;

    /// CR-015: 指定 capability scope 的下行 topic 是否全部 ACTIVE
    /// 无该 scope 的 topic 时返回 true（空集视为就绪；但 enabled/contract 由上层判定）。
    bool capabilityConverged(const std::string& readiness_scope) const;

    /// CR-015: 指定 capability scope 是否存在 desired 下行 topic
    bool hasCapability(const std::string& readiness_scope) const;

    /// CR-015: 指定 capability scope 是否存在 SUBACK 失败/拒绝的 topic
    bool capabilityRejected(const std::string& readiness_scope) const;

    /// 当前 ACTIVE 的 topic->qos（供 manifest 持久化）
    std::map<std::string, uint8_t> activeTopics() const;

    /// 清空内部状态（断开/重启）
    void clear();

private:
    struct TopicState {
        uint8_t qos = 0;
        bool mandatory = false;
        BrokerSubscriptionState state = BrokerSubscriptionState::PENDING;
        uint32_t retries = 0;
        uint64_t reconcile_generation = 0;
        std::chrono::steady_clock::time_point last_attempt;
        // CR-015: capability scope（空=全局）
        std::string readiness_scope;
    };

    void sendSubscribe(const std::string& topic, uint8_t qos, uint64_t gen);
    void sendUnsubscribe(const std::string& topic, uint64_t gen);

    mutable std::mutex mutex_;
    BrokerOperations* broker_ops_ = nullptr;
    AppliedSubscriptionManifestStore* manifest_ = nullptr;
    Duration reconcile_timeout_{10000};
    uint32_t suback_retry_max_ = 3;

    std::map<std::string, TopicState> topics_;
    std::set<std::string> mandatory_topics_;  // 当前 desired 的全局 mandatory 集合
    std::set<std::string> capability_scopes_;  // CR-015: desired 中出现的 capability scope
    uint64_t reconcile_generation_ = 0;
};

} // namespace mqtt
} // namespace tbox
