#pragma once

// EffectiveSubscriptionTable (CR-007 §2, §3, §5)
//
// 从 OwnerSnapshotRegistry 全量重算有效订阅投影：按展开 Topic 聚合 owner 引用、
// target、有效 QoS（max）与 mandatory（OR）。最后一个 owner 引用消失才不再出现该 Topic
// （由 reconciler 产生 UNSUBSCRIBE）。
//
// Topic 模板经同一 ProvisionIdentitySnapshot 的 ecu_uid 展开；不得使用 VIN/SN/client_id
// 兜底 (CR-007 §5)。
//
// 线程模型：不可变快照 + atomic shared_ptr 切换（CR-007 §9）。reconciler 读取当前
// 不可变快照计算 diff；慢 target 使用既有独立有界队列隔离。

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include "owner_snapshot_registry.h"
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace tbox {
namespace mqtt {

/// 下行分发目标（标识哪个 owner 的哪个 target）
struct RouteTarget {
    std::string owner;
    std::string target_service;
    std::string route_id;
};

/// Broker 订阅项协议状态（由 BrokerSubscriptionReconciler 维护，table 重算时置 PENDING）
enum class BrokerSubscriptionState : uint8_t {
    PENDING,
    SUBSCRIBING,
    ACTIVE,
    FAILED,
    UNSUBSCRIBING
};

/// 有效订阅（聚合后，按展开 Topic 索引）
struct EffectiveSubscription {
    std::string expanded_topic;
    std::string topic_template;     // 模板（日志只输出模板，CR-007 §11）
    uint8_t effective_qos = 1;      // 所有 owner 请求的最大值
    bool mandatory = false;         // 各 owner 引用逻辑 OR
    std::set<std::string> owners;   // 引用该 Topic 的 owner 集合
    std::vector<RouteTarget> targets;  // 所有有效 owner 路由（下行 fan-out）
    BrokerSubscriptionState state = BrokerSubscriptionState::PENDING;
};

/// 上行路由条目（供 RouteBridge 注册，发布用，不产生 Broker SUBSCRIBE）
struct UpstreamRoute {
    std::string internal_addr;  // route_id 或 target
    std::string mqtt_topic;     // 展开后 Topic
    uint8_t qos = 1;
};

class EffectiveSubscriptionTable {
public:
    EffectiveSubscriptionTable();
    ~EffectiveSubscriptionTable();

    /// 设置当前 ecu_uid（身份变化时更新，CR-007 §5; CR-006 §11.3）
    void setEcuUid(const std::string& ecu_uid);

    /// 从 registry 全量重算有效订阅投影 (CR-007 §5)
    /// 返回新的下行不可变快照；同时内部 atomic 切换。
    std::shared_ptr<const std::map<std::string, EffectiveSubscription>> recompute(
        const OwnerSnapshotRegistry& registry);

    /// 获取当前下行有效订阅不可变快照（线程安全）
    std::shared_ptr<const std::map<std::string, EffectiveSubscription>> downstreamSnapshot() const;

    /// 获取当前上行路由列表（线程安全，供 RouteBridge 注册）
    std::vector<UpstreamRoute> upstreamRoutes() const;

    /// 当前 mandatory 下行项数量
    uint32_t mandatoryCount() const;

    /// 清空投影（进程重启语义）
    void clear();

private:
    // 展开模板中的 {ecu_uid} 占位
    std::string expandTopic(const std::string& topic_template) const;

    std::string ecu_uid_;
    mutable std::mutex mutex_;
    std::shared_ptr<const std::map<std::string, EffectiveSubscription>> downstream_;
    std::shared_ptr<const std::vector<UpstreamRoute>> upstream_;
};

} // namespace mqtt
} // namespace tbox
