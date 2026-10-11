#pragma once

// SubscriptionCatalog (DSN-CR-015 §3, §18.2)
//
// 编译期受控 route 目录：冻结 catalog-governed route 的模板、方向、QoS、target、
// readiness scope 与 criticality。调用方（TSP 快照）只引用 route_id，不得覆盖这些属性。
//
// 本期只冻结 keyprov 两条 route：
//   - keyprov.uplink   -> vehicle/{ecu_uid}/up/keyprov   (UP,   QoS1, target=null)
//   - keyprov.downlink -> vehicle/{ecu_uid}/down/keyprov (DOWN, QoS1, target=tsp.keyprov)
// 两条均为 CAPABILITY_MANDATORY(security.keyprov)，上行不持久化。
//
// FOTA 等既有 route 不进入 catalog，继续由 owner 快照自带模板（保持 CR-007/009 行为）。

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include <map>
#include <optional>
#include <string>
#include <utility>

namespace tbox {
namespace mqtt {

/// catalog 冻结的 route 定义（调用方不可覆盖）
struct CatalogEntry {
    std::string owner;
    std::string route_id;
    SnapshotDirection direction = SnapshotDirection::UP;
    std::string topic_template;
    uint8_t qos = 1;
    std::string target;                 // 空 = 无 target（纯上行）
    std::string readiness_scope;        // 空 = 全局；非空 = capability scope
    SubscriptionCriticality criticality = SubscriptionCriticality::OPTIONAL;
    bool persist_offline = true;        // 是否允许写入持久离线队列

    bool capabilityGated() const { return !readiness_scope.empty(); }
};

/// 编译期受控 SubscriptionCatalog（进程内单例，只读）
class SubscriptionCatalog {
public:
    static const SubscriptionCatalog& instance();

    /// 按 owner + route_id 查找 catalog 定义；不存在返回 nullptr
    const CatalogEntry* lookup(const std::string& owner,
                               const std::string& route_id) const;

    /// 该 (owner, route_id) 是否由 catalog 冻结
    bool isCatalogRoute(const std::string& owner, const std::string& route_id) const;

    /// readiness scope -> capability 名。本期 capability 名等于 scope（security.keyprov）。
    static std::string capabilityForScope(const std::string& scope) { return scope; }

    const std::map<std::pair<std::string, std::string>, CatalogEntry>& entries() const {
        return entries_;
    }

private:
    SubscriptionCatalog();

    std::map<std::pair<std::string, std::string>, CatalogEntry> entries_;
};

} // namespace mqtt
} // namespace tbox
