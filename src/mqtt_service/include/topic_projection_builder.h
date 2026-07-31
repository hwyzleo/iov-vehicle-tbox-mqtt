#pragma once

// TopicProjectionBuilder (CR-009 §2, §5)
//
// 将 owner 快照与身份快照组合为不可变运行时投影。
// 在单写线程或串行 executor 中构建新表，完成后原子发布 (CR-009 §12)。
//
// 构建流程 (CR-009 §5):
// 1. 获取 registry 的不可变 owner 快照
// 2. 获取 resolver 的当前身份（ecu_uid + identity_generation）
// 3. 对每个 SubscriptionItem:
//    - 若身份有效: 展开模板 -> ResolvedRoute(state=ACTIVE, identity_generation=current)
//    - 若身份无效: ResolvedRoute(state=UNRESOLVED, expanded_topic="")
// 4. 构建 EffectiveRouteTable 正向+反向索引
// 5. 返回不可变 shared_ptr<const EffectiveRouteTable>
//
// 相同 owner/generation/digest 幂等；同 generation 不同 digest 按 MQTT-1010 拒绝
// (由 OwnerSnapshotRegistry 处理，builder 只消费已接受快照)。

#include "mqtt_types.h"
#include "effective_route_table.h"
#include "owner_snapshot_registry.h"
#include "topic_template_resolver.h"
#include <memory>
#include <string>

namespace tbox {
namespace mqtt {

class TopicProjectionBuilder {
public:
    TopicProjectionBuilder();
    ~TopicProjectionBuilder();

    /// 从 registry + resolver 构建不可变投影 (CR-009 §5)
    /// @param registry owner 快照注册表（读取已接受快照）
    /// @param resolver 模板解析器（含当前身份）
    /// @return 不可变 EffectiveRouteTable；身份无效时路由标记为 UNRESOLVED
    std::shared_ptr<const EffectiveRouteTable> build(
        const OwnerSnapshotRegistry& registry,
        const TopicTemplateResolver& resolver);

    /// 便捷方法：直接从快照 map + resolver 构建
    std::shared_ptr<const EffectiveRouteTable> build(
        const std::map<std::string, OwnerSubscriptionSnapshot>& snapshots,
        const TopicTemplateResolver& resolver);
};

} // namespace mqtt
} // namespace tbox
