// TopicProjectionBuilder (CR-009 §2, §5; CR-015 §18.2)
#include "topic_projection_builder.h"
#include "subscription_catalog.h"
#include <utility>

namespace tbox {
namespace mqtt {

TopicProjectionBuilder::TopicProjectionBuilder() = default;
TopicProjectionBuilder::~TopicProjectionBuilder() = default;

std::shared_ptr<const EffectiveRouteTable> TopicProjectionBuilder::build(
        const OwnerSnapshotRegistry& registry,
        const TopicTemplateResolver& resolver) {
    return build(registry.getAllLatestSnapshots(), resolver);
}

std::shared_ptr<const EffectiveRouteTable> TopicProjectionBuilder::build(
        const std::map<std::string, OwnerSubscriptionSnapshot>& snapshots,
        const TopicTemplateResolver& resolver) {
    auto table = std::make_shared<EffectiveRouteTable>();

    bool identity_valid = resolver.isIdentityValid();
    uint64_t identity_gen = resolver.identityGeneration();
    table->setIdentityGeneration(identity_gen);

    for (const auto& [owner, snap] : snapshots) {
        const bool contract_supported =
            (snap.contract_version == kSubscriptionCatalogContractVersion);
        for (const auto& item : snap.items) {
            // CR-015 §18.2: enabled=false 等价 desired 中不存在
            if (!item.enabled.value_or(true)) continue;
            // CR-015: capability-gated route 仅在双方 contract 兼容时进入投影
            const bool capability = !item.readiness_scope.empty();
            if (capability && !contract_supported) continue;

            ResolvedRoute route;
            route.definition.key.owner = owner;
            route.definition.key.route_id = item.route_id;
            route.definition.topic_template.value = item.topic_template;
            // 解析变量列表（用于诊断/日志，不暴露实例值）
            route.definition.topic_template.variables =
                resolver.validator().parseVariables(item.topic_template);
            route.definition.direction = item.direction;
            route.definition.qos = item.qos;
            route.definition.target = item.target;
            route.definition.mandatory =
                (item.criticality == SubscriptionCriticality::GLOBAL_MANDATORY);
            route.definition.snapshot_generation = snap.generation;
            route.definition.readiness_scope = item.readiness_scope;  // CR-015
            route.definition.criticality = item.criticality;          // CR-015
            route.definition.enabled = item.enabled.value_or(true);   // CR-015
            // CR-015: catalog-governed route 的上行不持久化约束
            const CatalogEntry* entry =
                SubscriptionCatalog::instance().lookup(owner, item.route_id);
            route.definition.persist_offline = (entry != nullptr)
                ? entry->persist_offline : true;
            route.identity_generation = identity_gen;

            if (!identity_valid) {
                // CR-009 §5: PROV 不可用/未绑定 -> UNRESOLVED
                route.state = RouteState::UNRESOLVED;
                route.expanded_topic.clear();
            } else {
                // CR-009 §4.2: 展开模板并校验
                auto rr = resolver.resolveAndValidate(item.topic_template);
                if (rr.ok) {
                    route.state = RouteState::ACTIVE;
                    route.expanded_topic = std::move(rr.expanded_topic);
                } else {
                    // 模板展开失败 -> INVALID (CR-009 §8)
                    route.state = RouteState::INVALID;
                    route.expanded_topic.clear();
                }
            }

            table->addRoute(std::move(route));
        }
    }

    return table;
}

} // namespace mqtt
} // namespace tbox
