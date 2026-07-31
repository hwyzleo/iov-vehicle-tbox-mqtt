// EffectiveRouteTable (CR-009 §2, §3)
#include "effective_route_table.h"

namespace tbox {
namespace mqtt {

EffectiveRouteTable::EffectiveRouteTable() = default;
EffectiveRouteTable::~EffectiveRouteTable() = default;

void EffectiveRouteTable::addRoute(ResolvedRoute route) {
    RouteKey key = route.definition.key;

    // 正向索引
    forward_index_[key] = std::move(route);

    // 反向索引（仅 DOWN/BIDIRECTIONAL 且已展开的路由）
    auto it = forward_index_.find(key);
    if (it == forward_index_.end()) return;

    const auto& r = it->second;
    if (r.expanded_topic.empty()) return;  // UNRESOLVED 路由不进反向索引

    if (r.definition.direction == SnapshotDirection::DOWN ||
        r.definition.direction == SnapshotDirection::BIDIRECTIONAL) {
        DownlinkRouteRef ref;
        ref.key = r.definition.key;
        ref.target = r.definition.target;
        ref.qos = r.definition.qos;
        ref.mandatory = r.definition.mandatory;
        reverse_index_[r.expanded_topic].push_back(std::move(ref));
    }
}

void EffectiveRouteTable::setIdentityGeneration(uint64_t gen) {
    identity_generation_ = gen;
}

std::optional<ResolvedRoute> EffectiveRouteTable::lookupRoute(const RouteKey& key) const {
    auto it = forward_index_.find(key);
    if (it == forward_index_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<DownlinkRouteRef> EffectiveRouteTable::lookupDownlink(
        const std::string& expanded_topic) const {
    auto it = reverse_index_.find(expanded_topic);
    if (it == reverse_index_.end()) {
        return {};
    }
    return it->second;
}

std::vector<std::string> EffectiveRouteTable::downlinkTopics() const {
    std::vector<std::string> topics;
    topics.reserve(reverse_index_.size());
    for (const auto& [topic, refs] : reverse_index_) {
        topics.push_back(topic);
    }
    return topics;
}

std::vector<ResolvedRoute> EffectiveRouteTable::upstreamRoutes() const {
    std::vector<ResolvedRoute> routes;
    for (const auto& [key, route] : forward_index_) {
        if (route.definition.direction == SnapshotDirection::UP ||
            route.definition.direction == SnapshotDirection::BIDIRECTIONAL) {
            routes.push_back(route);
        }
    }
    return routes;
}

std::vector<ResolvedRoute> EffectiveRouteTable::downstreamRoutes() const {
    std::vector<ResolvedRoute> routes;
    for (const auto& [key, route] : forward_index_) {
        if (route.definition.direction == SnapshotDirection::DOWN ||
            route.definition.direction == SnapshotDirection::BIDIRECTIONAL) {
            routes.push_back(route);
        }
    }
    return routes;
}

size_t EffectiveRouteTable::totalRoutes() const {
    return forward_index_.size();
}

size_t EffectiveRouteTable::activeRoutes() const {
    size_t count = 0;
    for (const auto& [key, route] : forward_index_) {
        if (route.state == RouteState::ACTIVE) {
            ++count;
        }
    }
    return count;
}

size_t EffectiveRouteTable::unresolvedRoutes() const {
    size_t count = 0;
    for (const auto& [key, route] : forward_index_) {
        if (route.state == RouteState::UNRESOLVED) {
            ++count;
        }
    }
    return count;
}

uint64_t EffectiveRouteTable::identityGeneration() const {
    return identity_generation_;
}

bool EffectiveRouteTable::isEmpty() const {
    return forward_index_.empty();
}

} // namespace mqtt
} // namespace tbox
