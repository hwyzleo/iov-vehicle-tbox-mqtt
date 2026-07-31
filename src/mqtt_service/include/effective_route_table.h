#pragma once

// EffectiveRouteTable (CR-009 §2, §3)
//
// 维护正向 route->Topic 与反向 Topic->owner routes 索引的不可变运行时投影。
// 由 TopicProjectionBuilder 从 owner 快照 + 身份快照组合构建，完成后原子替换。
//
// 线程模型：构建完成后不可变，发布/Broker callback/状态查询只读取同一版本。
// 投影记录 snapshot generation 与 identity generation；任一不匹配均视为 stale (CR-009 §12)。
//
// 与 EffectiveSubscriptionTable 共存 (CR-009):
// - EffectiveSubscriptionTable: 供 BrokerSubscriptionReconciler 计算 SUBSCRIBE/UNSUBSCRIBE diff
// - EffectiveRouteTable: 供 RoutePublisher 正向查找、DownlinkRouteDispatcher 反向查找
// 二者从同一数据源构建，不互相依赖。

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace tbox {
namespace mqtt {

/// 下行分发目标引用（反向索引条目，CR-009 §7）
struct DownlinkRouteRef {
    RouteKey key;
    std::string target;     // 目标处理器，如 tsp.fota
    uint8_t qos = 1;
    bool mandatory = false;
};

class TopicProjectionBuilder;

/// 不可变运行时路由投影 (CR-009 §2, §3)
class EffectiveRouteTable {
public:
    EffectiveRouteTable();
    ~EffectiveRouteTable();

    // ---- 正向查询（发布用，CR-009 §6.2）----

    /// 按 RouteKey 查找已解析路由 (CR-009 §6.2)
    /// @return 匹配的 ResolvedRoute；不存在返回 nullopt
    std::optional<ResolvedRoute> lookupRoute(const RouteKey& key) const;

    // ---- 反向查询（下行分发用，CR-009 §7）----

    /// 按 expanded_topic 查找下行分发目标 (CR-009 §7)
    /// @return 匹配的 DownlinkRouteRef 列表（多 owner 时多个）；无匹配返回空
    std::vector<DownlinkRouteRef> lookupDownlink(const std::string& expanded_topic) const;

    /// 获取所有下行 Topic（expanded_topic 集合）
    std::vector<std::string> downlinkTopics() const;

    /// 获取所有上行路由（direction 为 UP/BIDIRECTIONAL 且已展开）
    std::vector<ResolvedRoute> upstreamRoutes() const;

    /// 获取所有下行路由（direction 为 DOWN/BIDIRECTIONAL）
    std::vector<ResolvedRoute> downstreamRoutes() const;

    // ---- 统计 (CR-009 §10.1, §11) ----

    size_t totalRoutes() const;
    size_t activeRoutes() const;
    size_t unresolvedRoutes() const;
    uint64_t identityGeneration() const;
    bool isEmpty() const;

private:
    friend class TopicProjectionBuilder;

    void addRoute(ResolvedRoute route);
    void setIdentityGeneration(uint64_t gen);

    // 正向索引: RouteKey -> ResolvedRoute
    std::map<RouteKey, ResolvedRoute> forward_index_;
    // 反向索引: expanded_topic -> 下行分发目标列表
    std::map<std::string, std::vector<DownlinkRouteRef>> reverse_index_;
    uint64_t identity_generation_ = 0;
};

} // namespace mqtt
} // namespace tbox
