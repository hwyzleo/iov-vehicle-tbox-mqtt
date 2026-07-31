#pragma once

// DownlinkRouteDispatcher (CR-009 §2, §7)
//
// 将 Broker 下行完整 Topic 匹配为 owner/route_id/target，并通过 mqtt_client 事件分发。
//
// 处理流程 (CR-009 §7):
// 1. 使用当前 EffectiveRouteTable 的反向索引查找匹配路由
// 2. 丢弃或隔离 identity generation 已失效的回调
// 3. 为每个 owner route 生成 RoutedDownlinkEnvelope
// 4. 通过 owner 的独立 framework-ipc 订阅连接分发
// 5. 同一 Topic 多 owner 时复制轻量 envelope/共享受控 payload buffer

#include "mqtt_types.h"
#include "effective_route_table.h"
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace tbox {
namespace mqtt {

class DownlinkRouteDispatcher {
public:
    /// 下行分发回调（每个匹配的 owner route 调用一次）
    using DispatchFn = std::function<void(const RoutedDownlinkEnvelope& event)>;

    DownlinkRouteDispatcher();
    ~DownlinkRouteDispatcher();

    void setDispatchFn(DispatchFn fn);

    /// 分发下行消息 (CR-009 §7)
    /// @param expanded_topic Broker 回调收到的完整 Topic
    /// @param payload 消息 payload
    /// @param route_table 当前不可变路由投影
    /// @param current_identity_generation 当前身份 generation（隔离旧 generation callback）
    /// @return 匹配并分发的路由数；0 表示无匹配
    size_t dispatch(const std::string& expanded_topic,
                    const std::vector<uint8_t>& payload,
                    const std::shared_ptr<const EffectiveRouteTable>& route_table,
                    uint64_t current_identity_generation);

private:
    DispatchFn dispatch_fn_;
};

} // namespace mqtt
} // namespace tbox
