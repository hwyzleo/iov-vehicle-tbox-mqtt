// DownlinkRouteDispatcher (CR-009 §2, §7)
#include "downlink_route_dispatcher.h"
#include "mqtt_context.h"

namespace tbox {
namespace mqtt {

DownlinkRouteDispatcher::DownlinkRouteDispatcher() = default;
DownlinkRouteDispatcher::~DownlinkRouteDispatcher() = default;

void DownlinkRouteDispatcher::setDispatchFn(DispatchFn fn) {
    dispatch_fn_ = std::move(fn);
}

size_t DownlinkRouteDispatcher::dispatch(
        const std::string& expanded_topic,
        const std::vector<uint8_t>& payload,
        const std::shared_ptr<const EffectiveRouteTable>& route_table,
        uint64_t current_identity_generation) {
    if (!route_table) return 0;

    // 1. 反向索引查找匹配路由 (CR-009 §7)
    auto refs = route_table->lookupDownlink(expanded_topic);
    if (refs.empty()) return 0;

    size_t dispatched = 0;
    for (const auto& ref : refs) {
        // 2. 隔离 identity generation 已失效的回调 (CR-009 §7)
        // route_table 的 identity_generation 必须与当前一致
        if (route_table->identityGeneration() != current_identity_generation) {
            // 旧 generation callback，不分发 (CR-009 §7, §8)
            continue;
        }

        // 3. 生成 RoutedDownlinkEnvelope (CR-009 §7)
        RoutedDownlinkEnvelope event;
        event.key = ref.key;
        event.target = ref.target;
        event.payload = payload;  // 共享/复制轻量 envelope
        event.qos = ref.qos;
        event.request_id = generateRequestId();
        event.trace_id = "";

        // 4. 通过回调分发 (CR-009 §7)
        if (dispatch_fn_) {
            dispatch_fn_(event);
            ++dispatched;
        }
    }

    return dispatched;
}

} // namespace mqtt
} // namespace tbox
