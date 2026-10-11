// DownlinkRouteDispatcher (CR-009 §2, §7; CR-015 §18.6)
#include "downlink_route_dispatcher.h"
#include "mqtt_context.h"
#include "mqtt_log_adapter.h"

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

        // 2b. CR-015 §18.6: capability 下行 payload 上限（DOWNLINK_VALIDATE）
        // 超限整体拒绝，不截断、不落盘；日志只记录长度与上限，不记录 payload。
        if (!ref.readiness_scope.empty() &&
            payload.size() > kKeyprovMaxPayloadBytes) {
            MqttLogAdapter::router().warn(
                "mqtt.capability.downlink.rejected",
                "capability 下行 payload 超限，整体拒绝",
                {{"owner", tbox::fw::log::FieldValue::makeString(ref.key.owner)},
                 {"route_id", tbox::fw::log::FieldValue::makeString(ref.key.route_id)},
                 {"payload_size", tbox::fw::log::FieldValue::makeInt(
                     static_cast<int64_t>(payload.size()))},
                 {"max_payload_bytes", tbox::fw::log::FieldValue::makeInt(
                     static_cast<int64_t>(kKeyprovMaxPayloadBytes))},
                 {"stage", tbox::fw::log::FieldValue::makeString("DOWNLINK_VALIDATE")},
                 {"reason_code", tbox::fw::log::FieldValue::makeString(
                     capability_reason::kKeyprovPayloadTooLarge)},
                 {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1004")}}
            );
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
        event.readiness_scope = ref.readiness_scope;  // CR-015

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
