// RoutePublisher (CR-009 §2, §6)
#include "route_publisher.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <chrono>

namespace tbox {
namespace mqtt {

RoutePublisher::RoutePublisher() = default;
RoutePublisher::~RoutePublisher() = default;

void RoutePublisher::setBrokerPublishFn(BrokerPublishFn fn) {
    broker_publish_fn_ = std::move(fn);
}

void RoutePublisher::setQueueEnqueueFn(QueueEnqueueFn fn) {
    queue_enqueue_fn_ = std::move(fn);
}

void RoutePublisher::setIsConnectedFn(IsConnectedFn fn) {
    is_connected_fn_ = std::move(fn);
}

PublishResult RoutePublisher::publishRoute(
        const RoutePublishRequest& req,
        const std::shared_ptr<const EffectiveRouteTable>& route_table,
        const TopicTemplateResolver& resolver) {
    PublishResult result;

    // msg_id 必填 (CR-009 §6.1)
    if (req.msg_id.empty() || req.key.owner.empty() || req.key.route_id.empty()) {
        result.accepted = false;
        result.error = ErrorCode::MQTT_1004;
        return result;
    }

    // 1. 按 RouteKey 查询当前 immutable projection (CR-009 §6.2)
    auto route = route_table->lookupRoute(req.key);
    if (!route.has_value()) {
        // route 不存在 -> MQTT-1004 (CR-009 §6.2)
        result.accepted = false;
        result.error = ErrorCode::MQTT_1004;
        return result;
    }

    // 2. 校验方向 (CR-009 §6.2: UP/BIDIRECTIONAL)
    if (route->definition.direction != SnapshotDirection::UP &&
        route->definition.direction != SnapshotDirection::BIDIRECTIONAL) {
        result.accepted = false;
        result.error = ErrorCode::MQTT_1004;
        return result;
    }

    // 3. 校验状态 (CR-009 §6.2: ACTIVE)
    if (route->state != RouteState::ACTIVE) {
        // 模板未展开 -> MQTT-1012 (CR-009 §11)
        result.accepted = false;
        result.error = (route->state == RouteState::UNRESOLVED)
            ? ErrorCode::MQTT_1009   // PROV 不可用
            : ErrorCode::MQTT_1012;  // 模板/实例化失败
        return result;
    }

    // 4. 校验 identity generation 当前有效 (CR-009 §6.2, §4.2 stage 3)
    if (!resolver.isGenerationCurrent(route->identity_generation)) {
        // stale generation -> MQTT-1009 (CR-009 §8)
        result.accepted = false;
        result.error = ErrorCode::MQTT_1009;
        return result;
    }

    // 5. QoS: route 定义为默认，调用方可覆盖 (CR-009 §6.2)
    uint8_t qos = route->definition.qos;
    if (req.qos.has_value()) {
        if (req.qos.value() <= 2) {
            qos = req.qos.value();
        }
        // 超出范围的 QoS 覆盖忽略，使用 route 默认值
    }

    // 6. 在线/离线处理 (CR-009 §6.2)
    bool connected = is_connected_fn_ ? is_connected_fn_() : false;

    if (connected) {
        // 在线: 再次按当前 generation 解析 expanded_topic (CR-009 §6.2 step 6)
        // route->expanded_topic 已在投影构建时展开，且 generation 已校验一致
        if (route->expanded_topic.empty()) {
            result.accepted = false;
            result.error = ErrorCode::MQTT_1012;
            return result;
        }

        // 真正发送前再次核对 generation (CR-009 §6.2 step 6)
        if (!resolver.isGenerationCurrent(route->identity_generation)) {
            result.accepted = false;
            result.error = ErrorCode::MQTT_1009;
            return result;
        }

        if (broker_publish_fn_) {
            bool ok = broker_publish_fn_(route->expanded_topic, req.payload,
                                         static_cast<QoS>(qos));
            result.accepted = ok;
            if (!ok) {
                result.error = ErrorCode::MQTT_1002;
            }
        } else {
            result.accepted = false;
            result.error = ErrorCode::MQTT_1002;
        }
    } else {
        // 离线: 构造 OutboundMessage 入队 (CR-009 §6.2, §9)
        OutboundMessage msg;
        msg.msg_id = req.msg_id;
        msg.route = req.key;
        msg.accepted_identity_generation = route->identity_generation;
        msg.identity_generation = route->identity_generation;
        msg.qos = static_cast<QoS>(qos);
        msg.payload = req.payload;
        msg.priority = req.priority;
        msg.created_at = std::chrono::steady_clock::now();
        msg.state = MessageState::PENDING;

        if (queue_enqueue_fn_ && queue_enqueue_fn_(msg)) {
            result.accepted = true;
        } else {
            result.accepted = false;
            result.error = ErrorCode::MQTT_1006;
        }
    }

    return result;
}

} // namespace mqtt
} // namespace tbox
