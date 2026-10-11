#pragma once

// TBOX-MQTT IPC 方法 ID 与事件类型 (CR-004 §3)
//
// 传输层由 framework-ipc 负责，序列化/编解码在 MqttIpcDispatcher / mqtt_client 中完成。
// 此枚举仅定义方法号与事件号，供 dispatcher、retry policy、client facade 共享。

#include <cstdint>

namespace tbox {
namespace mqtt {
namespace ipc {

/// MQTT IPC 方法 ID（与 framework-ipc RequestHeader.method_id 一致, CR-004 §3）
enum class MethodId : uint32_t {
    PUBLISH              = 1,  // publish(envelope) -> accepted/rejected
    SUBSCRIBE            = 2,  // subscribe(route) -> 登记 route + client_fd 事件订阅
    UNSUBSCRIBE          = 3,  // unsubscribe -> 移除连接订阅
    REGISTER_ROUTE       = 4,  // registerRoute -> 维护 internal_addr <-> topic
    GET_CONNECTION_STATE = 5,  // getConnectionState -> 只读连接状态
    // CR-007 §4: 订阅快照与 Cloud Readiness
    REPLACE_SUBSCRIPTION_SNAPSHOT    = 6,  // replaceSubscriptionSnapshot -> 原子接受/拒绝
    GET_SUBSCRIPTION_SNAPSHOT_STATUS = 7,  // getSubscriptionSnapshotStatus -> 幂等查询
    GET_SUBSCRIPTION_READINESS       = 8,  // getSubscriptionReadiness -> Cloud Ready 诊断
    // CR-009 §6.1: route-based 发布
    PUBLISH_ROUTE                    = 9,  // publishRoute -> 按 owner/route_id 发布
    GET_ROUTE_PROJECTION_STATUS      = 10, // getRouteProjectionStatus -> 投影状态诊断
    // CR-010 §3: route-aware 下行事件订阅（握手携 owner，不触发 Broker SUBSCRIBE）
    SUBSCRIBE_ROUTED_DOWNLINK        = 11, // subscribeRoutedDownlink -> 登记 fd->owner 事件通道
    // CR-014 §5: Cloud Ready 事件订阅（仅登记 fd 到 CLOUD_READY_CHANGED，不触发 Broker 操作）
    SUBSCRIBE_CLOUD_READINESS        = 12, // subscribeCloudReadinessChanged -> 登记 fd 事件通道
};

/// MQTT IPC 事件类型（与 framework-ipc EventHeader.event_type 一致, CR-004 §6）
/// Broker 下行消息经 route registry 映射为 event_type，通过 Server::push_event 推送。
enum class EventType : uint32_t {
    DOWNLINK_MESSAGE          = 1,  // 下行消息（payload 为下行 envelope JSON）
    CONNECTION_STATE_CHANGED  = 2,  // 连接状态变更
    CLOUD_READY_CHANGED       = 3,  // Cloud Ready 状态变更 (CR-007 §10)
    // CR-010 §3: routed downlink 事件（daemon 按 owner 定向推送，不广播给 legacy 订阅者）
    ROUTED_DOWNLINK_MESSAGE   = 4,  // routed downlink（payload 为 routed envelope JSON）
};

} // namespace ipc
} // namespace mqtt
} // namespace tbox
