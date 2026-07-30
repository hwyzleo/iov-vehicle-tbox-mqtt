#pragma once

// TBOX-MQTT 公共 DTO 与枚举 (CR-004 §5.1, §7.1)
//
// 本头由 daemon (MqttIpcDispatcher) 与 client SDK (tbox::mqtt::Client) 共享，
// 构成双方的 protocol contract。调用方不接触 method_id、JSON/base64、socket。

#include <cctype>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include "tbox/mqtt/errors.h"

namespace tbox {
namespace mqtt {

// ============================================================
// 枚举 (设计 §3)
// ============================================================

/// QoS 等级
enum class QoS : uint8_t {
    AT_MOST_ONCE  = 0,
    AT_LEAST_ONCE = 1,
    EXACTLY_ONCE  = 2
};

/// 消息优先级（离线缓存排序）
enum class Priority : uint8_t {
    LOW      = 0,
    NORMAL   = 1,
    HIGH     = 2,
    CRITICAL = 3
};

/// 连接状态 (设计 §3, §4.1, CR-006 §4, CR-007 §7)
/// CR-007 新增 SUBSCRIPTION_WAIT/SUBSCRIBING/CONNECTED_DEGRADED/CLOUD_READY。
/// 新值追加在 OFFLINE 之后，保证既有枚举数值不变（framework-store int 兼容）。
enum class ConnectionState {
    INIT,
    PROV_WAIT,
    CERT_WAIT,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
    OFFLINE,
    // ---- CR-007 §7 新增 ----
    SUBSCRIPTION_WAIT,    // 等待 required owner 完整快照（门闩）
    SUBSCRIBING,          // CONNACK 后收敛 Broker 订阅
    CONNECTED_DEGRADED,   // mandatory 失败/门闩超时，连接在但未 Cloud Ready
    CLOUD_READY           // CONNECTED && SUBSCRIPTIONS_READY
};

/// 将 ConnectionState 转为可读字符串
inline std::string connectionStateToString(ConnectionState state) {
    switch (state) {
        case ConnectionState::INIT:         return "INIT";
        case ConnectionState::PROV_WAIT:    return "PROV_WAIT";
        case ConnectionState::CERT_WAIT:    return "CERT_WAIT";
        case ConnectionState::CONNECTING:   return "CONNECTING";
        case ConnectionState::CONNECTED:    return "CONNECTED";
        case ConnectionState::RECONNECTING: return "RECONNECTING";
        case ConnectionState::OFFLINE:      return "OFFLINE";
        case ConnectionState::SUBSCRIPTION_WAIT:  return "SUBSCRIPTION_WAIT";
        case ConnectionState::SUBSCRIBING:        return "SUBSCRIBING";
        case ConnectionState::CONNECTED_DEGRADED: return "CONNECTED_DEGRADED";
        case ConnectionState::CLOUD_READY:        return "CLOUD_READY";
        default: return "UNKNOWN";
    }
}

/// 将 QoS 转为可读字符串
inline std::string qosToString(QoS qos) {
    switch (qos) {
        case QoS::AT_MOST_ONCE:  return "0";
        case QoS::AT_LEAST_ONCE: return "1";
        case QoS::EXACTLY_ONCE:  return "2";
        default: return "?";
    }
}

// ============================================================
// 路由 (设计 §3, CR-004 §3)
// ============================================================

/// 路由条目：内部地址 <-> MQTT topic 映射
struct RouteEntry {
    std::string internal_addr;
    std::string mqtt_topic;
    enum class Direction : uint8_t { UP, DOWN } direction = Direction::UP;
    QoS qos = QoS::AT_LEAST_ONCE;
};

/// 订阅路由（client SDK subscribe 入参，CR-004 §7.1）
struct Route {
    std::string topic;
    QoS qos = QoS::AT_LEAST_ONCE;
};

// ============================================================
// 上行发布信封 (CR-004 §5.1)
// ============================================================

/// publish envelope：msg_id 为 IPC 重试、离线队列与 QoS 重发的统一幂等关联键。
/// payload 为原始二进制；wire 层由 dispatcher/client 做 base64，不在日志记录原文。
struct PublishEnvelope {
    std::string msg_id;          // 必填，幂等关联键
    std::string topic;
    QoS qos = QoS::AT_LEAST_ONCE;
    Priority priority = Priority::NORMAL;
    std::string content_type;    // 如 "application/x-protobuf"
    std::vector<uint8_t> payload;
    std::string trace_id;        // 合法且长度受限的上行关联标识
    std::string request_id;
};

/// publish 结果语义 (CR-004 §5.2)
/// - ACCEPTED:  envelope 校验通过且 MQTT daemon 已接管（在线入发送队列 / 离线入离线队列）
/// - UNKNOWN:   请求已发送但 Response 丢失；client 只能用相同 msg_id 查询/重试
enum class PublishOutcome {
    ACCEPTED,
    UNKNOWN
};

/// publish 结果 (CR-004 §5.2, §7.1)
/// accepted 仅表示 daemon 已接管，不表示 Broker 已 PUBACK。
struct PublishResult {
    bool accepted = false;
    PublishOutcome outcome = PublishOutcome::ACCEPTED;
    std::optional<ErrorCode> error;
};

// ============================================================
// 通用结果 (CR-004 §7.1)
// ============================================================

/// 通用结果模板（registerRoute 等返回）
template <typename T>
struct Result {
    bool ok = false;
    T value{};
    std::optional<ErrorCode> error;

    static Result success(T v) {
        return Result{true, std::move(v), std::nullopt};
    }
    static Result failure(ErrorCode err) {
        return Result{false, T{}, err};
    }
};

/// Result<void> 特化（registerRoute 返回）
template <>
struct Result<void> {
    bool ok = false;
    std::optional<ErrorCode> error;

    static Result success() {
        return Result{true, std::nullopt};
    }
    static Result failure(ErrorCode err) {
        return Result{false, err};
    }
};

/// ConnectionState 查询结果（含诊断信息）
struct ConnectionStateResult {
    ConnectionState state = ConnectionState::INIT;
    std::string last_error;
    uint32_t retry_count = 0;
};

// ============================================================
// 回调类型 (CR-004 §7.1)
// ============================================================

/// 下行消息回调（client SDK subscribe 注册）
using MessageCallback = std::function<void(
    const std::string& topic, const std::vector<uint8_t>& payload)>;

// ============================================================
// 订阅快照与 Cloud Readiness (CR-007 §3, §4, §10; 对齐 TSP DSN-CR-004 §3, §5)
// ============================================================
// 本组 DTO 由 daemon (MqttIpcDispatcher) 与 client SDK (tbox::mqtt::Client) 共享，
// 构成双方快照 IPC contract。TSP 等业务服务经 client SDK 提交完整版本化快照。
// digest 由 owner 端规范化生成（如 TSP SnapshotBuilder: route_id|topic_template|
// DIRECTION|qos|target|mandatory 按 route_id 排序 + SHA-256 hex），MQTT 侧只比对不重算。

/// 订阅方向 (CR-007 §3; TSP DSN-CR-004 §3 含 BIDIRECTIONAL)
enum class SnapshotDirection : uint8_t {
    UP            = 0,
    DOWN          = 1,
    BIDIRECTIONAL = 2  // 同时上行发布 + 下行订阅
};

inline std::string snapshotDirectionToString(SnapshotDirection d) {
    switch (d) {
        case SnapshotDirection::UP:            return "UP";
        case SnapshotDirection::DOWN:          return "DOWN";
        case SnapshotDirection::BIDIRECTIONAL: return "BIDIRECTIONAL";
        default: return "?";
    }
}

/// 方向字符串解析（大小写不敏感，BIDI 为 BIDIRECTIONAL 缩写）
inline bool snapshotDirectionFromString(std::string_view s, SnapshotDirection& out) {
    std::string upper;
    upper.reserve(s.size());
    for (char c : s) {
        upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    if (upper == "UP")                              { out = SnapshotDirection::UP;            return true; }
    if (upper == "DOWN")                            { out = SnapshotDirection::DOWN;          return true; }
    if (upper == "BIDIRECTIONAL" || upper == "BIDI") { out = SnapshotDirection::BIDIRECTIONAL; return true; }
    return false;
}

/// 单条业务订阅项 (CR-007 §3; TSP DSN-CR-004 §3)
struct SubscriptionItem {
    std::string route_id;        // owner 内稳定唯一
    std::string topic_template;  // 保存模板，不固化设备实例值（{ecu_uid} 占位）
    SnapshotDirection direction = SnapshotDirection::UP;
    uint8_t qos = 1;             // 0/1/2
    std::string target;          // 目标处理器，如 tsp.fota
    bool mandatory = false;
};

/// 完整版本化订阅快照 (CR-007 §3; TSP DSN-CR-004 §3)
struct OwnerSubscriptionSnapshot {
    std::string owner;                       // 稳定服务身份，如 "tsp"
    uint64_t generation = 0;                 // 集合语义变化时单调递增
    std::string content_digest;              // 规范化集合摘要（owner 生成, SHA-256 hex）
    bool registration_complete = false;      // 是否包含全部 Mandatory 项
    std::vector<SubscriptionItem> items;
};

/// 快照接受状态 (CR-007 §4; TSP DSN-CR-004 §5)
enum class SnapshotAcceptStatus : uint8_t {
    ACCEPTED = 0,  // MQTT 已原子接受完整集合（≠ Broker SUBACK）
    REJECTED = 1,  // 快照整体未生效，无部分替换
    UNKNOWN  = 2   // 结果未知，使用相同 owner/generation 查询或幂等重试
};

inline std::string snapshotAcceptStatusToString(SnapshotAcceptStatus s) {
    switch (s) {
        case SnapshotAcceptStatus::ACCEPTED: return "ACCEPTED";
        case SnapshotAcceptStatus::REJECTED: return "REJECTED";
        case SnapshotAcceptStatus::UNKNOWN:  return "UNKNOWN";
        default: return "?";
    }
}

/// replaceSubscriptionSnapshot 返回 (CR-007 §4; TSP DSN-CR-004 §5)
/// ACCEPTED 仅表示本地投影已接受，不等于 Broker SUBACK 或 Cloud Ready。
struct ReplaceSnapshotResult {
    SnapshotAcceptStatus status = SnapshotAcceptStatus::UNKNOWN;
    uint64_t accepted_generation = 0;       // ACCEPTED 时回填接受的 generation
    std::vector<std::string> rejected_route_ids;  // REJECTED 时非法项 route_id
    std::string reason_code;                // REJECTED 原因（MQTT-1010 等）
};

/// getSubscriptionSnapshotStatus 返回 (CR-007 §4; TSP DSN-CR-004 §5)
/// 用于响应丢失后的幂等收敛：相同 owner/generation 查询已接受状态。
struct SnapshotStatusResult {
    SnapshotAcceptStatus status = SnapshotAcceptStatus::UNKNOWN;
    uint64_t generation = 0;
    std::string content_digest;
    bool registration_complete = false;
};

/// Cloud Readiness / 订阅就绪查询返回 (CR-007 §4, §10)
/// TSP 不消费此接口（用本地 RegistrationState 做业务门闩）；供诊断与事件联动。
/// CLOUD_READY = connected && subscriptions_ready。
struct SubscriptionReadiness {
    bool cloud_ready = false;               // CLOUD_READY 综合态
    bool subscriptions_ready = false;       // Mandatory 全部 ACTIVE
    bool connected = false;                 // CONNACK 成功（含 SUBSCRIBING/CLOUD_READY/DEGRADED）
    uint64_t connection_generation = 0;     // 连接 generation
    std::map<std::string, uint64_t> owner_generations;  // owner -> 最新已接受 generation
    uint32_t mandatory_count = 0;           // 当前 mandatory 项总数
    uint32_t mandatory_active = 0;          // 已 SUBACK ACTIVE 的 mandatory 项数
    std::string reason_code;                // 非 ready 时的原因（MQTT-1011 等）
};

} // namespace mqtt
} // namespace tbox
