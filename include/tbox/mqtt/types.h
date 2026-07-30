#pragma once

// TBOX-MQTT 公共 DTO 与枚举 (CR-004 §5.1, §7.1)
//
// 本头由 daemon (MqttIpcDispatcher) 与 client SDK (tbox::mqtt::Client) 共享，
// 构成双方的 protocol contract。调用方不接触 method_id、JSON/base64、socket。

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
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

/// 连接状态 (设计 §3, §4.1, CR-006 §4)
enum class ConnectionState {
    INIT,
    PROV_WAIT,
    CERT_WAIT,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
    OFFLINE
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

} // namespace mqtt
} // namespace tbox
