#pragma once

// TBOX-MQTT daemon 内部类型 (设计 §3)
//
// 公共 DTO/枚举（QoS/Priority/ConnectionState/RouteEntry/PublishEnvelope/PublishResult/
// ConnectionStateResult/ErrorCode 等）已上移到 include/tbox/mqtt/{types.h,errors.h}，
// 由 daemon 与 client SDK 共享。本头仅保留 daemon 内部实现类型。

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "tbox/mqtt/types.h"
#include "tbox/mqtt/errors.h"

namespace tbox {
namespace mqtt {

// ============================================================
// daemon 内部枚举 (设计 §3)
// ============================================================

/// 密钥来源类型
enum class KeySource : uint8_t {
    FILE,   // 私钥来自文件（开发）
    HSM     // 私钥在 HSM 中经 PKCS#11（量产）
};

/// 消息投递状态
enum class MessageState {
    PENDING,
    INFLIGHT,
    ACKED,
    DROPPED
};

// ============================================================
// daemon 内部配置/状态 (设计 §3)
// ============================================================

/// 连接配置 (设计 §3)
/// 静态项经 framework-config（只读）；可更新项（broker_host/broker_port）经 framework-store。
struct ConnectionConfig {
    // --- 可经 framework-store 更新 (source=OTA) ---
    std::string broker_host;
    uint16_t broker_port = 8883;
    // --- 静态经 framework-config (只读) ---
    std::string root_ca_ref;
    std::string client_id;  // VIN + SN
    uint16_t keepalive = 60;
    bool clean_start = false;
    uint32_t session_expiry = 3600;
    std::string tls_cert_ref;   // 证书文件路径 / SEC 句柄引用
    std::string tls_key_ref;    // 文件路径或 PKCS#11 URI
    KeySource key_source = KeySource::FILE;
    std::string username;
    std::string password;
    enum class Source : uint8_t { FACTORY, OTA } source = Source::FACTORY;
    uint32_t version = 0;
};

/// 连接状态跟踪 (设计 §3，运行时可变，经 framework-store 持久化)
struct ConnectionStateInfo {
    ConnectionState state = ConnectionState::INIT;
    std::string last_error;
    uint32_t retry_count = 0;
    std::chrono::steady_clock::time_point next_retry_at;
};

/// 离线缓存队列元素 (设计 §3，MQTT 自身业务数据，自研持久化)
struct OutboundMessage {
    std::string msg_id;
    std::string topic;
    QoS qos;
    std::vector<uint8_t> payload;
    Priority priority = Priority::NORMAL;
    std::chrono::steady_clock::time_point created_at;
    uint32_t retries = 0;
    MessageState state = MessageState::PENDING;
};

/// LWT 配置 (设计 §3)
struct LwtConfig {
    std::string topic;
    std::vector<uint8_t> payload;  // offline 状态
    QoS qos = QoS::AT_LEAST_ONCE;
    bool retain = false;
};

/// 订阅结果（daemon 内部，MqttClientManager::subscribe 返回）
struct SubscribeResult {
    bool status = false;
    std::optional<ErrorCode> error;
};

/// 离线缓存队列配置（取自 framework-config，设计 §3）
struct OfflineQueueConfig {
    std::string path = "/var/lib/tbox/mqtt/queue";
    size_t max_size = 1000;
    std::string drop_policy = "drop_oldest";  // "drop_oldest" or "drop_lowest_priority"
};

/// IPC 配置（取自 common.ipc.* + mqtt.ipc.*，CR-004 §8）
struct MqttIpcConfig {
    std::string socket_path = "/tmp/tbox-mqtt.sock";
    uint32_t dispatch_queue_size = 1024;
    uint32_t subscriber_queue_size = 128;
    std::string slow_subscriber_policy = "disconnect";  // disconnect/drop/backpressure
};

// ============================================================
// framework-store key 常量 (设计 §5)
// ============================================================
namespace StoreKeys {
    constexpr const char* kBrokerHost     = "mqtt.broker_host";
    constexpr const char* kBrokerPort     = "mqtt.broker_port";
    constexpr const char* kConnState      = "mqtt.conn_state";
    constexpr const char* kConnLastError  = "mqtt.conn_last_error";
    constexpr const char* kConnRetryCount = "mqtt.conn_retry_count";
    constexpr const char* kConfigVersion  = "mqtt.config_version";
} // namespace StoreKeys

} // namespace mqtt
} // namespace tbox
