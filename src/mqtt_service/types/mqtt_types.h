#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <optional>
#include <cstdint>

namespace tbox {
namespace mqtt {

// Connection states (Design §4.1)
enum class ConnectionState {
    INIT,
    CERT_WAIT,
    CONNECTING,
    CONNECTED,
    RECONNECTING,
    OFFLINE
};

// Message delivery states
enum class MessageState {
    PENDING,
    INFLIGHT,
    ACKED,
    DROPPED
};

// QoS levels
enum class QoS : uint8_t {
    AT_MOST_ONCE = 0,
    AT_LEAST_ONCE = 1,
    EXACTLY_ONCE = 2
};

// Message priority for offline cache
enum class Priority : uint8_t {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2,
    CRITICAL = 3
};

// Connection configuration (Design §3)
struct ConnectionConfig {
    std::string broker_host;
    uint16_t broker_port = 8883;
    std::string root_ca_ref;
    std::string client_id;  // VIN + SN
    uint16_t keepalive = 60;
    bool clean_start = false;
    uint32_t session_expiry = 3600;
    std::string tls_cert_ref;  // SEC handle
    enum class Source : uint8_t { FACTORY, OTA } source = Source::FACTORY;
    uint32_t version = 0;
};

// Connection state tracking (Design §3)
struct ConnectionStateInfo {
    ConnectionState state = ConnectionState::INIT;
    std::string last_error;
    uint32_t retry_count = 0;
    std::chrono::steady_clock::time_point next_retry_at;
};

// Outbound message (Design §3)
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

// Subscription (Design §3)
struct Subscription {
    std::string topic;
    QoS qos;
    std::string target_service;
};

// Route entry (Design §3)
struct RouteEntry {
    std::string internal_addr;
    std::string mqtt_topic;
    enum class Direction : uint8_t { UP, DOWN } direction = Direction::UP;
    QoS qos = QoS::AT_MOST_ONCE;
};

// LWT configuration (Design §3)
struct LwtConfig {
    std::string topic;
    std::vector<uint8_t> payload;  // offline status
    QoS qos = QoS::AT_LEAST_ONCE;
    bool retain = false;
};

// Error codes (Requirements §6)
enum class ErrorCode {
    MQTT_1001 = 1001,  // Certificate/key unavailable or mTLS handshake failed
    MQTT_1002 = 1002,  // Network unavailable / connection failed
    MQTT_1003 = 1003,  // Connection disconnected (trigger reconnect)
    MQTT_1004 = 1004,  // Invalid topic/message format
    MQTT_1005 = 1005,  // Publish timeout / unconfirmed (QoS retries exhausted)
    MQTT_1006 = 1006,  // Offline cache full (message dropped)
    MQTT_1007 = 1007,  // Downstream message cannot route / no subscriber
    MQTT_1008 = 1008   // Subscribe failed
};

// API result types
struct PublishResult {
    bool accepted = false;
    std::optional<ErrorCode> error;
};

struct SubscribeResult {
    bool status = false;
    std::optional<ErrorCode> error;
};

struct ConnectionStateResult {
    ConnectionState state = ConnectionState::INIT;
    std::string last_error;
    uint32_t retry_count = 0;
};

} // namespace mqtt
} // namespace tbox
