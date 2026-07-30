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
#include "tbox/sec/types.h"
#include "tbox/prov/types.h"

namespace tbox {
namespace mqtt {

// ============================================================
// daemon 内部枚举 (设计 §3, CR-005 §3)
// ============================================================

/// TLS 版本 (CR-005 §3.1)
enum class TlsVersion : uint8_t {
    TLS_1_2 = 0,
    TLS_1_3 = 1
};

/// 持续时间类型 (CR-005 §3.1)
using Duration = std::chrono::milliseconds;

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

/// PROV 绑定退避配置 (CR-006 §9)
struct ProvisionRetryConfig {
    Duration initial_backoff{500};    // ms
    Duration max_backoff{30000};      // ms
    double multiplier = 2.0;
};

/// 连接配置 (设计 §3, CR-005 §3.1, CR-006 §9)
/// 静态项经 framework-config（只读）；可更新项（broker_host/broker_port）经 framework-store。
/// 凭据相关字段已移除：root_ca_ref/tls_cert_ref/tls_key_ref/key_source 不再存在，
/// TLS 凭据全部经 tbox::sec_client 获取 (CR-005 §1)。
struct ConnectionConfig {
    // --- 可经 framework-store 更新 (source=OTA) ---
    std::string broker_host;
    uint16_t broker_port = 8883;
    // --- 静态经 framework-config (只读) ---
    std::string credential_profile;  // fixed: mqtt (CR-005 §3.1)
    // CR-006 §9: client_id_format 为只读云端接入契约，必须同时包含且只引用 vin、sn 身份占位
    std::string client_id_format;    // e.g. "{vin}-{sn}" (CR-006 §9)
    // CR-006 §6: client_id 运行时由 ProvisionIdentityManager 从 VIN+SN 派生，不经配置文件读取实例值
    std::string client_id;           // runtime-derived: buildClientId(vin, sn) (CR-006 §6.1)
    TlsVersion min_tls_version = TlsVersion::TLS_1_2;
    TlsVersion max_tls_version = TlsVersion::TLS_1_3;
    Duration handshake_timeout{10000};  // ms (CR-005 §3.1)
    uint16_t keepalive = 60;
    bool clean_start = false;
    uint32_t session_expiry = 3600;
    // CR-006 §6.3: username 运行时从 ecu_uid 填充，不经配置文件读取实例值
    std::string username;           // runtime-derived: binding.ecu_uid (CR-006 §6.3)
    std::string password;           // 可选，默认 mTLS 认证
    ProvisionRetryConfig prov_retry_config;  // CR-006 §9: PROV_WAIT 退避配置
    enum class Source : uint8_t { FACTORY, OTA } source = Source::FACTORY;
    uint32_t version = 0;
};

/// PROV 绑定身份快照 (CR-006 §3)
/// 仅存在于进程内，不作为绑定事实持久化到 framework-store。
/// 有效条件：state == BOUND 且 vin、ecu_uid、sn 均非空并通过格式校验。
struct ProvisionIdentitySnapshot {
    std::string vin;
    std::string ecu_uid;
    std::string sn;
    tbox::prov::ProvisionState state = tbox::prov::ProvisionState::NONE;
    bool locked = false;
    std::string client_id;   // buildClientId(vin, sn)，不得读取 ecu_uid 参与生成
    uint64_t generation = 0; // VIN/SN/ECU UID/state 任一变化时递增
};

/// 运行时 TLS 凭据 (CR-005 §3.2)
/// 仅存在于 MQTT 进程内存，不进入 framework-store、离线队列或文件系统。
/// private_key_ref 生命周期不超过当前 SEC boot epoch 和 credential version。
struct ActiveTlsCredential {
    std::string credential_id;
    uint64_t version = 0;
    tbox::sec::TlsCredentialStatus status = tbox::sec::TlsCredentialStatus::NOT_READY;
    std::vector<uint8_t> root_ca_bundle;       // DER bundle (一个或多个受信根/中间 CA)
    std::vector<uint8_t> client_cert_chain;    // DER, leaf -> intermediate
    tbox::sec::OpaqueKeyRef private_key_ref;   // 不透明引用，不得记录/序列化/跨重启复用
    tbox::sec::KeyAlgorithm key_algorithm = tbox::sec::KeyAlgorithm::UNKNOWN;  // 适配器算法校验 (CR-005 §5.2)
    int64_t not_before = 0;  // unix epoch 秒
    int64_t not_after = 0;   // unix epoch 秒
    bool ready = false;      // status == READY 且 bundle 完整
};

/// 凭据轮询退避配置 (CR-005 §8)
struct CredentialPollConfig {
    Duration initial_backoff{1000};   // ms
    Duration max_backoff{60000};      // ms
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
    uint64_t identity_generation = 0;  // CR-006 §7: 入队时绑定身份 generation，身份变化后跳过过期消息
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
