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

/// TLS 会话身份快照 (DSN-CR-014 §3, §4.1, §12.5)
///
/// 每次创建 TLS context 时固化：cert_session_generation 不可复用；
/// device_identity_digest/certificate_ref 来自实际写入 SSL_CTX 的 leaf 客户端证书
/// （规范 DER 的 SHA-256），不依赖重新查询 SEC 当前 credential。
/// 仅存在于 MQTT 进程内存，不持久化、不写入日志原文。
struct TlsSessionIdentity {
    uint64_t cert_session_generation = 0;
    uint64_t connection_generation = 0;
    std::string credential_id;
    uint64_t credential_version = 0;
    std::string device_identity_digest;  // sha256(leaf DER)
    std::string certificate_ref;         // sha256:<hex fingerprint>
    // CR-017 §3, §19.2: 从实际装入 SSL_CTX 的 leaf 客户端证书 CN 规范化得到的 ecu_uid。
    // 与上述指纹/generation 固化于同一不可变对象，查询时不得重新读取 SEC/PROV 拼接覆盖。
    std::string ecu_uid;
    bool valid = false;                  // 是否已成功固化 leaf 身份
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

// ============================================================
// CR-009: Topic 模板与 route-based 路由投影
// ============================================================
// 业务 owner 维护稳定的 route_id、topic_template 和业务路由属性；MQTT 使用自身
// 当前有效的 ProvisionIdentitySnapshot.ecu_uid 将模板实例化为完整 Broker Topic。
// expanded_topic 只存在于 MQTT 内存运行时投影，不得回写业务快照或成为业务事实来源。

/// Topic 模板变量 (CR-009 §3; v1 仅 ecu_uid)
struct TopicTemplateVariable {
    std::string name;  // e.g. "ecu_uid"
};

/// Topic 模板 (CR-009 §3)
struct TopicTemplate {
    std::string value;                          // 原始模板字符串，如 "vehicle/{ecu_uid}/up/fota"
    std::vector<TopicTemplateVariable> variables;  // v1 仅 ecu_uid
};

/// 路由键：owner + route_id (CR-009 §3)
struct RouteKey {
    std::string owner;
    std::string route_id;

    bool operator==(const RouteKey& o) const {
        return owner == o.owner && route_id == o.route_id;
    }
    bool operator<(const RouteKey& o) const {
        if (owner != o.owner) return owner < o.owner;
        return route_id < o.route_id;
    }
};

/// 路由定义（业务 owner 提交的稳定属性，CR-009 §3）
struct RouteDefinition {
    RouteKey key;
    TopicTemplate topic_template;
    SnapshotDirection direction = SnapshotDirection::UP;
    uint8_t qos = 1;
    std::string target;          // 目标处理器，如 tsp.fota
    bool mandatory = false;
    uint64_t snapshot_generation = 0;  // owner 快照 generation
    // ---- CR-015 ----
    std::string readiness_scope;  // 空=全局；非空=capability scope
    SubscriptionCriticality criticality = SubscriptionCriticality::OPTIONAL;
    bool enabled = true;          // catalog-governed route 要求显式 enabled=true
    bool persist_offline = true;  // 是否允许写入持久离线队列
};

/// 路由解析状态 (CR-009 §3, §8)
enum class RouteState : uint8_t {
    UNRESOLVED = 0,  // PROV 不可用/未绑定，模板未实例化
    BUILDING   = 1,  // 正在用当前 identity generation 实例化
    ACTIVE     = 2,  // 投影已原子发布，可发布/订阅
    RETIRING   = 3,  // identity generation 已变化，旧路由失效中
    INVALID    = 4   // 模板展开失败
};

inline std::string routeStateToString(RouteState s) {
    switch (s) {
        case RouteState::UNRESOLVED: return "UNRESOLVED";
        case RouteState::BUILDING:   return "BUILDING";
        case RouteState::ACTIVE:     return "ACTIVE";
        case RouteState::RETIRING:   return "RETIRING";
        case RouteState::INVALID:    return "INVALID";
        default: return "UNKNOWN";
    }
}

/// 已解析路由（运行时投影条目，CR-009 §3）
/// expanded_topic 只存在于内存运行时投影，不持久化、不进日志明文。
struct ResolvedRoute {
    RouteDefinition definition;
    std::string expanded_topic;      // 实例化后的完整 Broker Topic（内存）
    uint64_t identity_generation = 0;  // 展开时使用的身份 generation
    RouteState state = RouteState::UNRESOLVED;
};

/// Route-based 发布请求 (CR-009 §3, §6.1; CR-017 §5, §19.3)
struct RoutePublishRequest {
    RouteKey key;
    std::string msg_id;
    std::vector<uint8_t> payload;
    std::optional<uint8_t> qos;      // 覆盖 route 定义 QoS（策略允许范围内）
    Priority priority = Priority::NORMAL;
    std::string content_type;
    std::string trace_id;
    std::string request_id;
    // CR-017 §5: 调用方构造报文时使用的预期会话上下文；keyprov.uplink 必填。
    std::optional<SessionContext> expected_session;
};

/// Route-based 下行事件 (CR-009 §3, §7)
/// 通过 owner 的独立 framework-ipc 订阅连接分发；业务服务无需解析完整 Topic。
struct RoutedDownlinkEnvelope {
    RouteKey key;
    std::string target;             // 目标处理器，如 tsp.fota
    std::vector<uint8_t> payload;
    uint8_t qos = 1;
    std::string request_id;
    std::string trace_id;
    std::string readiness_scope;    // CR-015: 空=全局；非空=capability scope
};

/// 离线缓存队列元素 (设计 §3，MQTT 自身业务数据，自研持久化)
/// CR-009 §9: route-based 路径保存 RouteKey + accepted_identity_generation，
/// 发送时从当前投影重新解析 Topic，不将完整 Topic 作为唯一恢复依据。
/// legacy 路径（route.owner 为空）仍使用 topic 字段。
struct OutboundMessage {
    std::string msg_id;
    std::string topic;               // legacy 路径：完整 Topic；route-based 路径可为空
    RouteKey route;                  // CR-009 §9: route-based 路径的 RouteKey
    uint64_t accepted_identity_generation = 0;  // CR-009 §9: 入队时身份 generation
    QoS qos;
    std::vector<uint8_t> payload;
    Priority priority = Priority::NORMAL;
    std::chrono::steady_clock::time_point created_at;
    uint32_t retries = 0;
    MessageState state = MessageState::PENDING;
    uint64_t identity_generation = 0;  // CR-006 §7: 入队时绑定身份 generation，身份变化后跳过过期消息

    /// 是否为 route-based 消息（CR-009 §9）
    bool isRouteBased() const { return !route.owner.empty(); }
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

/// 订阅快照与 Cloud Readiness 配置 (CR-007 §6)
struct SubscriptionConfig {
    std::vector<std::string> required_owners;  // 启动必需 owner，如 ["tsp"]
    Duration startup_wait_timeout_ms{5000};    // 门闩等待超时
    Duration reconcile_timeout_ms{10000};      // SUBACK 收敛超时
    uint32_t suback_retry_max = 3;             // SUBACK 重试上限
    uint64_t owner_lease_ms = 0;               // 0=关闭自动过期 (CR-007 §5)
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
    // CR-007 §3.1: 已应用 Broker 订阅清单（JSON blob，仅传输恢复状态）
    constexpr const char* kSubsManifest = "mqtt.subs_manifest";
} // namespace StoreKeys

} // namespace mqtt
} // namespace tbox
