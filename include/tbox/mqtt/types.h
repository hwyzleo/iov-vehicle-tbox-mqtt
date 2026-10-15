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

/// publish 结果 (CR-004 §5.2, §7.1; CR-015 §18.4)
/// accepted 仅表示 daemon 已接管，不表示 Broker 已 PUBACK。
/// CR-015: reason_code 为稳定脱敏原因（如 KEYPROV_PAYLOAD_TOO_LARGE），
/// 供调用方与指标按 reason 聚合；不含身份/Topic/payload。
struct PublishResult {
    bool accepted = false;
    PublishOutcome outcome = PublishOutcome::ACCEPTED;
    std::optional<ErrorCode> error;
    std::string reason_code;   // CR-015：稳定原因码（可为空）
    bool retryable = true;     // CR-015：是否可安全重试（长度/格式门禁失败为 false）
};

// ============================================================
// 会话上下文与身份绑定发布 (CR-017 §4, §19.2)
// ============================================================

/// 可校验的 daemon 生命周期 / 连接 / 证书会话上下文 (CR-017 §4)
///
/// 全部字段由 MQTT daemon 从同一不可变会话身份对象产生；SDK/调用方只读。
/// 该上下文用于「读快照 -> 构造报文 -> publish」期间的一致性检查（防 TOCTOU），
/// 不是授权凭据，也不是云端认证证据。
/// - daemon_epoch：进程级生命周期标识，进程重启后变化；
/// - connection_generation：Broker 连接 generation；
/// - cert_session_generation：本次 TLS 会话不可复用 generation。
struct SessionContext {
    std::string daemon_epoch;
    uint64_t connection_generation = 0;
    uint64_t cert_session_generation = 0;
};

/// 身份绑定发布的控制元数据 (CR-017 §4, §5, §18.4)
///
/// keyprov.uplink 必须携带 expected_session；缺失/不支持/过期时明确拒绝，
/// 不得退化为未绑定发布。普通业务可省略 expected_session 保持既有行为。
struct PublishOptions {
    QoS qos = QoS::AT_LEAST_ONCE;
    Priority priority = Priority::NORMAL;
    std::string content_type;
    std::string trace_id;
    std::string request_id;
    std::optional<SessionContext> expected_session;  // CR-017：会话绑定 guard
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
// Routed Downlink 事件模型 (CR-010 §2.1)
// ============================================================
//
// daemon 已将 Broker 下行完整 Topic 映射为 routed downlink 事件，事件 payload
// 包含 owner/route_id/target/qos/payload_base64/request_id/trace_id，不下发完整 Topic。
// SDK 严格解码后以此结构交付业务调用方，使调用方按稳定业务路由而非完整 Topic 分发。
// 完整 Broker Topic 默认不进入该公开事件模型。

/// Routed downlink 公开事件 (CR-010 §2.1)
/// 字段经 SDK 严格校验与 base64 解码后填充；调用方按 owner/route_id/target 分发。
struct RoutedDownlinkEvent {
    std::string owner;            // 必填，订阅 owner
    std::string route_id;         // 必填，owner 内稳定路由标识
    std::string target;           // 必填，目标处理器，如 tsp.fota
    std::uint8_t qos{0};          // 0/1/2
    std::vector<std::uint8_t> payload;  // payload_base64 解码后的二进制
    std::string request_id;       // 必填
    std::string trace_id;         // 可选，存在时已通过格式校验
};

/// Routed downlink 业务回调 (CR-010 §2.1)
/// callback 在 SDK 内部锁外调用；抛出的异常由 SDK 捕获，不得终止读取线程。
using RoutedDownlinkCallback =
    std::function<void(const RoutedDownlinkEvent&)>;

// ============================================================
// 订阅快照与 Cloud Readiness (CR-007 §3, §4, §10; CR-014 §3, §5;
// 对齐 TSP DSN-CR-004 §3, §5)
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

/// 订阅关键等级 (DSN-CR-015 §3, §18.3)
/// - OPTIONAL: 非必需，不参与任何 readiness 门禁
/// - GLOBAL_MANDATORY: 进入共享 Cloud Ready 的全局必需下行
/// - CAPABILITY_MANDATORY: 绑定到具体 capability scope，只影响该 capability readiness
enum class SubscriptionCriticality : uint8_t {
    OPTIONAL            = 0,
    GLOBAL_MANDATORY    = 1,
    CAPABILITY_MANDATORY = 2
};

inline std::string subscriptionCriticalityToString(SubscriptionCriticality c) {
    switch (c) {
        case SubscriptionCriticality::OPTIONAL:             return "OPTIONAL";
        case SubscriptionCriticality::GLOBAL_MANDATORY:     return "GLOBAL_MANDATORY";
        case SubscriptionCriticality::CAPABILITY_MANDATORY: return "CAPABILITY_MANDATORY";
        default: return "OPTIONAL";
    }
}

inline bool subscriptionCriticalityFromString(std::string_view s,
                                              SubscriptionCriticality& out) {
    if (s == "OPTIONAL")             { out = SubscriptionCriticality::OPTIONAL; return true; }
    if (s == "GLOBAL_MANDATORY")     { out = SubscriptionCriticality::GLOBAL_MANDATORY; return true; }
    if (s == "CAPABILITY_MANDATORY") { out = SubscriptionCriticality::CAPABILITY_MANDATORY; return true; }
    return false;
}

/// CR-015 SubscriptionCatalog 契约版本；catalog-governed route 需要双方一致
constexpr uint32_t kSubscriptionCatalogContractVersion = 1;

/// CR-015 capability: 安全 keyprov 能力名（同时作为 readiness_scope 值）
constexpr const char* kCapabilitySecurityKeyprov = "security.keyprov";

/// CR-015 catalog-governed route id
constexpr const char* kKeyprovUplinkRouteId   = "keyprov.uplink";
constexpr const char* kKeyprovDownlinkRouteId = "keyprov.downlink";

/// CR-015 keyprov payload 上限（上下行一致，PAR-PROTO-DSN-CR-006）
constexpr size_t kKeyprovMaxPayloadBytes = 16 * 1024;

/// CR-017 §6 稳定 reason code（脱敏，不含身份/Topic/payload）
///
/// 调用方必须按 reason 判断处理方式，不能只看数字码：
/// - STALE 应刷新快照、等待就绪并对账；不得误判为 PROV 绑定故障；
/// - CONFLICT 不能靠原样重试解决。
namespace session_reason {
constexpr const char* kSessionIdentityMissing   = "SESSION_IDENTITY_MISSING";
constexpr const char* kSessionIdentityInvalid   = "SESSION_IDENTITY_INVALID";
constexpr const char* kSessionIdentityConflict  = "SESSION_IDENTITY_CONFLICT";
constexpr const char* kPublishContextRequired   = "PUBLISH_SESSION_CONTEXT_REQUIRED";
constexpr const char* kPublishContextStale      = "PUBLISH_SESSION_CONTEXT_STALE";
constexpr const char* kPublishContextUnsupported = "PUBLISH_SESSION_CONTEXT_UNSUPPORTED";
} // namespace session_reason

/// CR-015 稳定 reason code（脱敏，不含身份/Topic/payload）
namespace capability_reason {
constexpr const char* kKeyprovDisabled          = "KEYPROV_DISABLED";
constexpr const char* kKeyprovContractUnsupported = "KEYPROV_CONTRACT_UNSUPPORTED";
constexpr const char* kKeyprovSnapshotRejected  = "KEYPROV_SNAPSHOT_REJECTED";
constexpr const char* kKeyprovSubscribePending  = "KEYPROV_SUBSCRIBE_PENDING";
constexpr const char* kKeyprovSubackRejected    = "KEYPROV_SUBACK_REJECTED";
constexpr const char* kKeyprovTopicNotAuthorized = "KEYPROV_TOPIC_NOT_AUTHORIZED";
constexpr const char* kKeyprovGenerationStale   = "KEYPROV_GENERATION_STALE";
constexpr const char* kKeyprovPayloadTooLarge   = "KEYPROV_PAYLOAD_TOO_LARGE";
constexpr const char* kKeyprovDownlinkBackpressure = "KEYPROV_DOWNLINK_BACKPRESSURE";
} // namespace capability_reason

/// 单条业务订阅项 (CR-007 §3; TSP DSN-CR-004 §3; CR-015 §3)
struct SubscriptionItem {
    std::string route_id;        // owner 内稳定唯一
    std::string topic_template;  // 保存模板，不固化设备实例值（{ecu_uid} 占位）
    SnapshotDirection direction = SnapshotDirection::UP;
    uint8_t qos = 1;             // 0/1/2
    std::string target;          // 目标处理器，如 tsp.fota
    bool mandatory = false;
    // ---- CR-015 ----
    // enabled 语义与 route 类型相关（DSN-CR-015 §4, §18.2）：
    // - legacy route: nullopt 解释为 enabled=true（保持兼容）
    // - capability-gated route: nullopt 解释为 enabled=false（必须显式开启）
    std::optional<bool> enabled;
    // readiness_scope: 空=全局（进入共享 Cloud Ready）；非空=capability scope
    // catalog-governed route 由 catalog 权威覆盖，不接受调用方伪造
    std::string readiness_scope;
    SubscriptionCriticality criticality = SubscriptionCriticality::OPTIONAL;
};

/// 完整版本化订阅快照 (CR-007 §3; TSP DSN-CR-004 §3; CR-015 §3)
struct OwnerSubscriptionSnapshot {
    std::string owner;                       // 稳定服务身份，如 "tsp"
    uint64_t generation = 0;                 // 集合语义变化时单调递增
    std::string content_digest;              // 规范化集合摘要（owner 生成, SHA-256 hex）
    bool registration_complete = false;      // 是否包含全部 Mandatory 项
    // CR-015: catalog/contract 版本；catalog-governed route 需要 == kSubscriptionCatalogContractVersion
    // 缺失(0)/未知/不支持时 catalog route 不得激活，返回 KEYPROV_CONTRACT_UNSUPPORTED
    uint32_t contract_version = 0;
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

/// Cloud Readiness 契约版本 (DSN-CR-014 §7; DSN-CR-017 §4)
/// daemon 与 SDK 通过 IPC 传递该版本号；SDK 遇到缺失或不兼容版本时返回
/// UNSUPPORTED_CONTRACT_VERSION，不得伪造空证书身份后宣告可用。
/// CR-017: v3 覆盖完整身份绑定能力（readiness 暴露 ecu_uid + session_context，
/// 且 daemon 对 keyprov publish 执行 expected_session guard）。
constexpr uint32_t kCloudReadinessContractVersion = 3;

/// 身份绑定发布契约版本 (DSN-CR-017 §4, §19.6)
/// 与 readiness 契约分别协商：仅 readiness 升到 v3 不自动代表 publish guard 可用。
/// SDK 依此字段判断 daemon 是否接受 expected_session，缺失/不兼容时 keyprov not-ready。
constexpr uint32_t kPublishSessionContractVersion = 1;

/// Capability Readiness 契约版本 (DSN-CR-015 §18.4)
constexpr uint32_t kCapabilityReadinessContractVersion = 1;

/// Capability-scoped readiness 状态 (DSN-CR-015 §3, §18.3)
///
/// keyprov 属于 CAPABILITY_MANDATORY(security.keyprov)，不进入共享 Cloud Ready。
/// ready = cloud_ready && enabled && contract_supported && snapshot_accepted
///         && uplink_ready && downlink_suback_ready。
struct CapabilityReadiness {
    std::string capability;              // 如 security.keyprov
    bool enabled = false;                // TSP 显式开启
    bool contract_supported = false;     // 双方 contract_version 兼容
    bool snapshot_accepted = false;      // 携带该 capability 的快照已被本地接受
    bool uplink_ready = false;           // 上行 route 已投影 ACTIVE
    bool downlink_suback_ready = false;  // 下行 SUBACK 已就绪
    bool ready = false;                  // 综合能力就绪
    uint64_t owner_generation = 0;       // 提交快照的 owner generation
    uint64_t connection_generation = 0;  // 当前连接 generation
    uint64_t cert_session_generation = 0; // 本次 TLS 会话 generation
    std::string reason_code;             // 非 ready 原因（KEYPROV_* 等）
    uint64_t event_sequence = 0;         // capability 事件序列（单调递增）
};

/// Capability Readiness 事件回调 (DSN-CR-015 §18.3)
using CapabilityReadinessCallback = std::function<void(const CapabilityReadiness&)>;

/// Cloud Readiness 权威快照 (DSN-CR-014 §3, §5, §12.5)
///
/// 由 MQTT daemon 生成，client 只读。cloud_ready 与本次 TLS context 实际装载的
/// leaf 客户端证书身份绑定：cert_session_generation/device_identity_digest/
/// certificate_ref 均来自固化到 TLS context 的 leaf 证书，调用方不得提供或覆盖。
///
/// 兼容策略 (DSN-CR-014 §5)：在 CR-007 诊断字段基础上扩展；旧调用方忽略新增字段。
struct CloudReadinessSnapshot {
    // ---- CR-014 §3: Cloud Ready 权威状态 ----
    bool cloud_ready = false;               // CLOUD_READY 综合态
    uint64_t connection_generation = 0;     // 连接 generation
    uint64_t cert_session_generation = 0;   // 本次 TLS 会话不可复用 generation
    std::string credential_id;              // 审计/轮换关联（非秘密）
    uint64_t credential_version = 0;        // credential 版本
    std::string device_identity_digest;     // sha256(leaf DER)
    std::string certificate_ref;            // sha256:<hex fingerprint>
    std::string reason_code;                // 非 ready 时的原因（MQTT-1011 等）
    uint64_t event_sequence = 0;            // daemon 生命周期内单调递增，用于发现事件缺口
    // ---- CR-017 §4: 当前 mTLS 会话身份投影（只读，daemon 生成）----
    // ecu_uid：来自实际装入 TLS context 的 leaf 客户端证书 CN 的规范化值。
    // 与 device_identity_digest/certificate_ref/两个 generation 来自同一不可变对象；
    // 无有效会话时为空，且不得声明 ready。
    std::string ecu_uid;
    SessionContext session_context;         // 可校验的 daemon epoch + 两个 generation
    // CR-017：身份绑定发布契约版本（与 readiness 契约独立协商）
    uint32_t publish_contract_version = 0;  // 0=旧 daemon 未提供
    // ---- CR-007 兼容诊断字段（旧调用方忽略）----
    bool subscriptions_ready = false;       // Mandatory 全部 ACTIVE
    bool connected = false;                 // CONNACK 成功（含 SUBSCRIBING/CLOUD_READY/DEGRADED）
    std::map<std::string, uint64_t> owner_generations;  // owner -> 最新已接受 generation
    uint32_t mandatory_count = 0;           // 当前 mandatory 项总数
    uint32_t mandatory_active = 0;          // 已 SUBACK ACTIVE 的 mandatory 项数
    // ---- CR-014 §7: 契约版本兼容 ----
    uint32_t contract_version = 0;          // daemon 声明的契约版本（0=旧 daemon 未提供）
    bool contract_supported = true;         // false = UNSUPPORTED_CONTRACT_VERSION
    // ---- CR-015 §18.3: capability readiness map（与独立查询读取同一原子状态）----
    std::map<std::string, CapabilityReadiness> capabilities;
};

/// 源兼容别名：既有调用方仍可使用 SubscriptionReadiness。
using SubscriptionReadiness = CloudReadinessSnapshot;

/// Cloud Readiness 事件回调 (DSN-CR-014 §5)
/// 在独立执行器（非 IPC 读线程）中调用，携带完整快照。
using CloudReadinessCallback = std::function<void(const CloudReadinessSnapshot&)>;

} // namespace mqtt
} // namespace tbox
