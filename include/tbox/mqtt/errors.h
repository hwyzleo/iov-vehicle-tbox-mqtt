#pragma once

// TBOX-MQTT 公共错误码 (CR-004 §3, 设计 §6)
//
// 业务错误码 MQTT-1001~1008 嵌入 IPC 响应 JSON 的 status 字段；
// framework-ipc 传输层错误码 FW-0301~0307 写入 ResponseHeader.status_code。
// 两者语义独立：传输成功(fw_status==0) 时再从 JSON 解析业务 status。

#include <string>

namespace tbox {
namespace mqtt {

/// MQTT 业务错误码 (设计 §6, 需求 §6)
enum class ErrorCode : uint32_t {
    MQTT_1001 = 1001,  // 证书/密钥不可用或 mTLS 握手失败
    MQTT_1002 = 1002,  // 网络不可用 / 连接失败
    MQTT_1003 = 1003,  // 连接断开（触发重连）
    MQTT_1004 = 1004,  // 上行 topic / 消息格式非法；CR-017：发布控制元数据缺失或契约不支持
    MQTT_1005 = 1005,  // 发布超时 / 未确认（QoS 重发耗尽）
    MQTT_1006 = 1006,  // 离线缓存已满（消息被丢弃）
    MQTT_1007 = 1007,  // 下行消息无法路由 / 无订阅者
    MQTT_1008 = 1008,  // 订阅失败
    MQTT_1009 = 1009,  // PROV 不可用、未绑定或 VIN/SN/ECU UID 不完整/不一致；CR-017：会话身份无效、冲突或预期会话上下文已过期
    MQTT_1010 = 1010,  // 订阅快照无效、generation/digest 冲突或原子替换失败 (CR-007 §11)
    MQTT_1011 = 1011,  // Mandatory Subscription 未就绪、门闩超时或 Cloud Ready 收敛失败 (CR-007 §11)
    MQTT_1012 = 1012,  // Topic 模板、变量或运行时实例化失败 (CR-009 §11)
    MQTT_1013 = 1013,  // Routed downlink SDK 事件缺失、非法、owner 不匹配或 base64 解码失败 (CR-010 §9)
    // ---- DSN-CR-018 §5: 可信时间门禁类别（已核查本仓 1014~1019 空闲、消费方零引用后正式登记）----
    MQTT_1020 = 1020,  // 时间信任或执行时钟不足（time_untrusted/time_unknown/time_sample_expired/time_clock_not_aligned）→ TIME_WAIT；新上行 retryable unavailable
    MQTT_1021 = 1021,  // 时间依赖／契约不可用（time_provider_unavailable/time_query_timeout/time_contract_unsupported/time_sample_malformed）→ TIME_WAIT；恢复 SEC／持续轮询
    MQTT_1022 = 1022,  // 上行因时间门禁拒绝（publish_time_unavailable）→ accepted=false，关联底层 time_reason
    MQTT_1023 = 1023,  // 历史消息安全隔离／终结（queue_time_provenance_unknown/queue_transport_expired）→ 本地消息状态／计数，不伪造云业务回执
};

/// 将 ErrorCode 转为可读字符串 (CR-003 §6)
inline std::string errorCodeToString(ErrorCode code) {
    switch (code) {
        case ErrorCode::MQTT_1001: return "MQTT-1001";
        case ErrorCode::MQTT_1002: return "MQTT-1002";
        case ErrorCode::MQTT_1003: return "MQTT-1003";
        case ErrorCode::MQTT_1004: return "MQTT-1004";
        case ErrorCode::MQTT_1005: return "MQTT-1005";
        case ErrorCode::MQTT_1006: return "MQTT-1006";
        case ErrorCode::MQTT_1007: return "MQTT-1007";
        case ErrorCode::MQTT_1008: return "MQTT-1008";
        case ErrorCode::MQTT_1009: return "MQTT-1009";
        case ErrorCode::MQTT_1010: return "MQTT-1010";
        case ErrorCode::MQTT_1011: return "MQTT-1011";
        case ErrorCode::MQTT_1012: return "MQTT-1012";
        case ErrorCode::MQTT_1013: return "MQTT-1013";
        case ErrorCode::MQTT_1020: return "MQTT-1020";
        case ErrorCode::MQTT_1021: return "MQTT-1021";
        case ErrorCode::MQTT_1022: return "MQTT-1022";
        case ErrorCode::MQTT_1023: return "MQTT-1023";
        default: return "MQTT-UNKNOWN";
    }
}

} // namespace mqtt
} // namespace tbox
