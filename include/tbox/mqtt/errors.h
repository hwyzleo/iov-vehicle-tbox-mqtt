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
    MQTT_1004 = 1004,  // 上行 topic / 消息格式非法
    MQTT_1005 = 1005,  // 发布超时 / 未确认（QoS 重发耗尽）
    MQTT_1006 = 1006,  // 离线缓存已满（消息被丢弃）
    MQTT_1007 = 1007,  // 下行消息无法路由 / 无订阅者
    MQTT_1008 = 1008,  // 订阅失败
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
        default: return "MQTT-UNKNOWN";
    }
}

} // namespace mqtt
} // namespace tbox
