#pragma once

/**
 * TBOX-MQTT 上下文传播与脱敏辅助 (CR-003 §3, §5)
 *
 * 使用 framework-log 提供的 ContextScope 和 LogContext。
 * 本头文件提供便捷的工厂函数和脱敏辅助函数。
 *
 * 关联 ID 不得取自 clientId、VIN/SN、证书、Token、原始 topic 或 payload (CR-003 §3)。
 */

#include "log.h"
#include "log_types.h"
#include "mqtt_types.h"
#include <string>
#include <string_view>

namespace tbox {
namespace mqtt {

// ============================================================
// 上下文传播辅助 (CR-003 §3)
// ============================================================

/**
 * 创建 ContextScope 的便捷函数 (CR-003 §3)
 *
 * 用法：
 *   auto scope = makeContextScope(trace_id, request_id, session_id);
 *   // scope 离开作用域时自动清理上下文
 */
[[nodiscard]] inline tbox::fw::log::ContextScope makeContextScope(
    const std::string& trace_id,
    const std::string& request_id,
    const std::string& session_id = ""
) {
    tbox::fw::log::LogContext ctx;
    ctx.trace_id = trace_id;
    ctx.request_id = request_id;
    ctx.session_id = session_id;
    return tbox::fw::log::ContextScope(ctx);
}

/**
 * 生成请求 ID（用于下行消息单次处理，CR-003 §3）
 * 格式: mqtt-<timestamp>-<random>
 */
std::string generateRequestId();

// ============================================================
// 脱敏辅助函数 (CR-003 §5)
// ============================================================

/**
 * 对 Broker 主机/IP 生成不可逆摘要 (CR-003 §5)
 * 输出格式: "h_<8位hex>"
 */
std::string brokerHash(const std::string& broker_host);

/**
 * 将 topic 模板化，移除设备实例值 (CR-003 §5)
 * 例如: vehicle/HWYZTEST900000001-00000001AA00000001/up/fota
 *    -> vehicle/{device_sn}/up/fota
 * 若无法模板化则输出不可逆摘要: t_<8位hex>
 */
std::string topicTemplate(const std::string& topic);

/**
 * 对 clientId 生成不可逆摘要 (CR-003 §5)
 * 输出格式: "c_<8位hex>"
 */
std::string clientIdHash(const std::string& client_id);

/**
 * 对 msg_id 生成不可逆摘要 (CR-003 §5)
 * 输出格式: "m_<8位hex>"
 */
std::string msgIdHash(const std::string& msg_id);

/**
 * 将 QoS 枚举转为可读字符串
 */
inline std::string qosToString(QoS qos) {
    switch (qos) {
        case QoS::AT_MOST_ONCE:  return "0";
        case QoS::AT_LEAST_ONCE: return "1";
        case QoS::EXACTLY_ONCE:  return "2";
        default: return "?";
    }
}

/**
 * 将 ConnectionState 枚举转为可读字符串
 */
inline std::string connectionStateToString(ConnectionState state) {
    switch (state) {
        case ConnectionState::INIT:         return "INIT";
        case ConnectionState::CERT_WAIT:    return "CERT_WAIT";
        case ConnectionState::CONNECTING:   return "CONNECTING";
        case ConnectionState::CONNECTED:    return "CONNECTED";
        case ConnectionState::RECONNECTING: return "RECONNECTING";
        case ConnectionState::OFFLINE:      return "OFFLINE";
        default: return "UNKNOWN";
    }
}

/**
 * 将 ErrorCode 枚举转为错误码字符串 (CR-003 §6)
 */
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
