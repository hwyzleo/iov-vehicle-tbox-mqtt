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
#include "tbox/mqtt/types.h"  // ConnectionState, QoS, ErrorCode + helpers
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
 * 将 topic 模板化，移除设备实例值 (CR-003 §5, CR-006 §6.2)
 * 例如: vehicle/HWYZTEST900000001-00000001AA00000001/up/fota
 *    -> vehicle/{ecu_uid}/up/fota
 * 若无法模板化则输出不可逆摘要: t_<8位hex>
 */
std::string topicTemplate(const std::string& topic);

/**
 * 对 clientId 生成不可逆摘要 (CR-003 §5)
 * 输出格式: "c_<8位hex>"
 */
std::string clientIdHash(const std::string& client_id);

/**
 * 对 VIN 生成不可逆摘要 (CR-006 §8)
 * 输出格式: "v_<8位hex>"
 */
std::string vinHash(const std::string& vin);

/**
 * 对 SN 生成不可逆摘要 (CR-006 §8)
 * 输出格式: "s_<8位hex>"
 */
std::string snHash(const std::string& sn);

/**
 * 对 ECU UID 生成不可逆摘要 (CR-006 §8)
 * 输出格式: "e_<8位hex>"
 */
std::string ecuUidHash(const std::string& ecu_uid);

/**
 * 对 msg_id 生成不可逆摘要 (CR-003 §5)
 * 输出格式: "m_<8位hex>"
 */
std::string msgIdHash(const std::string& msg_id);

} // namespace mqtt
} // namespace tbox
