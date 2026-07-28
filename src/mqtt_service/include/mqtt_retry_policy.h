#pragma once

#include <cstdint>
#include "tbox/mqtt/ipc_protocol.h"

namespace tbox {
namespace mqtt {

/// MQTT IPC 方法重试策略 (CR-004 §5.2, §7.1)
///
/// framework-ipc Client 提供传输失败后的单次重连能力，
/// MqttRetryPolicy 按 method 分类决定是否允许自动重放：
///
/// - 只读/幂等（getConnectionState、registerRoute 同 key 覆盖）：允许一次重试
/// - publish：仅在相同 msg_id 可去重时允许一次重试；否则禁止重放（unknown outcome）
/// - subscribe/unsubscribe：一次性，禁止自动重放（避免重复登记/移除）
class MqttRetryPolicy {
public:
    /// 方法安全类别
    enum class Category : uint8_t {
        kReadOnlyIdempotent,  ///< 只读/幂等，允许一次重试
        kPublishDedupable,    ///< publish，相同 msg_id 可去重时允许一次
        kOneShot              ///< 一次性/消费型，禁止重放
    };

    /// 返回指定 method_id 的安全类别
    static Category categorize(uint32_t method_id);

    /// 传输失败后是否允许自动重试
    static bool should_retry(uint32_t method_id);
};

} // namespace mqtt
} // namespace tbox
