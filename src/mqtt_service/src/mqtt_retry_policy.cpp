#include "mqtt_retry_policy.h"

namespace tbox {
namespace mqtt {

MqttRetryPolicy::Category MqttRetryPolicy::categorize(uint32_t method_id) {
    switch (static_cast<ipc::MethodId>(method_id)) {
        // 只读/幂等：允许一次重试 (CR-004 §7.1)
        case ipc::MethodId::GET_CONNECTION_STATE:
        case ipc::MethodId::REGISTER_ROUTE:  // 同 route key 覆盖，幂等
            return Category::kReadOnlyIdempotent;

        // publish：相同 msg_id 可去重时允许一次重试 (CR-004 §5.2, §7.1)
        // 实际是否去重由 Server 端按 msg_id 判定；client 侧允许重试一次
        case ipc::MethodId::PUBLISH:
            return Category::kPublishDedupable;

        // 一次性：subscribe/unsubscribe 禁止自动重放 (避免重复登记/移除)
        case ipc::MethodId::SUBSCRIBE:
        case ipc::MethodId::UNSUBSCRIBE:
            return Category::kOneShot;

        default:
            return Category::kOneShot;  // 未知方法保守处理
    }
}

bool MqttRetryPolicy::should_retry(uint32_t method_id) {
    Category cat = categorize(method_id);
    // publish 的去重由 Server 端保障，client 侧允许一次重试
    return cat == Category::kReadOnlyIdempotent ||
           cat == Category::kPublishDedupable;
}

} // namespace mqtt
} // namespace tbox
