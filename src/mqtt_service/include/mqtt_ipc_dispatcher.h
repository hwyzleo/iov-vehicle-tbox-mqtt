#pragma once

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include "tbox/mqtt/ipc_protocol.h"
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <map>
#include <mutex>
#include <unordered_set>

namespace tbox {
namespace mqtt {

class MqttService;

/// MqttIpcDispatcher 依赖的业务接口 (CR-004 §3)
///
/// 将 framework-ipc 的 RequestHandler 签名适配到 MQTT 业务 handler。
/// 只负责 JSON 解码/编码、调用业务 handler 及 MQTT-10xx 映射，
/// 不复制 socket 逻辑（由 framework-ipc Server 负责）。
///
/// 响应 JSON 中嵌入 `status` 字段（MQTT-10xx 业务状态码），
/// framework 在 ResponseHeader.status_code 中写入 0（传输成功）或 FW-03xx（传输失败）。
///
/// 适配层不接触 payload 明文日志：IPC logger 只记录 msg_id_hash、topic_template、qos、
/// payload_bytes、status、duration、peer (CR-004 §8)。
class MqttIpcDispatcher {
public:
    /// 业务 handler 接口（MqttService 实现，便于单元测试解耦）
    class ServiceHandler {
    public:
        virtual ~ServiceHandler() = default;
        /// publish：校验并接管消息，返回 (业务状态码, accepted)
        virtual std::pair<int32_t, bool> onPublish(const PublishEnvelope& env) = 0;
        /// subscribe（带 route）：登记 route + Broker 订阅，返回 (业务状态码, ok)
        virtual std::pair<int32_t, bool> onSubscribe(const Route& route, int client_fd) = 0;
        /// subscribe 事件通道（空 params）：仅登记 framework 事件订阅，返回 (业务状态码, ok)
        virtual std::pair<int32_t, bool> onSubscribeEventChannel(int client_fd) = 0;
        /// unsubscribe：移除连接订阅，返回 (业务状态码, ok)
        virtual std::pair<int32_t, bool> onUnsubscribe(int client_fd) = 0;
        /// registerRoute：维护 internal_addr <-> topic，返回 (业务状态码, ok)
        virtual std::pair<int32_t, bool> onRegisterRoute(const RouteEntry& entry) = 0;
        /// getConnectionState：只读连接状态，返回 JSON 片段
        virtual std::string onGetConnectionState() = 0;
        // ---- CR-007 §4: 订阅快照与 Cloud Readiness ----
        /// replaceSubscriptionSnapshot：原子接受/拒绝完整快照
        virtual ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
            const OwnerSubscriptionSnapshot& snapshot) = 0;
        /// getSubscriptionSnapshotStatus：幂等查询接受状态
        virtual SnapshotStatusResult onGetSubscriptionSnapshotStatus(
            const std::string& owner, uint64_t generation) = 0;
        /// getSubscriptionReadiness：Cloud Ready 诊断
        virtual SubscriptionReadiness onGetSubscriptionReadiness() = 0;
    };

    explicit MqttIpcDispatcher(ServiceHandler* service);

    /// framework RequestHandler 回调入口 (CR-004 §3)
    /// @param method_id    MQTT method ID
    /// @param params_json  请求参数 JSON
    /// @param client_fd    客户端 fd（用于订阅管理）
    /// @return 响应 JSON 字符串（含 status 字段）
    std::string dispatch(uint32_t method_id,
                         std::string_view params_json,
                         int client_fd);

    /// client 断连清理：移除 per-fd 订阅，不自动删除持久 RouteEntry (CR-004 §4)
    void onClientDisconnected(int client_fd);

    /// 查询 msg_id 是否已被受理（去重，CR-004 §5.2）
    bool isAccepted(const std::string& msg_id) const;

private:
    ServiceHandler* service_;

    // 各 method handler，返回 (业务状态码, response_json)
    std::pair<int32_t, std::string> handlePublish(std::string_view params);
    std::pair<int32_t, std::string> handleSubscribe(std::string_view params, int client_fd);
    std::pair<int32_t, std::string> handleUnsubscribe(int client_fd);
    std::pair<int32_t, std::string> handleRegisterRoute(std::string_view params);
    std::pair<int32_t, std::string> handleGetConnectionState();
    // CR-007 §4
    std::pair<int32_t, std::string> handleReplaceSubscriptionSnapshot(std::string_view params);
    std::pair<int32_t, std::string> handleGetSubscriptionSnapshotStatus(std::string_view params);
    std::pair<int32_t, std::string> handleGetSubscriptionReadiness();

    // per-fd 订阅集合（client 断连时清理，CR-004 §4）
    std::map<int, std::unordered_set<std::string>> fd_subscriptions_;
    mutable std::mutex fd_mutex_;

    // msg_id 去重表（进程生命周期内，accepted 状态，CR-004 §5.2）
    std::unordered_set<std::string> accepted_msg_ids_;
    mutable std::mutex dedup_mutex_;
};

} // namespace mqtt
} // namespace tbox
