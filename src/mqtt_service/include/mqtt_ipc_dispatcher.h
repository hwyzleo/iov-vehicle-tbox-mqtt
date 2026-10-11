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

/// 序列化 CloudReadinessSnapshot 为 IPC 契约 JSON (CR-014 §3/§4.2/§5)。
/// daemon 的快照查询响应与 CLOUD_READY_CHANGED 事件共用此实现，保证字段集一致。
std::string cloudReadinessToJson(const CloudReadinessSnapshot& snapshot);

/// CR-015: 序列化 CapabilityReadiness 为 IPC 契约 JSON。
/// 查询响应与 CAPABILITY_READINESS_CHANGED 事件共用，字段集一致。
std::string capabilityReadinessToJson(const CapabilityReadiness& readiness);

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
        virtual CloudReadinessSnapshot onGetSubscriptionReadiness() = 0;
        // ---- CR-015 §18.4: capability readiness ----
        /// getCapabilityReadiness：查询 capability-scoped readiness
        /// 默认实现返回空状态（便于既有 stub 不破纲）。
        virtual CapabilityReadiness onGetCapabilityReadiness(
            const std::string& capability) {
            CapabilityReadiness r;
            r.capability = capability;
            return r;
        }
        /// 登记 fd 到 capability 事件通道（默认实现返回成功）
        virtual std::pair<int32_t, bool> onSubscribeCapabilityReadinessEventChannel(
            const std::string& capability, int client_fd) {
            (void)capability; (void)client_fd; return {0, true};
        }
        // ---- CR-009 §6.1: route-based 发布 ----
        /// publishRoute：按 owner/route_id 发布
        virtual PublishResult onPublishRoute(const RoutePublishRequest& req) = 0;
        // ---- CR-010 §3: routed downlink 事件订阅 ----
        /// subscribeRoutedDownlink 事件通道：登记 fd 到 ROUTED_DOWNLINK_MESSAGE 并记录 owner。
        /// 默认实现返回成功（便于既有 stub 不破纲）。
        virtual std::pair<int32_t, bool> onSubscribeRoutedDownlinkEventChannel(
            const std::string& owner, int client_fd) { (void)owner; (void)client_fd; return {0, true}; }
        // ---- CR-014 §5: Cloud Ready 事件订阅 ----
        /// 登记 fd 到 CLOUD_READY_CHANGED 事件通道（不触发 Broker 操作）。
        virtual std::pair<int32_t, bool> onSubscribeCloudReadinessEventChannel(int client_fd) {
            (void)client_fd; return {0, true};
        }
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

    /// CR-010 §3: 查询 owner 的活跃 routed 订阅 fd 列表（daemon 按 owner 定向推送）
    std::vector<int> getRoutedFdsForOwner(const std::string& owner) const;

    /// CR-015 §18.3: 查询 capability 的活跃订阅 fd 列表（daemon 按 capability 定向推送）
    std::vector<int> getCapabilityFds(const std::string& capability) const;

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
    // CR-009 §6.1
    std::pair<int32_t, std::string> handlePublishRoute(std::string_view params);
    // CR-010 §3
    std::pair<int32_t, std::string> handleSubscribeRoutedDownlink(std::string_view params, int client_fd);
    // CR-014 §5
    std::pair<int32_t, std::string> handleSubscribeCloudReadiness(int client_fd);
    // CR-015 §18.4
    std::pair<int32_t, std::string> handleGetCapabilityReadiness(std::string_view params);
    std::pair<int32_t, std::string> handleSubscribeCapabilityReadiness(std::string_view params, int client_fd);

    // per-fd 订阅集合（client 断连时清理，CR-004 §4）
    std::map<int, std::unordered_set<std::string>> fd_subscriptions_;
    mutable std::mutex fd_mutex_;

    // CR-010 §3: routed downlink fd<->owner 注册表（daemon 按 owner 定向推送）
    std::unordered_map<int, std::string> fd_routed_owner_;
    std::unordered_map<std::string, std::unordered_set<int>> routed_owner_fds_;
    mutable std::mutex routed_mutex_;

    // CR-015 §18.3: capability fd<->capability 注册表（daemon 按 capability 定向推送）
    std::unordered_map<int, std::string> fd_capability_;
    std::unordered_map<std::string, std::unordered_set<int>> capability_fds_;
    mutable std::mutex capability_mutex_;

    // msg_id 去重表（进程生命周期内，accepted 状态，CR-004 §5.2）
    std::unordered_set<std::string> accepted_msg_ids_;
    mutable std::mutex dedup_mutex_;
};

} // namespace mqtt
} // namespace tbox
