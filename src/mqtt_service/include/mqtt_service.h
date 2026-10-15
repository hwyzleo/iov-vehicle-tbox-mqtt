#pragma once

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "mqtt_dispatch_queue.h"
#include "owner_snapshot_registry.h"
#include "effective_subscription_table.h"
#include "startup_subscription_gate.h"
#include "applied_subscription_manifest.h"
#include "broker_subscription_reconciler.h"
#include "cloud_readiness_gate.h"
#include "topic_template_resolver.h"
#include "topic_projection_builder.h"
#include "effective_route_table.h"
#include "route_publisher.h"
#include "downlink_route_dispatcher.h"
#include "legacy_topic_guard.h"
#include "store.h"
#include "ipc.h"
#include <functional>
#include <memory>
#include <mutex>

namespace tbox {
namespace mqtt {

// Forward declarations
class ConnectionStateMachine;
class MqttClientManager;
class OfflineCacheQueue;
class RouteBridge;
class ConfigManager;
class CredentialManager;
class SecTlsKeyAdapter;
class ProvisionIdentityManager;

/// Main MQTT service (设计 §5, CR-004 §2)
///
/// 拥有 Broker 连接、QoS/PUBACK、离线队列和路由业务状态；
/// 通过 framework-ipc Server 对外提供 publish/subscribe/route/state 接口。
///
/// CR-008: 外部依赖（Store / ProvisionIdentityManager / CredentialManager /
/// SecTlsKeyAdapter）由 MqttApplication 组合根注入；本类保留 broker 运行时、
/// 离线队列、订阅子系统与 IPC 栈的内聚所有权。生命周期拆为 initializeLocal()
/// （本地能力）与 startExternal()（PROV/SEC/TLS/Broker），停机拆为分阶段方法。
class MqttService : public MqttIpcDispatcher::ServiceHandler, public BrokerOperations {
public:
    /// CR-008: 构造接收注入的外部依赖（所有权归 MqttApplication）。
    /// @param store      framework-store（运行时状态/可更新连接参数）
    /// @param prov_mgr   PROV 身份管理器
    /// @param cred_mgr   SEC 凭据管理器
    /// @param tls_adapter SEC TLS key adapter
    MqttService(std::shared_ptr<hwyz::store::Store> store,
                ProvisionIdentityManager& prov_mgr,
                CredentialManager& cred_mgr,
                SecTlsKeyAdapter& tls_adapter);
    ~MqttService();

    // ---- CR-008 分阶段生命周期 ----
    // initializeLocal: 配置/离线队列/路由/订阅子系统/IPC Server（本地能力先可用）
    bool initializeLocal();
    // startExternal: PROV/SEC/TLS/Broker 状态机（外部依赖与云端连接）
    bool startExternal();

    // 分阶段停机（CR-008 §7.1，由 MqttApplication::cleanup 按序调用）
    void beginShutdown();                  // STOPPING、退出 Cloud Ready、拒绝新请求
    void stopBrokerAndPersistInflight();   // 停重连/TLS，保存未 PUBACK 消息
    void stopExternalCallbacks();          // SEC/PROV callback stop + join
    void shutdown();                       // 停订阅/队列 worker，flush queue，释放 IPC
    // 便捷停机（测试用，等价于依次调用上述四步）
    void stop();

    // ---- IPC Server (CR-004 §2, §4) ----
    // 启动 framework-ipc Server + Dispatcher + DispatchQueue
    bool startIpcServer(const MqttIpcConfig& ipc_config);
    void stopIpcServer();

    // ---- 直接 API（daemon 内部/测试用，CR-004 §5）----
    // publish（envelope 版，msg_id 必填，CR-004 §5.1）
    PublishResult publishEnvelope(const PublishEnvelope& env);

    // publish（便捷版，自动生成 msg_id）
    PublishResult publish(const std::string& topic,
                          const std::vector<uint8_t>& payload,
                          QoS qos,
                          Priority priority = Priority::NORMAL);

    // CR-009 §6.1: route-based 发布（按 owner/route_id）
    PublishResult publishRoute(const RoutePublishRequest& req);

    // subscribe（直接登记下行路由）
    SubscribeResult subscribe(const std::string& topic,
                              QoS qos,
                              const std::string& target_service);

    // registerRoute（维护 internal_addr <-> topic，同 key 覆盖）
    bool registerRoute(const std::string& internal_addr,
                       const std::string& mqtt_topic,
                       RouteEntry::Direction direction);

    // getConnectionState
    ConnectionStateResult getConnectionState() const;

    // CR-009: 获取当前不可变路由投影（测试/诊断用）
    std::shared_ptr<const EffectiveRouteTable> getRouteTable() const;
    // CR-009: 重建路由投影（测试/身份变化后用）
    void rebuildRouteProjection();

    // applyConfig（source=OTA 写 store 覆盖）
    bool applyConfig(const ConnectionConfig& config,
                     ConnectionConfig::Source source);

    // ---- MqttIpcDispatcher::ServiceHandler 实现 (CR-004 §3) ----
    std::pair<int32_t, bool> onPublish(const PublishEnvelope& env) override;
    std::pair<int32_t, bool> onSubscribe(const Route& route, int client_fd) override;
    std::pair<int32_t, bool> onSubscribeEventChannel(int client_fd) override;
    std::pair<int32_t, bool> onUnsubscribe(int client_fd) override;
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry& entry) override;
    std::string onGetConnectionState() override;
    // ---- CR-007 §4: 订阅快照与 Cloud Readiness ----
    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot& snapshot) override;
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string& owner, uint64_t generation) override;
    SubscriptionReadiness onGetSubscriptionReadiness() override;
    // ---- BrokerOperations (CR-007 §8, reconciler 调用) ----
    bool brokerSubscribe(const std::string& topic, uint8_t qos) override;
    bool brokerUnsubscribe(const std::string& topic) override;

private:
    // Connection state changed callback
    void onConnectionStateChanged(ConnectionState new_state);

    // Message received from cloud (下行, CR-004 §6)
    void onMessageReceived(const std::string& topic,
                           const std::vector<uint8_t>& payload);

    // 构建下行事件 payload JSON（topic + payload_base64）
    std::string buildDownlinkEventPayload(const std::string& topic,
                                          const std::vector<uint8_t>& payload) const;

    // CR-009 §7: 构建 route-based 下行事件 payload JSON
    std::string buildRoutedDownlinkPayload(const RoutedDownlinkEnvelope& event) const;

    // 向已订阅 client_fd 推送下行事件
    void pushDownlinkEvent(const std::string& topic,
                           const std::vector<uint8_t>& payload);

    // CR-009 §7: 向已订阅 client_fd 推送 route-based 下行事件
    void pushRoutedDownlinkEvent(const RoutedDownlinkEnvelope& event);

    std::unique_ptr<ConnectionStateMachine> state_machine_;
    std::unique_ptr<MqttClientManager> client_manager_;
    std::unique_ptr<OfflineCacheQueue> cache_queue_;
    std::unique_ptr<RouteBridge> route_bridge_;
    std::unique_ptr<ConfigManager> config_manager_;
    // CR-008: 外部依赖以引用注入，所有权归 MqttApplication
    ProvisionIdentityManager& prov_mgr_;
    CredentialManager& cred_mgr_;
    SecTlsKeyAdapter& tls_adapter_;
    std::shared_ptr<hwyz::store::Store> store_;  // 注入（ConfigManager/StateMachine 需 shared_ptr）

    // IPC (CR-004 §2)
    std::unique_ptr<::tbox::fw::ipc::Server> ipc_server_;
    std::unique_ptr<MqttIpcDispatcher> ipc_dispatcher_;
    std::unique_ptr<MqttDispatchQueue> dispatch_queue_;
    MqttIpcConfig ipc_config_;
    bool ipc_reject_new_ = false;  // 停止时拒绝新请求 (CR-004 §4)

    // CR-006 §7: 连接构造时捕获的身份 generation，CONNACK 后核对
    uint64_t connect_identity_generation_ = 0;

    // CR-017 §5, §19.3: 串行化「校验 expected_session + 选择连接 + 提交发送」与
    // 会话身份切换（setTlsSessionIdentity/revokeSession），消除 TOCTOU。
    std::mutex session_guard_mutex_;

    // ---- CR-007 §2 订阅快照与 Cloud Readiness 组件 ----
    std::unique_ptr<OwnerSnapshotRegistry> snapshot_registry_;
    std::unique_ptr<EffectiveSubscriptionTable> effective_table_;
    std::unique_ptr<StartupSubscriptionGate> subscription_gate_;
    std::unique_ptr<BrokerSubscriptionReconciler> reconciler_;
    std::unique_ptr<AppliedSubscriptionManifestStore> manifest_store_;
    std::unique_ptr<CloudReadinessGate> cloud_ready_gate_;
    SubscriptionConfig subscription_config_;
    bool ipc_started_ = false;  // CR-007 §6: IPC Server 先于首次 CONNECT

    // ---- CR-009: Topic 模板与 route-based 路由投影 ----
    std::unique_ptr<TopicTemplateResolver> template_resolver_;
    std::unique_ptr<TopicProjectionBuilder> projection_builder_;
    std::shared_ptr<const EffectiveRouteTable> route_table_;  // 原子替换的不可变投影
    std::unique_ptr<RoutePublisher> route_publisher_;  // CR-009 §6
    std::unique_ptr<DownlinkRouteDispatcher> downlink_dispatcher_;  // CR-009 §7
    std::unique_ptr<LegacyTopicGuard> legacy_guard_;  // CR-009 §10.2

    // CR-007 辅助
    void onSnapshotAccepted(const std::string& owner, bool registration_complete);
    void reconcileSubscriptions();
    void evaluateCloudReadiness();
    void publishCloudReadyEvent(const CloudReadinessSnapshot& snapshot);
    // CR-015: capability readiness 刷新与事件发布
    void refreshCapabilityReadiness();
    void publishCapabilityEvent(const CapabilityReadiness& readiness);
    void refreshUpstreamRoutes();
    void checkStartupGate();

    // ---- CR-009: 路由投影 ----
    PublishResult onPublishRoute(const RoutePublishRequest& req) override;
    // ---- CR-010 §3: routed downlink 事件订阅 ----
    std::pair<int32_t, bool> onSubscribeRoutedDownlinkEventChannel(
        const std::string& owner, int client_fd) override;
    // ---- CR-014 §5: Cloud Ready 事件订阅 ----
    std::pair<int32_t, bool> onSubscribeCloudReadinessEventChannel(int client_fd) override;
    // ---- CR-015 §18.4: capability readiness ----
    CapabilityReadiness onGetCapabilityReadiness(const std::string& capability) override;
    std::pair<int32_t, bool> onSubscribeCapabilityReadinessEventChannel(
        const std::string& capability, int client_fd) override;
};

} // namespace mqtt
} // namespace tbox
