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
#include "store.h"
#include "ipc.h"
#include <functional>
#include <memory>

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
class MqttService : public MqttIpcDispatcher::ServiceHandler, public BrokerOperations {
public:
    MqttService();
    ~MqttService();

    // Initialize service with dependencies and framework paths (设计 §5, CR-004 §4, CR-005 §4.2, CR-006 §4)
    // CR-005: 使用 CredentialManager 替代 SecCertInterface
    // CR-006: 使用 ProvisionIdentityManager 获取 PROV 绑定前置
    bool initialize(std::shared_ptr<ProvisionIdentityManager> prov_mgr,
                    std::shared_ptr<CredentialManager> cred_mgr,
                    const std::string& config_root,
                    const std::string& store_root);

    // Start service (state machine + certificate flow)
    bool start();

    // Stop service (CR-004 §4: 先拒新请求 -> 停 IPC -> 等 client 线程 -> 停 Broker -> flush)
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

    // 向已订阅 client_fd 推送下行事件
    void pushDownlinkEvent(const std::string& topic,
                           const std::vector<uint8_t>& payload);

    std::unique_ptr<ConnectionStateMachine> state_machine_;
    std::unique_ptr<MqttClientManager> client_manager_;
    std::unique_ptr<OfflineCacheQueue> cache_queue_;
    std::unique_ptr<RouteBridge> route_bridge_;
    std::unique_ptr<ConfigManager> config_manager_;
    std::shared_ptr<CredentialManager> cred_mgr_;
    std::shared_ptr<ProvisionIdentityManager> prov_mgr_;
    std::unique_ptr<SecTlsKeyAdapter> tls_adapter_;
    std::shared_ptr<hwyz::store::Store> store_;

    // IPC (CR-004 §2)
    std::unique_ptr<::tbox::fw::ipc::Server> ipc_server_;
    std::unique_ptr<MqttIpcDispatcher> ipc_dispatcher_;
    std::unique_ptr<MqttDispatchQueue> dispatch_queue_;
    MqttIpcConfig ipc_config_;
    bool ipc_reject_new_ = false;  // 停止时拒绝新请求 (CR-004 §4)

    // CR-006 §7: 连接构造时捕获的身份 generation，CONNACK 后核对
    uint64_t connect_identity_generation_ = 0;

    // ---- CR-007 §2 订阅快照与 Cloud Readiness 组件 ----
    std::unique_ptr<OwnerSnapshotRegistry> snapshot_registry_;
    std::unique_ptr<EffectiveSubscriptionTable> effective_table_;
    std::unique_ptr<StartupSubscriptionGate> subscription_gate_;
    std::unique_ptr<BrokerSubscriptionReconciler> reconciler_;
    std::unique_ptr<AppliedSubscriptionManifestStore> manifest_store_;
    std::unique_ptr<CloudReadinessGate> cloud_ready_gate_;
    SubscriptionConfig subscription_config_;
    bool ipc_started_ = false;  // CR-007 §6: IPC Server 先于首次 CONNECT

    // CR-007 辅助
    void onSnapshotAccepted(const std::string& owner, bool registration_complete);
    void reconcileSubscriptions();
    void evaluateCloudReadiness();
    void publishCloudReadyEvent(bool ready, uint32_t mandatory_active,
                                uint32_t mandatory_total, const std::string& reason);
    void refreshUpstreamRoutes();
    void checkStartupGate();
};

} // namespace mqtt
} // namespace tbox
