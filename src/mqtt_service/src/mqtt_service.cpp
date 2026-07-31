#include "mqtt_service.h"
#include "connection_state_machine.h"
#include "mqtt_client_manager.h"
#include "offline_cache_queue.h"
#include "route_bridge.h"
#include "config_manager.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "sec_tls_key_adapter.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "utils.h"
#include <nlohmann/json.hpp>
#include <chrono>

namespace tbox {
namespace mqtt {

namespace {

std::string generateMsgId() {
    return "msg_" + std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
}

std::string b64_encode(const std::vector<uint8_t>& data) {
    return ::hwyz::Utils::base64_encode(
        std::string(reinterpret_cast<const char*>(data.data()), data.size()));
}

} // anonymous namespace

MqttService::MqttService(std::shared_ptr<hwyz::store::Store> store,
                         ProvisionIdentityManager& prov_mgr,
                         CredentialManager& cred_mgr,
                         SecTlsKeyAdapter& tls_adapter)
    : state_machine_(std::make_unique<ConnectionStateMachine>()),
      client_manager_(std::make_unique<MqttClientManager>()),
      cache_queue_(std::make_unique<OfflineCacheQueue>(1000)),
      route_bridge_(std::make_unique<RouteBridge>()),
      config_manager_(std::make_unique<ConfigManager>()),
      prov_mgr_(prov_mgr),
      cred_mgr_(cred_mgr),
      tls_adapter_(tls_adapter),
      store_(std::move(store)),
      snapshot_registry_(std::make_unique<OwnerSnapshotRegistry>()),
      effective_table_(std::make_unique<EffectiveSubscriptionTable>()),
      subscription_gate_(std::make_unique<StartupSubscriptionGate>()),
      reconciler_(std::make_unique<BrokerSubscriptionReconciler>()),
      manifest_store_(std::make_unique<AppliedSubscriptionManifestStore>()),
      cloud_ready_gate_(std::make_unique<CloudReadinessGate>()) {
}

MqttService::~MqttService() {
    stop();
}

bool MqttService::initializeLocal() {
    // CR-008: Application::run() 已完成 Config::load("mqtt")，从全局快照读取配置。
    if (!config_manager_->loadFromGlobal()) {
        return false;
    }

    // Set store and load OTA-updatable params from framework-store (Design §5)
    config_manager_->setStore(store_);
    config_manager_->loadFromStore(*store_);

    // Set store for state machine persistence
    state_machine_->setStore(store_);

    // CR-005 §4.2: 初始化离线队列
    auto queue_config = config_manager_->getOfflineQueueConfig();
    cache_queue_ = std::make_unique<OfflineCacheQueue>(queue_config.max_size);

    // ---- 回调装配（仅注册，实际触发在 startExternal 后）----
    state_machine_->setStateChangeCallback(
        [this](ConnectionState new_state) {
            onConnectionStateChanged(new_state);
        });

    client_manager_->setMessageCallback(
        [this](const std::string& topic, const std::vector<uint8_t>& payload) {
            onMessageReceived(topic, payload);
        });

    client_manager_->setConnectionCallback(
        [this](bool connected) {
            if (connected) {
                state_machine_->onConnectionSuccess();
            } else {
                state_machine_->onDisconnect();
            }
        });

    config_manager_->setChangeCallback(
        [this]() {
            auto config = config_manager_->getConfig();
            // CR-006 §6: Populate identity-derived fields before connect
            auto identity = prov_mgr_.getSnapshot();
            if (identity.client_id.empty() || identity.ecu_uid.empty()) {
                MqttLogAdapter::connection().warn(
                    "mqtt.config.applied",
                    "配置已应用但身份无效，等待 PROV 绑定",
                    {{"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}}
                );
                return;
            }
            config.client_id = identity.client_id;
            config.username = identity.ecu_uid;
            MqttLogAdapter::connection().info(
                "mqtt.config.applied",
                "配置已应用，发起重连",
                {
                    {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                    {"broker_port", tbox::fw::log::FieldValue::makeInt(config.broker_port)}
                }
            );
            client_manager_->connect(config);
        });

    // CR-007 §2: 订阅快照与 Cloud Readiness 组件初始化
    manifest_store_->setStore(store_);
    subscription_config_ = config_manager_->getSubscriptionConfig();
    subscription_gate_->configure(subscription_config_.required_owners,
                                  subscription_config_.startup_wait_timeout_ms);
    reconciler_->setBrokerOperations(this);
    reconciler_->setManifestStore(manifest_store_.get());
    reconciler_->setReconcileTimeout(subscription_config_.reconcile_timeout_ms);
    reconciler_->setSubackRetryMax(subscription_config_.suback_retry_max);
    effective_table_->setEcuUid(prov_mgr_.getEcuUid());

    // CR-007 §9: SUBACK/UNSUBACK 回调 -> reconciler -> 评估 Cloud Ready
    client_manager_->setSubscribeAckCallback(
        [this](const std::string& topic, bool success) {
            if (reconciler_) {
                reconciler_->onSubAck(topic, success);
                evaluateCloudReadiness();
            }
        });
    client_manager_->setUnsubscribeAckCallback(
        [this](const std::string& topic, bool success) {
            if (reconciler_) {
                reconciler_->onUnsubAck(topic, success);
                evaluateCloudReadiness();
            }
        });

    // CR-007 §10: CloudReadinessGate 回调 -> 发布事件 + 状态机
    cloud_ready_gate_->setCallback(
        [this](bool ready, uint32_t ma, uint32_t mt, const std::string& reason) {
            publishCloudReadyEvent(ready, ma, mt, reason);
            if (ready && state_machine_->getCurrentState() == ConnectionState::SUBSCRIBING) {
                state_machine_->onSubscriptionsReady();
            }
        });

    // CR-007 §6: IPC Server 先于首次 CONNECT 可用
    if (!ipc_server_) {
        MqttIpcConfig ipc_config;
        auto snap = config_manager_->getSnapshot();
        if (snap) {
            auto ipc_section = snap->getSection("mqtt.ipc");
            if (ipc_section) {
                ipc_config.socket_path = ipc_section->getString("socket_path", ipc_config.socket_path);
                ipc_config.dispatch_queue_size = static_cast<uint32_t>(
                    ipc_section->getInt("dispatch_queue_size", ipc_config.dispatch_queue_size));
                ipc_config.subscriber_queue_size = static_cast<uint32_t>(
                    ipc_section->getInt("subscriber_queue_size", ipc_config.subscriber_queue_size));
                ipc_config.slow_subscriber_policy = ipc_section->getString(
                    "slow_subscriber_policy", ipc_config.slow_subscriber_policy);
            }
        }
        startIpcServer(ipc_config);
    }

    return true;
}

bool MqttService::startExternal() {
    // CR-006 §4: 初始化 tbox::prov_client (before SEC, per CR-006 §4 init order)
    auto identity_snapshot = config_manager_->getSnapshot();
    if (identity_snapshot) {
        auto identity_section = identity_snapshot->getSection("mqtt.identity");
        if (identity_section) {
            prov_mgr_.setClientIdFormat(
                identity_section->getString("client_id_format", "{vin}-{sn}"));

            auto prov_retry_section = identity_section->getSection("prov_retry");
            if (prov_retry_section) {
                ProvisionRetryConfig prov_config;
                prov_config.initial_backoff = Duration(
                    prov_retry_section->getInt("initial_backoff_ms", 500));
                prov_config.max_backoff = Duration(
                    prov_retry_section->getInt("max_backoff_ms", 30000));
                prov_config.multiplier =
                    prov_retry_section->getDouble("multiplier", 2.0);
                state_machine_->setProvisionRetryConfig(prov_config);
            }
        }
    }
    prov_mgr_.initialize();

    // CR-005 §4.2: 初始化 tbox::sec_client
    cred_mgr_.initialize();

    // CR-005 §6: 设置 CERT_WAIT 退避配置（从 framework-config 读取）
    CredentialPollConfig poll_config;
    auto snapshot = config_manager_->getSnapshot();
    if (snapshot) {
        auto tls_section = snapshot->getSection("mqtt.tls");
        if (tls_section) {
            auto poll_section = tls_section->getSection("credential_poll");
            if (poll_section) {
                poll_config.initial_backoff = Duration(
                    poll_section->getInt("initial_backoff_ms", 1000));
                poll_config.max_backoff = Duration(
                    poll_section->getInt("max_backoff_ms", 60000));
            }
        }
    }
    state_machine_->setCredentialPollConfig(poll_config);

    // CR-005 §4.2: 建立 credential change subscription (在首次获取前)
    // CR-005 §7: payload 仅用于触发，不直接替换凭据；MQTT 必须重新调用 getTlsCredential()
    cred_mgr_.subscribeTlsCredentialChanged([this]() {
        // CR-005 §7 cleanup 顺序: 标记 generation 失效 -> 清理旧 TLS context -> 状态机转 CERT_WAIT
        cred_mgr_.invalidateCredential();
        tls_adapter_.destroyContext();
        client_manager_->disconnect();
        state_machine_->onCredentialChanged();
    });

    // CR-005 §4.2: 获取初始凭据 (getTlsCredential)
    // SEC 不可用或 NOT_READY 时保持 CERT_WAIT (CR-005 §4.2)
    ActiveTlsCredential credential;
    cred_mgr_.getTlsCredential(credential);

    // CR-005 §5: 初始化 MqttClientManager (注入 CredentialManager + SecTlsKeyAdapter)
    client_manager_->initialize(cred_mgr_, tls_adapter_);

    // CR-006 §4: State machine starts in PROV_WAIT; onConnectionStateChanged
    // callback reads PROV binding and transitions to CERT_WAIT if valid.
    state_machine_->start();

    return true;
}

// ============================================================
// CR-008 分阶段停机 (§7.1)
// ============================================================

void MqttService::beginShutdown() {
    // §7.1.1: STOPPING，退出 Cloud Ready，拒绝新 publish/快照/路由更新
    ipc_reject_new_ = true;
    if (cloud_ready_gate_ && cloud_ready_gate_->isCloudReady()) {
        cloud_ready_gate_->setDegraded(true, "shutting_down");
    }
}

void MqttService::stopBrokerAndPersistInflight() {
    // §7.1.2-3: 禁止 CONNECT/重连/TLS/signTls，accepted 未 PUBACK 消息保留原 msg_id
    // 状态机停止 -> 停重连定时器；client 断开（graceful DISCONNECT）
    state_machine_->stop();
    client_manager_->disconnect();
    // 离线队列中 inflight/accepted 消息保留原 msg_id，不静默丢失或生成新 ID
}

void MqttService::stopExternalCallbacks() {
    // §7.1.7: 取消 SEC credential 订阅并 join 回调，之后才可销毁 clients/credential/TLS
    // Broker 网络线程已退出（stopBrokerAndPersistInflight），可安全清理 TLS context
    cred_mgr_.cancelCredentialSubscription();
    tls_adapter_.destroyContext();
    cred_mgr_.cleanupCredential();
}

void MqttService::shutdown() {
    // §7.1.5-6: 停止 IPC Server + dispatch queue（join 本地连接/dispatcher 线程）
    stopIpcServer();
}

void MqttService::stop() {
    // 便捷停机（测试用）：等价于依次调用分阶段方法
    beginShutdown();
    stopBrokerAndPersistInflight();
    shutdown();
    stopExternalCallbacks();
}

// ============================================================
// IPC Server (CR-004 §2, §4)
// ============================================================

bool MqttService::startIpcServer(const MqttIpcConfig& ipc_config) {
    if (ipc_server_) {
        return true;  // already started
    }

    ipc_config_ = ipc_config;
    ipc_reject_new_ = false;

    // Create dispatcher (this implements ServiceHandler)
    ipc_dispatcher_ = std::make_unique<MqttIpcDispatcher>(this);

    // Create dispatch queue: decouple Broker callback from push_event (CR-004 §4, §6)
    auto* server_ptr_placeholder = this;
    dispatch_queue_ = std::make_unique<MqttDispatchQueue>(
        ipc_config.dispatch_queue_size,
        [server_ptr_placeholder](const std::string& topic, const std::vector<uint8_t>& payload) {
            server_ptr_placeholder->pushDownlinkEvent(topic, payload);
        });
    dispatch_queue_->start();

    // Create framework-ipc Server
    ::tbox::fw::ipc::IpcConfig fw_ipc_config;
    fw_ipc_config.max_frame_bytes = 10485760;
    fw_ipc_config.receive_timeout_ms = 60000;
    fw_ipc_config.connect_timeout_ms = 3000;
    fw_ipc_config.listen_backlog = 5;
    ipc_server_ = std::make_unique<::tbox::fw::ipc::Server>(
        ipc_config.socket_path, fw_ipc_config);

    // Register RequestHandler (adapt to dispatcher)
    auto* dispatcher_ptr = ipc_dispatcher_.get();
    auto request_handler = [dispatcher_ptr](uint32_t method_id,
                                            std::string_view params_json,
                                            int client_fd) -> std::string {
        return dispatcher_ptr->dispatch(method_id, params_json, client_fd);
    };

    // disconnect handler: clean per-fd subscriptions (CR-004 §4)
    auto* dispatcher_for_dc = ipc_dispatcher_.get();
    auto disconnect_handler = [dispatcher_for_dc](int client_fd) {
        dispatcher_for_dc->onClientDisconnected(client_fd);
        MqttLogAdapter::ipc().debug(
            "mqtt.ipc.client_disconnected",
            "Client disconnected (framework)",
            {tbox::fw::log::Field("peer", tbox::fw::log::FieldValue::makeInt(client_fd))}
        );
    };

    if (!ipc_server_->start(std::move(request_handler), std::move(disconnect_handler))) {
        ipc_server_.reset();
        ipc_dispatcher_.reset();
        dispatch_queue_->stop();
        dispatch_queue_.reset();
        return false;
    }

    MqttLogAdapter::ipc().info(
        "mqtt.ipc.server_started",
        "IPC server started (framework-ipc)",
        {{"socket_path", tbox::fw::log::FieldValue::makeString(ipc_config.socket_path)},
         {"dispatch_queue_size", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(ipc_config.dispatch_queue_size))},
         {"subscriber_queue_size", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(ipc_config.subscriber_queue_size))},
         {"slow_subscriber_policy", tbox::fw::log::FieldValue::makeString(
             ipc_config.slow_subscriber_policy)}}
    );
    return true;
}

void MqttService::stopIpcServer() {
    if (ipc_server_) {
        ipc_server_->stop();
        ipc_server_.reset();
    }
    if (dispatch_queue_) {
        dispatch_queue_->stop();
        dispatch_queue_.reset();
    }
    if (ipc_dispatcher_) {
        ipc_dispatcher_.reset();
    }
    MqttLogAdapter::ipc().info(
        "mqtt.ipc.server_stopped",
        "IPC server stopped (framework-ipc)"
    );
}

// ============================================================
// 直接 API (CR-004 §5)
// ============================================================

PublishResult MqttService::publishEnvelope(const PublishEnvelope& env) {
    PublishResult result;

    // msg_id 必填 (CR-004 §5.1)
    if (env.msg_id.empty() || env.topic.empty()) {
        MqttLogAdapter::publish().warn(
            "mqtt.publish.rejected",
            "msg_id 或 topic 为空，拒绝发布",
            {{"msg_id_hash", tbox::fw::log::FieldValue::makeString(msgIdHash(env.msg_id))},
             {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(env.topic))},
             {"qos", tbox::fw::log::FieldValue::makeString(qosToString(env.qos))},
             {"payload_bytes", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(env.payload.size()))},
             {"validation_reason", tbox::fw::log::FieldValue::makeString("empty_msg_id_or_topic")},
             {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1004")}}
        );
        result.accepted = false;
        result.error = ErrorCode::MQTT_1004;
        return result;
    }

    if (state_machine_->getCurrentState() != ConnectionState::CONNECTED) {
        OutboundMessage msg;
        msg.msg_id = env.msg_id;
        msg.topic = env.topic;
        msg.qos = env.qos;
        msg.payload = env.payload;
        msg.priority = env.priority;
        msg.created_at = std::chrono::steady_clock::now();
        msg.state = MessageState::PENDING;
        msg.identity_generation = prov_mgr_.getGeneration();  // CR-006 §7

        if (cache_queue_->enqueue(msg)) {
            // accepted: 离线时成功写入离线队列 (CR-004 §5.2)
            result.accepted = true;
        } else {
            auto queue_config = config_manager_->getOfflineQueueConfig();
            MqttLogAdapter::offline_queue().warn(
                "mqtt.offline_queue.message_dropped",
                "离线缓存达到上限，消息被丢弃",
                {
                    {"msg_id_hash", tbox::fw::log::FieldValue::makeString(msgIdHash(env.msg_id))},
                    {"priority", tbox::fw::log::FieldValue::makeString(
                        [&]() -> std::string {
                            switch (env.priority) {
                                case Priority::LOW: return "LOW";
                                case Priority::NORMAL: return "NORMAL";
                                case Priority::HIGH: return "HIGH";
                                case Priority::CRITICAL: return "CRITICAL";
                                default: return "UNKNOWN";
                            }
                        }())},
                    {"queue_depth", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(cache_queue_->size()))},
                    {"queue_capacity", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(queue_config.max_size))},
                    {"drop_policy", tbox::fw::log::FieldValue::makeString(queue_config.drop_policy)},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1006")}
                }
            );
            result.accepted = false;
            result.error = ErrorCode::MQTT_1006;
        }
        return result;
    }

    // 在线：accepted 仅表示 daemon 已接管进入发送队列，不等于 PUBACK (CR-004 §5.2)
    auto client_result = client_manager_->publish(env.topic, env.payload, env.qos);
    result.accepted = client_result.accepted;
    result.error = client_result.error;
    return result;
}

PublishResult MqttService::publish(const std::string& topic,
                                   const std::vector<uint8_t>& payload,
                                   QoS qos,
                                   Priority priority) {
    PublishEnvelope env;
    env.msg_id = generateMsgId();
    env.topic = topic;
    env.qos = qos;
    env.priority = priority;
    env.payload = payload;
    return publishEnvelope(env);
}

SubscribeResult MqttService::subscribe(const std::string& topic,
                                       QoS qos,
                                       const std::string& target_service) {
    SubscribeResult result;

    RouteEntry entry;
    entry.internal_addr = target_service;
    entry.mqtt_topic = topic;
    entry.direction = RouteEntry::Direction::DOWN;
    entry.qos = qos;

    if (!route_bridge_->registerRoute(entry)) {
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.failed",
            "订阅路由注册失败",
            {
                {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
                {"qos", tbox::fw::log::FieldValue::makeString(qosToString(qos))},
                {"reason_code", tbox::fw::log::FieldValue::makeString("route_conflict")},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1008")}
            }
        );
        result.status = false;
        result.error = ErrorCode::MQTT_1008;
        return result;
    }

    if (state_machine_->getCurrentState() == ConnectionState::CONNECTED) {
        return client_manager_->subscribe(topic, qos);
    }

    result.status = true;
    return result;
}

bool MqttService::registerRoute(const std::string& internal_addr,
                                const std::string& mqtt_topic,
                                RouteEntry::Direction direction) {
    RouteEntry entry;
    entry.internal_addr = internal_addr;
    entry.mqtt_topic = mqtt_topic;
    entry.direction = direction;
    entry.qos = QoS::AT_LEAST_ONCE;

    return route_bridge_->registerRoute(entry);
}

ConnectionStateResult MqttService::getConnectionState() const {
    ConnectionStateResult result;
    auto state_info = state_machine_->getStateInfo();
    result.state = state_info.state;
    result.last_error = state_info.last_error;
    result.retry_count = state_info.retry_count;
    return result;
}

bool MqttService::applyConfig(const ConnectionConfig& config,
                              ConnectionConfig::Source source) {
    return config_manager_->applyConfig(config, source);
}

// ============================================================
// MqttIpcDispatcher::ServiceHandler 实现 (CR-004 §3)
// ============================================================

std::pair<int32_t, bool> MqttService::onPublish(const PublishEnvelope& env) {
    if (ipc_reject_new_) {
        return {static_cast<int32_t>(ErrorCode::MQTT_1002), false};
    }
    auto result = publishEnvelope(env);
    int32_t status = result.error.has_value()
        ? static_cast<int32_t>(result.error.value())
        : 0;
    return {status, result.accepted};
}

std::pair<int32_t, bool> MqttService::onSubscribe(const Route& route, int client_fd) {
    if (ipc_reject_new_) {
        return {static_cast<int32_t>(ErrorCode::MQTT_1002), false};
    }

    // 登记 route (DOWN 方向，CR-004 §6)
    RouteEntry entry;
    entry.internal_addr = route.topic;  // 订阅侧 route 的内部地址默认为 topic
    entry.mqtt_topic = route.topic;
    entry.direction = RouteEntry::Direction::DOWN;
    entry.qos = route.qos;
    route_bridge_->registerRoute(entry);

    // 连接在线时向 Broker 订阅
    if (state_machine_->getCurrentState() == ConnectionState::CONNECTED) {
        client_manager_->subscribe(route.topic, route.qos);
    }

    // 注：framework 事件订阅由 onSubscribeEventChannel 负责（独立连接）
    return {0, true};
}

std::pair<int32_t, bool> MqttService::onSubscribeEventChannel(int client_fd) {
    // 登记 framework 事件订阅：client_fd 接收 DOWNLINK_MESSAGE 事件 (CR-004 §6)
    bool ok = false;
    if (ipc_server_) {
        ok = ipc_server_->add_subscription(
            client_fd, static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE));
    }
    if (!ok) {
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.failed",
            "framework 事件订阅登记失败",
            {tbox::fw::log::Field("error_code", tbox::fw::log::FieldValue::makeString("MQTT-1008"))}
        );
        return {static_cast<int32_t>(ErrorCode::MQTT_1008), false};
    }
    return {0, true};
}

std::pair<int32_t, bool> MqttService::onUnsubscribe(int client_fd) {
    if (ipc_server_) {
        ipc_server_->remove_subscription(
            client_fd, static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE));
    }
    // per-fd 订阅清理由 dispatcher::onClientDisconnected 负责；不自动删除持久 RouteEntry (CR-004 §4)
    return {0, true};
}

std::pair<int32_t, bool> MqttService::onRegisterRoute(const RouteEntry& entry) {
    if (ipc_reject_new_) {
        return {static_cast<int32_t>(ErrorCode::MQTT_1002), false};
    }
    // 同 route key 覆盖 (CR-004 §7.1): 先移除再登记
    route_bridge_->removeRoute(entry.internal_addr);
    bool ok = route_bridge_->registerRoute(entry);
    if (!ok) {
        return {static_cast<int32_t>(ErrorCode::MQTT_1004), false};
    }

    // DOWN 路由：连接在线时向 Broker 订阅
    if (entry.direction == RouteEntry::Direction::DOWN &&
        state_machine_->getCurrentState() == ConnectionState::CONNECTED) {
        client_manager_->subscribe(entry.mqtt_topic, entry.qos);
    }
    return {0, true};
}

std::string MqttService::onGetConnectionState() {
    auto state_info = state_machine_->getStateInfo();
    nlohmann::json j;
    j["state"] = connectionStateToString(state_info.state);
    j["last_error"] = state_info.last_error;
    j["retry_count"] = state_info.retry_count;
    j["cloud_ready"] = cloud_ready_gate_ ? cloud_ready_gate_->isCloudReady() : false;
    return j.dump();
}

// ============================================================
// 下行分发 (CR-004 §6)
// ============================================================

std::string MqttService::buildDownlinkEventPayload(const std::string& topic,
                                                   const std::vector<uint8_t>& payload) const {
    nlohmann::json j;
    j["topic"] = topic;
    j["payload_base64"] = b64_encode(payload);
    return j.dump();
}

void MqttService::pushDownlinkEvent(const std::string& topic,
                                    const std::vector<uint8_t>& payload) {
    if (!ipc_server_) {
        return;
    }
    std::string payload_json = buildDownlinkEventPayload(topic, payload);
    // MQTT 网络 callback 不直接阻塞：由 dispatch queue worker 线程调用 push_event (CR-004 §4)
    ipc_server_->push_event(static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE),
                            payload_json);
}

void MqttService::onMessageReceived(const std::string& topic,
                                    const std::vector<uint8_t>& payload) {
    // 下行消息进入时生成 request_id (CR-003 §3)
    std::string request_id = generateRequestId();
    auto scope = makeContextScope("", request_id);

    auto route = route_bridge_->findRouteByMqttTopic(topic);
    if (!route.has_value()) {
        // mqtt.downlink.route_failed event (CR-003 §4, MQTT-1007)
        MqttLogAdapter::router().warn(
            "mqtt.downlink.route_failed",
            "下行消息无法路由或无订阅者",
            {
                {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
                {"payload_size", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(payload.size()))},
                {"reason", tbox::fw::log::FieldValue::makeString("no_route")},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1007")}
            }
        );
        return;
    }

    // 投递至有界 dispatch queue（非阻塞，不阻塞 Broker callback, CR-004 §4/§6）
    if (dispatch_queue_) {
        dispatch_queue_->enqueue(topic, payload);
    } else if (ipc_server_) {
        // 无 dispatch queue 时直接推送（兼容无 IPC 配置场景）
        pushDownlinkEvent(topic, payload);
    }
}

// ============================================================
// 连接状态变更 (CR-003 §4)
// ============================================================

void MqttService::onConnectionStateChanged(ConnectionState new_state) {
    // CR-006 §4: PROV_WAIT 状态读取 PROV 绑定
    if (new_state == ConnectionState::PROV_WAIT) {
        if (prov_mgr_.readBinding()) {
            // CR-007 §5: 身份变化后更新 ecu_uid 用于 Topic 展开
            effective_table_->setEcuUid(prov_mgr_.getEcuUid());
            state_machine_->onProvisionReady();
        } else {
            auto state_info = state_machine_->getStateInfo();
            MqttLogAdapter::provision().info(
                "mqtt.provision.waiting",
                "PROV 绑定未就绪，保持 PROV_WAIT 退避重试",
                {
                    {"retry_count", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(state_info.retry_count))},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}
                }
            );
        }
    } else if (new_state == ConnectionState::CERT_WAIT) {
        ActiveTlsCredential credential;
        if (cred_mgr_.getTlsCredential(credential) && credential.ready) {
            MqttLogAdapter::connection().info(
                "mqtt.certificate.ready",
                "SEC 凭据就绪，进入连接流程"
            );
            state_machine_->onCertificateReady();
        } else {
            auto state_info = state_machine_->getStateInfo();
            MqttLogAdapter::connection().info(
                "mqtt.certificate.waiting",
                "SEC 凭据尚未就绪，保持 CERT_WAIT 退避查询",
                {
                    {"retry_count", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(state_info.retry_count))},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
                }
            );
        }
    } else if (new_state == ConnectionState::SUBSCRIPTION_WAIT) {
        // CR-007 §6: 等待 required owner 完整快照（门闩）
        subscription_gate_->startWait();
        state_machine_->setSubscriptionGateDeadline(
            std::chrono::steady_clock::now() +
            subscription_config_.startup_wait_timeout_ms);
        checkStartupGate();
    } else if (new_state == ConnectionState::CONNECTING) {
        auto config = config_manager_->getConfig();
        // CR-006 §6: Populate identity-derived fields from PROV binding
        auto identity = prov_mgr_.getSnapshot();
        config.client_id = identity.client_id;
        config.username = identity.ecu_uid;
        // CR-006 §7: Capture identity generation for post-CONNACK verification
        connect_identity_generation_ = identity.generation;

        MqttLogAdapter::connection().info(
            "mqtt.connection.connecting",
            "发起 MQTT 连接",
            {
                {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                {"broker_port", tbox::fw::log::FieldValue::makeInt(config.broker_port)},
                {"keepalive_s", tbox::fw::log::FieldValue::makeInt(config.keepalive)},
                {"client_id_hash", tbox::fw::log::FieldValue::makeString(clientIdHash(config.client_id))},
                {"identity_generation", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(connect_identity_generation_))}
            }
        );
        if (!client_manager_->connect(config)) {
            auto state_info = state_machine_->getStateInfo();
            MqttLogAdapter::connection().error(
                "mqtt.connection.failed",
                "网络不可用或连接失败",
                {
                    {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                    {"reason_code", tbox::fw::log::FieldValue::makeString("connect_failed")},
                    {"retry_count", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(state_info.retry_count))},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1002")}
                }
            );
            state_machine_->onConnectionFailure("Failed to initiate MQTT connection");
        }
    } else if (new_state == ConnectionState::CONNECTED) {
        // CR-006 §7: Verify identity generation hasn't changed since CONNECT
        if (prov_mgr_.hasIdentityChanged(connect_identity_generation_)) {
            MqttLogAdapter::provision().warn(
                "mqtt.provision.identity_changed",
                "CONNACK 后身份 generation 已变化，断开重连",
                {
                    {"generation", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(prov_mgr_.getGeneration()))},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}
                }
            );
            client_manager_->disconnect();
            state_machine_->onProvisionChanged();
            return;
        }

        auto config = config_manager_->getConfig();
        MqttLogAdapter::connection().info(
            "mqtt.connection.established",
            "MQTT 连接建立成功",
            {
                {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                {"keepalive_s", tbox::fw::log::FieldValue::makeInt(config.keepalive)}
            }
        );

        int cached = cache_queue_->size();
        if (cached > 0) {
            MqttLogAdapter::offline_queue().info(
                "mqtt.offline_queue.flushing",
                "连接恢复，补发缓存消息",
                {{"queue_depth", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(cached))}}
            );
        }
        auto current_gen = prov_mgr_.getGeneration();
        while (!cache_queue_->empty()) {
            auto msg = cache_queue_->dequeue();
            if (msg.has_value()) {
                // CR-006 §7: Skip messages with stale identity generation
                if (msg->identity_generation != 0 &&
                    msg->identity_generation != current_gen) {
                    MqttLogAdapter::offline_queue().warn(
                        "mqtt.offline_queue.message_dropped",
                        "身份变化后丢弃旧 Topic 消息",
                        {
                            {"msg_id_hash", tbox::fw::log::FieldValue::makeString(msgIdHash(msg->msg_id))},
                            {"queue_depth", tbox::fw::log::FieldValue::makeInt(
                                static_cast<int64_t>(cache_queue_->size()))},
                            {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1006")}
                        }
                    );
                    continue;
                }
                client_manager_->publish(msg->topic, msg->payload, msg->qos);
            }
        }

        // CR-007 §8: CONNACK 后收敛订阅（替代旧的直接 subscribe downstream routes）
        bool session_present = client_manager_->getSessionPresent();
        // CR-007 §8.2: reconciler 内部根据 session_present + manifest identity 计算 diff
        state_machine_->onSubscribing();
        MqttLogAdapter::subscribe().info(
            "mqtt.subscription.reconcile.started",
            "CONNACK 后开始订阅收敛",
            {{"session_present", tbox::fw::log::FieldValue::makeBool(session_present)},
             {"identity_generation", tbox::fw::log::FieldValue::makeInt(
                 static_cast<int64_t>(connect_identity_generation_))}}
        );
        reconcileSubscriptions();
    } else if (new_state == ConnectionState::RECONNECTING) {
        auto state_info = state_machine_->getStateInfo();
        auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
            state_info.next_retry_at - std::chrono::steady_clock::now());
        MqttLogAdapter::connection().warn(
            "mqtt.connection.disconnected",
            "连接异常断开，触发重连",
            {
                {"reason_code", tbox::fw::log::FieldValue::makeString("unexpected_disconnect")},
                {"retry_count", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(state_info.retry_count))},
                {"next_retry_ms", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(std::max(0LL, delay.count())))},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1003")}
            }
        );
    } else if (new_state == ConnectionState::OFFLINE) {
        auto state_info = state_machine_->getStateInfo();
        MqttLogAdapter::connection().error(
            "mqtt.connection.failed",
            "重连次数耗尽，进入离线状态",
            {
                {"reason_code", tbox::fw::log::FieldValue::makeString("max_retries_exceeded")},
                {"retry_count", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(state_info.retry_count))},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1002")}
            }
        );
    }

    // 推送连接状态变更事件给订阅者 (CR-004 §6)
    if (ipc_server_ && dispatch_queue_) {
        nlohmann::json j;
        j["state"] = connectionStateToString(new_state);
        ipc_server_->push_event(
            static_cast<uint32_t>(ipc::EventType::CONNECTION_STATE_CHANGED), j.dump());
    }
}

// ============================================================
// CR-007: 订阅快照与 Cloud Readiness 实现
// ============================================================

ReplaceSnapshotResult MqttService::onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot& snapshot) {
    if (ipc_reject_new_) {
        ReplaceSnapshotResult r;
        r.status = SnapshotAcceptStatus::REJECTED;
        r.reason_code = "shutting_down";
        return r;
    }

    auto result = snapshot_registry_->replaceSnapshot(snapshot);

    if (result.status == SnapshotAcceptStatus::ACCEPTED &&
        result.reason_code != "idempotent" &&
        result.reason_code != "stale_generation") {
        // 重算有效订阅投影
        effective_table_->recompute(*snapshot_registry_);
        onSnapshotAccepted(snapshot.owner, snapshot.registration_complete);
        refreshUpstreamRoutes();

        // 运行中收到新 generation：退出 Cloud Ready 并收敛
        auto state = state_machine_->getCurrentState();
        if (state == ConnectionState::CLOUD_READY) {
            state_machine_->onCloudReadyLost();
        }
        if (state == ConnectionState::CLOUD_READY ||
            state == ConnectionState::SUBSCRIBING ||
            state == ConnectionState::CONNECTED_DEGRADED) {
            reconcileSubscriptions();
        }

        MqttLogAdapter::subscribe().info(
            "mqtt.subscription.snapshot.accepted",
            "订阅快照已接受",
            {{"owner", tbox::fw::log::FieldValue::makeString(snapshot.owner)},
             {"generation", tbox::fw::log::FieldValue::makeInt(
                 static_cast<int64_t>(snapshot.generation))},
             {"item_count", tbox::fw::log::FieldValue::makeInt(
                 static_cast<int64_t>(snapshot.items.size()))},
             {"mandatory_count", tbox::fw::log::FieldValue::makeInt(
                 static_cast<int64_t>(effective_table_->mandatoryCount()))}}
        );
    } else if (result.status == SnapshotAcceptStatus::REJECTED) {
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.snapshot.rejected",
            "订阅快照被拒绝",
            {{"owner", tbox::fw::log::FieldValue::makeString(snapshot.owner)},
             {"generation", tbox::fw::log::FieldValue::makeInt(
                 static_cast<int64_t>(snapshot.generation))},
             {"reason_code", tbox::fw::log::FieldValue::makeString(result.reason_code)},
             {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1010")}}
        );
    }

    return result;
}

SnapshotStatusResult MqttService::onGetSubscriptionSnapshotStatus(
        const std::string& owner, uint64_t generation) {
    return snapshot_registry_->getSnapshotStatus(owner, generation);
}

SubscriptionReadiness MqttService::onGetSubscriptionReadiness() {
    auto r = cloud_ready_gate_->getReadiness();
    auto all = snapshot_registry_->getAllLatestSnapshots();
    for (const auto& [owner, snap] : all) {
        r.owner_generations[owner] = snap.generation;
    }
    return r;
}

bool MqttService::brokerSubscribe(const std::string& topic, uint8_t qos) {
    auto r = client_manager_->subscribe(topic, static_cast<QoS>(qos));
    return r.status;
}

bool MqttService::brokerUnsubscribe(const std::string& topic) {
    return client_manager_->unsubscribe(topic);
}

void MqttService::onSnapshotAccepted(const std::string& owner, bool registration_complete) {
    subscription_gate_->onSnapshotAccepted(owner, registration_complete);
    if (state_machine_->getCurrentState() == ConnectionState::SUBSCRIPTION_WAIT) {
        checkStartupGate();
    }
}

void MqttService::checkStartupGate() {
    if (subscription_gate_->isSatisfied()) {
        subscription_gate_->stopWait();
        state_machine_->onGateSatisfied();
    } else if (subscription_gate_->isTimedOut()) {
        subscription_gate_->stopWait();
        auto missing = subscription_gate_->missingOwners();
        MqttLogAdapter::subscribe().warn(
            "mqtt.subscription.gate.timeout",
            "门闩等待超时，降级连接",
            {{"missing_owner_count", tbox::fw::log::FieldValue::makeInt(
                 static_cast<int64_t>(missing.size()))},
             {"timeout_ms", tbox::fw::log::FieldValue::makeInt(
                 static_cast<int64_t>(subscription_config_.startup_wait_timeout_ms.count()))},
             {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1011")}}
        );
        state_machine_->onGateTimeout();
    }
}

void MqttService::reconcileSubscriptions() {
    if (!reconciler_) return;
    auto desired = effective_table_->downstreamSnapshot();
    bool session_present = client_manager_->getSessionPresent();
    auto config = config_manager_->getConfig();
    std::string cid_hash = clientIdHash(config.client_id);
    auto result = reconciler_->reconcile(desired, session_present, cid_hash,
                                         connect_identity_generation_);
    MqttLogAdapter::subscribe().info(
        "mqtt.subscription.reconcile.completed",
        "订阅收敛完成",
        {{"session_present", tbox::fw::log::FieldValue::makeBool(session_present)},
         {"manifest_valid", tbox::fw::log::FieldValue::makeBool(result.manifest_valid)},
         {"subscribe_count", tbox::fw::log::FieldValue::makeInt(result.subscribe_count)},
         {"unsubscribe_count", tbox::fw::log::FieldValue::makeInt(result.unsubscribe_count)},
         {"active_count", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(reconciler_->activeTopics().size()))}}
    );
    evaluateCloudReadiness();
}

void MqttService::evaluateCloudReadiness() {
    if (!reconciler_ || !cloud_ready_gate_) return;
    bool ready = reconciler_->isConverged();
    uint32_t ma = reconciler_->mandatoryActive();
    uint32_t mt = reconciler_->mandatoryTotal();
    cloud_ready_gate_->onSubscriptionsReady(ready, ma, mt);
}

void MqttService::publishCloudReadyEvent(bool ready, uint32_t mandatory_active,
                                         uint32_t mandatory_total, const std::string& reason) {
    if (ipc_server_) {
        nlohmann::json j;
        j["ready"] = ready;
        j["mandatory_count"] = mandatory_total;
        j["mandatory_active"] = mandatory_active;
        j["reason_code"] = reason;
        ipc_server_->push_event(
            static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED), j.dump());
    }
    MqttLogAdapter::connection().info(
        "mqtt.cloud_ready.changed",
        ready ? "Cloud Ready 达成" : "Cloud Ready 丢失",
        {{"ready", tbox::fw::log::FieldValue::makeBool(ready)},
         {"mandatory_count", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(mandatory_total))},
         {"reason_code", tbox::fw::log::FieldValue::makeString(reason)}}
    );
}

void MqttService::refreshUpstreamRoutes() {
    // CR-007 §5: 上行路由（UP/BIDIRECTIONAL）注册到 RouteBridge（发布用）
    auto routes = effective_table_->upstreamRoutes();
    route_bridge_->clear();
    for (const auto& r : routes) {
        RouteEntry entry;
        entry.internal_addr = r.internal_addr;
        entry.mqtt_topic = r.mqtt_topic;
        entry.direction = RouteEntry::Direction::UP;
        entry.qos = static_cast<QoS>(r.qos);
        route_bridge_->registerRoute(entry);
    }
}

} // namespace mqtt
} // namespace tbox
