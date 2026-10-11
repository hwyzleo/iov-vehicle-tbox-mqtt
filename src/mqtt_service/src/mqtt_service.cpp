#include "mqtt_service.h"
#include "connection_state_machine.h"
#include "mqtt_client_manager.h"
#include "offline_cache_queue.h"
#include "route_bridge.h"
#include "config_manager.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "sec_tls_key_adapter.h"
#include "subscription_catalog.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "utils.h"
#include <nlohmann/json.hpp>
#include <chrono>
#include <set>

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

// CR-014 §8: 指纹仅截断输出，禁止完整指纹/证书原文进入日志
std::string fingerprintPrefix(const std::string& ref) {
    constexpr size_t kKeep = 7 + 16;  // "sha256:" + 16 hex
    if (ref.size() <= kKeep) return ref;
    return ref.substr(0, kKeep) + "...";
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
      cloud_ready_gate_(std::make_unique<CloudReadinessGate>()),
      template_resolver_(std::make_unique<TopicTemplateResolver>()),
      projection_builder_(std::make_unique<TopicProjectionBuilder>()),
      route_table_(std::make_shared<const EffectiveRouteTable>()),
      route_publisher_(std::make_unique<RoutePublisher>()),
      downlink_dispatcher_(std::make_unique<DownlinkRouteDispatcher>()),
      legacy_guard_(std::make_unique<LegacyTopicGuard>()) {
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

    // CR-009 §5: 初始化模板解析器身份
    template_resolver_->setIdentity(prov_mgr_.getEcuUid(), prov_mgr_.getGeneration());

    // CR-009 §6: RoutePublisher 回调装配
    route_publisher_->setBrokerPublishFn(
        [this](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos) -> bool {
            auto r = client_manager_->publish(topic, payload, qos);
            return r.accepted;
        });
    route_publisher_->setQueueEnqueueFn(
        [this](const OutboundMessage& msg) -> bool {
            return cache_queue_->enqueue(msg);
        });
    route_publisher_->setIsConnectedFn(
        [this]() -> bool {
            auto s = state_machine_->getCurrentState();
            return s == ConnectionState::CONNECTED || s == ConnectionState::CLOUD_READY ||
                   s == ConnectionState::SUBSCRIBING || s == ConnectionState::CONNECTED_DEGRADED;
        });

    // CR-009 §7: DownlinkRouteDispatcher 回调装配
    downlink_dispatcher_->setDispatchFn(
        [this](const RoutedDownlinkEnvelope& event) {
            if (dispatch_queue_) {
                dispatch_queue_->enqueueRouted(event);
            }
        });

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

    // CR-007 §10 / CR-014 §4.2: CloudReadinessGate 回调 -> 发布完整快照事件 + 状态机
    cloud_ready_gate_->setCallback(
        [this](const CloudReadinessSnapshot& snapshot) {
            publishCloudReadyEvent(snapshot);
            if (snapshot.cloud_ready &&
                state_machine_->getCurrentState() == ConnectionState::SUBSCRIBING) {
                state_machine_->onSubscriptionsReady();
            }
        });

    // CR-015 §18.3: capability readiness 变化 -> 按 capability 定向发布 typed 事件
    cloud_ready_gate_->setCapabilityCallback(
        [this](const CapabilityReadiness& readiness) {
            publishCapabilityEvent(readiness);
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
                prov_mgr_.setProvisionRetryConfig(prov_config);
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
    cred_mgr_.setCredentialPollConfig(poll_config);

    // CR-005 §4.2: 建立 credential change subscription (在首次获取前)
    // CR-005 §7: payload 仅用于触发，不直接替换凭据；MQTT 必须重新调用 getTlsCredential()
    // CR-014 §4.2: 先提交并发布旧 generation 的 cloud_ready=false（带旧身份），再销毁旧 context
    cred_mgr_.subscribeTlsCredentialChanged([this]() {
        // 先撤销旧会话 Cloud Ready，保证调用方精确失效旧 generation
        if (cloud_ready_gate_) {
            cloud_ready_gate_->revokeSession("credential_changed");
        }
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
        [server_ptr_placeholder](const std::string& topic, const std::vector<uint8_t>& payload,
                                 const std::optional<RoutedDownlinkEnvelope>& route_event) {
            if (route_event.has_value()) {
                server_ptr_placeholder->pushRoutedDownlinkEvent(route_event.value());
            } else {
                server_ptr_placeholder->pushDownlinkEvent(topic, payload);
            }
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

    // CR-009 §10.2: Legacy 完整 Topic 必须经过 LegacyTopicGuard 校验
    // 仅当身份有效且有 owner 启用 route API 时校验（混合模式安全守卫）
    if (template_resolver_->isIdentityValid() && snapshot_registry_->ownerCount() > 0) {
        if (!legacy_guard_->validate(env.topic, *template_resolver_)) {
            MqttLogAdapter::publish().warn(
                "mqtt.route.publish.rejected",
                "Legacy Topic 未通过守卫校验（设备层不匹配或身份无效）",
                {{"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(env.topic))},
                 {"qos", tbox::fw::log::FieldValue::makeString(qosToString(env.qos))},
                 {"validation_reason", tbox::fw::log::FieldValue::makeString("legacy_guard_rejected")},
                 {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1004")}}
            );
            result.accepted = false;
            result.error = ErrorCode::MQTT_1004;
            return result;
        }
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

// CR-010 §3.1: routed downlink 事件订阅 -- 登记 fd 到 ROUTED_DOWNLINK_MESSAGE
std::pair<int32_t, bool> MqttService::onSubscribeRoutedDownlinkEventChannel(
        const std::string& owner, int client_fd) {
    bool ok = false;
    if (ipc_server_) {
        ok = ipc_server_->add_subscription(
            client_fd, static_cast<uint32_t>(ipc::EventType::ROUTED_DOWNLINK_MESSAGE));
    }
    if (!ok) {
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.routed_failed",
            "routed downlink 事件订阅登记失败",
            {tbox::fw::log::Field("owner", tbox::fw::log::FieldValue::makeString(owner)),
             tbox::fw::log::Field("error_code", tbox::fw::log::FieldValue::makeString("MQTT-1008"))}
        );
        return {static_cast<int32_t>(ErrorCode::MQTT_1008), false};
    }
    return {0, true};
}

// CR-014 §5: Cloud Ready 事件通道（仅登记 fd 到 CLOUD_READY_CHANGED，不触发 Broker 操作）
std::pair<int32_t, bool> MqttService::onSubscribeCloudReadinessEventChannel(int client_fd) {
    bool ok = false;
    if (ipc_server_) {
        ok = ipc_server_->add_subscription(
            client_fd, static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED));
    }
    if (!ok) {
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.cloud_readiness_failed",
            "Cloud Ready 事件订阅登记失败",
            {tbox::fw::log::Field("error_code", tbox::fw::log::FieldValue::makeString("MQTT-1008"))}
        );
        return {static_cast<int32_t>(ErrorCode::MQTT_1008), false};
    }
    return {0, true};
}

// CR-015 §18.3: capability readiness 事件通道（仅登记 fd，不触发 Broker 操作）
std::pair<int32_t, bool> MqttService::onSubscribeCapabilityReadinessEventChannel(
        const std::string& capability, int client_fd) {
    bool ok = false;
    if (ipc_server_) {
        ok = ipc_server_->add_subscription(
            client_fd, static_cast<uint32_t>(ipc::EventType::CAPABILITY_READINESS_CHANGED));
    }
    if (!ok) {
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.capability_readiness_failed",
            "capability readiness 事件订阅登记失败",
            {tbox::fw::log::Field("capability",
                tbox::fw::log::FieldValue::makeString(capability)),
             tbox::fw::log::Field("error_code",
                tbox::fw::log::FieldValue::makeString("MQTT-1008"))}
        );
        return {static_cast<int32_t>(ErrorCode::MQTT_1008), false};
    }
    // 订阅建立后刷新一次，保证客户端能读到当前 capability 快照
    refreshCapabilityReadiness();
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

// CR-009 §7: route-based 下行事件 payload（不含完整 Topic）
std::string MqttService::buildRoutedDownlinkPayload(const RoutedDownlinkEnvelope& event) const {
    nlohmann::json j;
    j["owner"] = event.key.owner;
    j["route_id"] = event.key.route_id;
    j["target"] = event.target;
    j["qos"] = static_cast<int>(event.qos);
    j["payload_base64"] = b64_encode(event.payload);
    j["request_id"] = event.request_id;
    j["trace_id"] = event.trace_id;
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

// CR-009 §7: 推送 route-based 下行事件
// CR-010 §3.1: 按 owner 定向推送（push_event_to），不广播给 legacy 订阅者
void MqttService::pushRoutedDownlinkEvent(const RoutedDownlinkEnvelope& event) {
    if (!ipc_server_) {
        return;
    }
    auto fds = ipc_dispatcher_->getRoutedFdsForOwner(event.key.owner);
    if (fds.empty()) {
        return;  // 无该 owner 的活跃 routed 订阅者
    }
    std::string payload_json = buildRoutedDownlinkPayload(event);
    for (int fd : fds) {
        ipc_server_->push_event_to(fd,
            static_cast<uint32_t>(ipc::EventType::ROUTED_DOWNLINK_MESSAGE),
            payload_json);
    }
}

void MqttService::onMessageReceived(const std::string& topic,
                                    const std::vector<uint8_t>& payload) {
    // 下行消息进入时生成 request_id (CR-003 §3)
    std::string request_id = generateRequestId();
    auto scope = makeContextScope("", request_id);

    // CR-009 §7: 优先使用 route-based 反向索引分发
    if (downlink_dispatcher_ && route_table_) {
        auto current_gen = prov_mgr_.getGeneration();
        size_t matched = downlink_dispatcher_->dispatch(
            topic, payload, route_table_, current_gen);
        if (matched > 0) {
            return;  // 已通过 route-based 分发
        }
    }

    // Legacy: route_bridge 查找 (CR-004 §6)
    auto route = route_bridge_->findRouteByMqttTopic(topic);
    if (!route.has_value()) {
        // mqtt.route.downlink.unmatched event (CR-009 §11)
        MqttLogAdapter::router().warn(
            "mqtt.route.downlink.unmatched",
            "下行消息无法匹配路由或无订阅者",
            {
                {"topic_hash", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
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
    // CR-014 §3/§4.2: 快照权威对象跟踪连接状态与 connection_generation
    if (cloud_ready_gate_) {
        cloud_ready_gate_->onConnectionState(
            new_state, state_machine_->getConnectionGeneration());
    }

    // CR-006 §4: PROV_WAIT 状态读取 PROV 绑定
    if (new_state == ConnectionState::PROV_WAIT) {
        // CR-016 §5: 先恢复 IPC transport，再查询绑定；
        // 首次连接失败不再永久自锁，无需重启 daemon。
        prov_mgr_.ensureConnected();
        if (prov_mgr_.readBinding()) {
            prov_mgr_.markDependencyReady();
            // CR-009 §5: 身份变化后更新 ecu_uid 用于 Topic 展开
            uint64_t prev_gen = template_resolver_->identityGeneration();
            effective_table_->setEcuUid(prov_mgr_.getEcuUid());
            // CR-009 §5: 更新模板解析器身份并重建路由投影
            template_resolver_->setIdentity(prov_mgr_.getEcuUid(), prov_mgr_.getGeneration());
            // CR-009 §8: 身份变化时退出 Cloud Ready 并失效旧投影
            if (prev_gen != 0 && prev_gen != prov_mgr_.getGeneration()) {
                auto state = state_machine_->getCurrentState();
                if (state == ConnectionState::CLOUD_READY) {
                    state_machine_->onCloudReadyLost();
                }
                MqttLogAdapter::subscribe().warn(
                    "mqtt.topic_projection.invalidated",
                    "身份变化，旧路由投影已失效",
                    {{"old_generation", tbox::fw::log::FieldValue::makeInt(
                         static_cast<int64_t>(prev_gen))},
                     {"new_generation", tbox::fw::log::FieldValue::makeInt(
                         static_cast<int64_t>(prov_mgr_.getGeneration()))},
                     {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}}
                );
            }
            rebuildRouteProjection();
            state_machine_->onProvisionReady();
        } else {
            auto retry = prov_mgr_.getRetryState();
            std::string reason = prov_mgr_.getLastError();
            if (reason.empty()) {
                reason = "prov_binding_not_ready";
            }
            MqttLogAdapter::provision().info(
                "mqtt.provision.waiting",
                "PROV 绑定未就绪，保持 PROV_WAIT 退避重试",
                {
                    {"reason_code", tbox::fw::log::FieldValue::makeString(reason)},
                    {"retry_count", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.retry_count))},
                    {"total_attempts", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.total_attempts))},
                    {"backoff_ms", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.next_backoff.count()))},
                    {"client_generation", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.client_generation))},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}
                }
            );
        }
    } else if (new_state == ConnectionState::CERT_WAIT) {
        // CR-016 §4: 先恢复 SEC IPC transport，再查询凭据
        cred_mgr_.ensureConnected();
        ActiveTlsCredential credential;
        if (cred_mgr_.getTlsCredential(credential) && credential.ready) {
            cred_mgr_.markDependencyReady();
            MqttLogAdapter::connection().info(
                "mqtt.certificate.ready",
                "SEC 凭据就绪，进入连接流程"
            );
            state_machine_->onCertificateReady();
        } else {
            auto retry = cred_mgr_.getRetryState();
            std::string reason = cred_mgr_.getLastError();
            if (reason.empty()) {
                reason = "sec_credential_not_ready";
            }
            MqttLogAdapter::connection().info(
                "mqtt.certificate.waiting",
                "SEC 凭据尚未就绪，保持 CERT_WAIT 退避查询",
                {
                    {"reason_code", tbox::fw::log::FieldValue::makeString(reason)},
                    {"retry_count", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.retry_count))},
                    {"total_attempts", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.total_attempts))},
                    {"backoff_ms", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.next_backoff.count()))},
                    {"client_generation", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(retry.client_generation))},
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
            // CR-016 §10: 连接失败关键路径集中落盘
            MqttLogAdapter::flushCritical();
            state_machine_->onConnectionFailure("Failed to initiate MQTT connection");
        } else {
            // CR-014 §4.1: TLS context 已建立，固化本次会话证书身份到快照权威对象。
            // 仅当后续握手/CONNACK/Mandatory SUBACK 全部成功且 generation 未变时才 cloud_ready=true。
            if (cloud_ready_gate_) {
                auto identity = client_manager_->getTlsSessionIdentity();
                if (identity.valid) {
                    identity.connection_generation = state_machine_->getConnectionGeneration();
                    cloud_ready_gate_->setTlsSessionIdentity(identity);
                }
            }
        }
    } else if (new_state == ConnectionState::CONNECTED) {
        // CR-014 §4.1: 兑底确保快照权威对象持有本次 TLS 会话身份（与 CONNECTING 分支竞态）。
        // 同一 identity 重复设置幂等；新会话才重置 subscriptions_ready。
        if (cloud_ready_gate_) {
            auto identity = client_manager_->getTlsSessionIdentity();
            if (identity.valid) {
                identity.connection_generation = state_machine_->getConnectionGeneration();
                cloud_ready_gate_->setTlsSessionIdentity(identity);
            }
        }
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

                // CR-009 §9: route-based 消息从当前投影重新解析 Topic
                if (msg->isRouteBased()) {
                    auto route = route_table_->lookupRoute(msg->route);
                    if (!route.has_value() || route->state != RouteState::ACTIVE ||
                        route->expanded_topic.empty()) {
                        // 路由不存在或未展开 -> 丢弃 (CR-009 §9)
                        MqttLogAdapter::offline_queue().warn(
                            "mqtt.offline_queue.message_dropped",
                            "路由不可用或未展开，丢弃 route-based 消息",
                            {
                                {"msg_id_hash", tbox::fw::log::FieldValue::makeString(msgIdHash(msg->msg_id))},
                                {"route_id_hash", tbox::fw::log::FieldValue::makeString(
                                    topicTemplate(msg->route.route_id))},
                                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1012")}
                            }
                        );
                        continue;
                    }
                    // identity generation 变化检查 (CR-009 §9)
                    if (msg->accepted_identity_generation != route->identity_generation) {
                        // 默认策略：IDENTITY_CHANGED，不静默改写 Topic (CR-009 §9)
                        MqttLogAdapter::offline_queue().warn(
                            "mqtt.offline_queue.message_dropped",
                            "身份变化后不补发旧 UID Topic 的 route-based 消息",
                            {
                                {"msg_id_hash", tbox::fw::log::FieldValue::makeString(msgIdHash(msg->msg_id))},
                                {"route_id_hash", tbox::fw::log::FieldValue::makeString(
                                    topicTemplate(msg->route.route_id))},
                                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1006")}
                            }
                        );
                        continue;
                    }
                    // 重新解析成功，用当前投影的 expanded_topic 发布
                    client_manager_->publish(route->expanded_topic, msg->payload, msg->qos);
                } else {
                    // legacy 路径：使用存储的完整 Topic
                    client_manager_->publish(msg->topic, msg->payload, msg->qos);
                }
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
                    static_cast<int64_t>(std::max<int64_t>(0, delay.count())))},
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
        // CR-009 §5: 重建 route-based 路由投影
        rebuildRouteProjection();
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
        // CR-015: 刷新 capability readiness（enabled/contract/snapshot/uplink/downlink）
        refreshCapabilityReadiness();

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

// CR-015 §18.4: capability readiness 查询（与事件读取同一原子状态）
CapabilityReadiness MqttService::onGetCapabilityReadiness(const std::string& capability) {
    if (!cloud_ready_gate_) return CapabilityReadiness{};
    return cloud_ready_gate_->getCapabilityReadiness(capability);
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
    // CR-015: mandatory 收敛后同步 capability 下行就绪状态
    refreshCapabilityReadiness();
}

void MqttService::publishCloudReadyEvent(const CloudReadinessSnapshot& snapshot) {
    if (ipc_server_) {
        ipc_server_->push_event(
            static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED),
            cloudReadinessToJson(snapshot));
    }
    MqttLogAdapter::connection().info(
        "mqtt.cloud_ready.changed",
        snapshot.cloud_ready ? "Cloud Ready 达成" : "Cloud Ready 丢失",
        {{"ready", tbox::fw::log::FieldValue::makeBool(snapshot.cloud_ready)},
         {"connection_generation", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(snapshot.connection_generation))},
         {"cert_session_generation", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(snapshot.cert_session_generation))},
         {"credential_version", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(snapshot.credential_version))},
         {"certificate_ref_prefix", tbox::fw::log::FieldValue::makeString(
             fingerprintPrefix(snapshot.certificate_ref))},
         {"event_sequence", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(snapshot.event_sequence))},
         {"reason_code", tbox::fw::log::FieldValue::makeString(snapshot.reason_code)}}
    );
}

// CR-015 §18.3: 根据 owner 快照 + catalog + 当前投影/收敛状态刷新 capability readiness
void MqttService::refreshCapabilityReadiness() {
    if (!cloud_ready_gate_) return;
    const auto& catalog = SubscriptionCatalog::instance();

    std::set<std::string> scopes;
    for (const auto& [key, entry] : catalog.entries()) {
        if (!entry.readiness_scope.empty()) scopes.insert(entry.readiness_scope);
    }

    for (const auto& scope : scopes) {
        bool enabled = false;
        bool present = false;
        bool contract_supported = false;
        uint64_t owner_generation = 0;
        bool uplink_ready = false;

        for (const auto& [key, entry] : catalog.entries()) {
            if (entry.readiness_scope != scope) continue;

            auto snap = snapshot_registry_->getLatestSnapshot(entry.owner);
            if (snap) {
                for (const auto& item : snap->items) {
                    if (item.route_id != entry.route_id) continue;
                    present = true;
                    if (item.enabled.value_or(false)) enabled = true;
                    contract_supported =
                        (snap->contract_version == kSubscriptionCatalogContractVersion);
                    owner_generation = snap->generation;
                    break;
                }
            }

            if (entry.direction == SnapshotDirection::UP ||
                entry.direction == SnapshotDirection::BIDIRECTIONAL) {
                if (route_table_) {
                    auto r = route_table_->lookupRoute({entry.owner, entry.route_id});
                    if (r.has_value() && r->state == RouteState::ACTIVE) {
                        uplink_ready = true;
                    }
                }
            }
        }

        cloud_ready_gate_->setCapabilityState(
            scope, enabled, contract_supported, present, uplink_ready, owner_generation);
        bool downlink_ready = reconciler_ && reconciler_->capabilityConverged(scope);
        bool downlink_rejected = reconciler_ && reconciler_->capabilityRejected(scope);
        cloud_ready_gate_->setCapabilityDownlinkReady(scope, downlink_ready, downlink_rejected);
    }
}

// CR-015 §18.3: 向已订阅的 fd 定向推送 capability readiness 事件（脱敏）
void MqttService::publishCapabilityEvent(const CapabilityReadiness& readiness) {
    if (ipc_server_ && ipc_dispatcher_) {
        auto fds = ipc_dispatcher_->getCapabilityFds(readiness.capability);
        if (!fds.empty()) {
            std::string json = capabilityReadinessToJson(readiness);
            for (int fd : fds) {
                ipc_server_->push_event_to(
                    fd, static_cast<uint32_t>(ipc::EventType::CAPABILITY_READINESS_CHANGED),
                    json);
            }
        }
    }
    MqttLogAdapter::connection().info(
        "mqtt.capability.readiness.changed",
        readiness.ready ? "capability 就绪" : "capability 未就绪",
        {{"capability", tbox::fw::log::FieldValue::makeString(readiness.capability)},
         {"ready", tbox::fw::log::FieldValue::makeBool(readiness.ready)},
         {"enabled", tbox::fw::log::FieldValue::makeBool(readiness.enabled)},
         {"contract_supported", tbox::fw::log::FieldValue::makeBool(readiness.contract_supported)},
         {"snapshot_accepted", tbox::fw::log::FieldValue::makeBool(readiness.snapshot_accepted)},
         {"uplink_ready", tbox::fw::log::FieldValue::makeBool(readiness.uplink_ready)},
         {"downlink_suback_ready", tbox::fw::log::FieldValue::makeBool(readiness.downlink_suback_ready)},
         {"owner_generation", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(readiness.owner_generation))},
         {"event_sequence", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(readiness.event_sequence))},
         {"reason_code", tbox::fw::log::FieldValue::makeString(readiness.reason_code)}}
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

// ============================================================
// CR-009: 路由投影实现
// ============================================================

void MqttService::rebuildRouteProjection() {
    // CR-009 §5: 从 owner 快照 + 当前身份构建不可变路由投影
    // 先同步当前身份到模板解析器
    template_resolver_->setIdentity(prov_mgr_.getEcuUid(), prov_mgr_.getGeneration());
    auto new_table = projection_builder_->build(*snapshot_registry_, *template_resolver_);
    route_table_ = new_table;  // 原子替换（shared_ptr 赋值）

    // 日志: 投影构建事件 (CR-009 §11)
    MqttLogAdapter::subscribe().info(
        "mqtt.topic_projection.built",
        "路由投影已构建",
        {{"identity_generation", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(new_table->identityGeneration()))},
         {"route_count", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(new_table->totalRoutes()))},
         {"active_count", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(new_table->activeRoutes()))},
         {"unresolved_count", tbox::fw::log::FieldValue::makeInt(
             static_cast<int64_t>(new_table->unresolvedRoutes()))}}
    );
    // CR-015: 身份/投影变化后刷新 capability 上行就绪
    refreshCapabilityReadiness();
}

std::shared_ptr<const EffectiveRouteTable> MqttService::getRouteTable() const {
    return route_table_;
}

// ============================================================
// CR-009 §6: Route-based 发布
// ============================================================

PublishResult MqttService::publishRoute(const RoutePublishRequest& req) {
    return route_publisher_->publishRoute(req, route_table_, *template_resolver_);
}

PublishResult MqttService::onPublishRoute(const RoutePublishRequest& req) {
    if (ipc_reject_new_) {
        PublishResult r;
        r.accepted = false;
        r.error = ErrorCode::MQTT_1002;
        return r;
    }
    return publishRoute(req);
}

} // namespace mqtt
} // namespace tbox
