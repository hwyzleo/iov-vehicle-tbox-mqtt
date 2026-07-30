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

MqttService::MqttService()
    : state_machine_(std::make_unique<ConnectionStateMachine>()),
      client_manager_(std::make_unique<MqttClientManager>()),
      cache_queue_(std::make_unique<OfflineCacheQueue>(1000)),
      route_bridge_(std::make_unique<RouteBridge>()),
      config_manager_(std::make_unique<ConfigManager>()),
      tls_adapter_(std::make_unique<SecTlsKeyAdapter>()) {
}

MqttService::~MqttService() {
    stop();
}

bool MqttService::initialize(std::shared_ptr<ProvisionIdentityManager> prov_mgr,
                             std::shared_ptr<CredentialManager> cred_mgr,
                             const std::string& config_root,
                             const std::string& store_root) {
    if (!cred_mgr || !prov_mgr) {
        return false;
    }

    cred_mgr_ = cred_mgr;
    prov_mgr_ = prov_mgr;

    // Open framework-store for runtime state and OTA-updatable params (Design §5)
    store_ = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_root));
    if (!store_->isReady()) {
        MqttLogAdapter::ipc().error(
            "mqtt.store.open_failed",
            "framework-store 打开失败",
            {{"store_root", tbox::fw::log::FieldValue::makeString(store_root)}}
        );
        return false;
    }

    // Load static config from framework-config (Design §5)
    if (!config_manager_->loadFromFramework(config_root)) {
        return false;
    }

    // Set store and load OTA-updatable params from framework-store
    config_manager_->setStore(store_);
    config_manager_->loadFromStore(*store_);

    // Set store for state machine persistence
    state_machine_->setStore(store_);

    // CR-005 §4.2: 初始化离线队列
    auto queue_config = config_manager_->getOfflineQueueConfig();
    cache_queue_ = std::make_unique<OfflineCacheQueue>(queue_config.max_size);

    // CR-006 §4: 初始化 tbox::prov_client (before SEC, per CR-006 §4 init order)
    auto identity_snapshot = config_manager_->getSnapshot();
    if (identity_snapshot) {
        auto identity_section = identity_snapshot->getSection("mqtt.identity");
        if (identity_section) {
            prov_mgr_->setClientIdFormat(
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
    prov_mgr_->initialize();

    // CR-005 §4.2: 初始化 tbox::sec_client
    cred_mgr_->initialize();

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
    cred_mgr_->subscribeTlsCredentialChanged([this]() {
        // CR-005 §7 cleanup 顺序: 标记 generation 失效 -> 清理旧 TLS context -> 状态机转 CERT_WAIT
        cred_mgr_->invalidateCredential();
        tls_adapter_->destroyContext();
        client_manager_->disconnect();
        state_machine_->onCredentialChanged();
    });

    // CR-005 §4.2: 获取初始凭据 (getTlsCredential)
    // SEC 不可用或 NOT_READY 时保持 CERT_WAIT (CR-005 §4.2)
    ActiveTlsCredential credential;
    cred_mgr_->getTlsCredential(credential);

    // CR-005 §5: 初始化 MqttClientManager (注入 CredentialManager + SecTlsKeyAdapter)
    client_manager_->initialize(*cred_mgr_, *tls_adapter_);

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
            auto identity = prov_mgr_->getSnapshot();
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

    return true;
}

bool MqttService::start() {
    if (!cred_mgr_ || !prov_mgr_) {
        return false;
    }

    // CR-006 §4: State machine starts in PROV_WAIT; onConnectionStateChanged
    // callback reads PROV binding and transitions to CERT_WAIT if valid.
    state_machine_->start();

    return true;
}

void MqttService::stop() {
    // CR-004 §4: 先拒绝新 publish/route 请求
    ipc_reject_new_ = true;

    stopIpcServer();
    state_machine_->stop();
    client_manager_->disconnect();
    // CR-005 §7: 清理 TLS context 和凭据缓冲
    tls_adapter_->destroyContext();
    cred_mgr_->cleanupCredential();
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
        msg.identity_generation = prov_mgr_->getGeneration();  // CR-006 §7

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
        if (prov_mgr_->readBinding()) {
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
        if (cred_mgr_->getTlsCredential(credential) && credential.ready) {
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
    } else if (new_state == ConnectionState::CONNECTING) {
        auto config = config_manager_->getConfig();
        // CR-006 §6: Populate identity-derived fields from PROV binding
        auto identity = prov_mgr_->getSnapshot();
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
        if (prov_mgr_->hasIdentityChanged(connect_identity_generation_)) {
            MqttLogAdapter::provision().warn(
                "mqtt.provision.identity_changed",
                "CONNACK 后身份 generation 已变化，断开重连",
                {
                    {"generation", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(prov_mgr_->getGeneration()))},
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
        auto current_gen = prov_mgr_->getGeneration();
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

        auto downstream_routes = route_bridge_->getDownstreamRoutes();
        if (!downstream_routes.empty()) {
            MqttLogAdapter::subscribe().info(
                "mqtt.subscribe.batch_start",
                "连接恢复，订阅下行路由",
                {{"route_count", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(downstream_routes.size()))}}
            );
        }
        for (const auto& route : downstream_routes) {
            client_manager_->subscribe(route.mqtt_topic, route.qos);
        }
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

} // namespace mqtt
} // namespace tbox
