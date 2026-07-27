#include "mqtt_service.h"
#include "connection_state_machine.h"
#include "mqtt_client_manager.h"
#include "offline_cache_queue.h"
#include "route_bridge.h"
#include "config_manager.h"
#include "sec_cert_interface.h"
#include "internal_bus_interface.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"

namespace tbox {
namespace mqtt {

MqttService::MqttService()
    : state_machine_(std::make_unique<ConnectionStateMachine>()),
      client_manager_(std::make_unique<MqttClientManager>()),
      cache_queue_(std::make_unique<OfflineCacheQueue>(1000)),
      route_bridge_(std::make_unique<RouteBridge>()),
      config_manager_(std::make_unique<ConfigManager>()) {
}

MqttService::~MqttService() {
    stop();
}

bool MqttService::initialize(std::shared_ptr<SecCertInterface> sec_interface,
                             std::shared_ptr<InternalBusInterface> bus_interface,
                             const std::string& config_root,
                             const std::string& store_root) {
    if (!sec_interface || !bus_interface) {
        return false;
    }

    sec_interface_ = sec_interface;
    bus_interface_ = bus_interface;

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

    // Load certificates from configured paths (unless already ready, e.g. via simulateCertificateReady in tests)
    if (!sec_interface_->isCertificateReady()) {
        auto config = config_manager_->getConfig();
        if (!sec_interface_->loadCertificates(config.root_ca_ref,
                                               config.tls_cert_ref,
                                               config.tls_key_ref,
                                               config.key_source)) {
            MqttLogAdapter::tls().error(
                "mqtt.tls.handshake_failed",
                "证书加载失败",
                {
                    {"reason_code", tbox::fw::log::FieldValue::makeString("cert_load_failed")},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
                }
            );
        }
    }

    // Configure offline cache queue from framework-config snapshot
    auto queue_config = config_manager_->getOfflineQueueConfig();
    cache_queue_ = std::make_unique<OfflineCacheQueue>(queue_config.max_size);

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
    if (!sec_interface_ || !bus_interface_) {
        return false;
    }

    state_machine_->start();

    if (sec_interface_->isCertificateReady()) {
        MqttLogAdapter::connection().info(
            "mqtt.certificate.ready",
            "证书就绪，进入连接流程"
        );
        state_machine_->onCertificateReady();
    } else {
        // mqtt.certificate.waiting event (CR-003 §4)
        auto state_info = state_machine_->getStateInfo();
        MqttLogAdapter::connection().info(
            "mqtt.certificate.waiting",
            "SEC 证书尚未就绪，连接保持等待",
            {
                {"state", tbox::fw::log::FieldValue::makeString(
                    connectionStateToString(state_info.state))},
                {"retry_count", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(state_info.retry_count))},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
    }

    return true;
}

void MqttService::stop() {
    state_machine_->stop();
    client_manager_->disconnect();
}

PublishResult MqttService::publish(const std::string& topic,
                                   const std::vector<uint8_t>& payload,
                                   QoS qos,
                                   Priority priority) {
    PublishResult result;

    // Topic validation (CR-003 §4: mqtt.publish.rejected MQTT-1004)
    if (topic.empty()) {
        MqttLogAdapter::publish().warn(
            "mqtt.publish.rejected",
            "topic 为空，拒绝发布",
            {
                {"topic_template", tbox::fw::log::FieldValue::makeString("empty")},
                {"qos", tbox::fw::log::FieldValue::makeString(qosToString(qos))},
                {"payload_size", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(payload.size()))},
                {"validation_reason", tbox::fw::log::FieldValue::makeString("empty_topic")},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1004")}
            }
        );
        result.accepted = false;
        result.error = ErrorCode::MQTT_1004;
        return result;
    }

    if (state_machine_->getCurrentState() != ConnectionState::CONNECTED) {
        OutboundMessage msg;
        msg.msg_id = "msg_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        msg.topic = topic;
        msg.qos = qos;
        msg.payload = payload;
        msg.priority = priority;
        msg.created_at = std::chrono::steady_clock::now();
        msg.state = MessageState::PENDING;

        if (cache_queue_->enqueue(msg)) {
            result.accepted = true;
        } else {
            // mqtt.offline_queue.message_dropped event (CR-003 §4, MQTT-1006)
            auto queue_config = config_manager_->getOfflineQueueConfig();
            MqttLogAdapter::offline_queue().warn(
                "mqtt.offline_queue.message_dropped",
                "离线缓存达到上限，消息被丢弃",
                {
                    {"msg_id_hash", tbox::fw::log::FieldValue::makeString(msgIdHash(msg.msg_id))},
                    {"priority", tbox::fw::log::FieldValue::makeString(
                        [&]() -> std::string {
                            switch (priority) {
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

    return client_manager_->publish(topic, payload, qos);
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
        // mqtt.subscription.failed event (CR-003 §4, MQTT-1008)
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

void MqttService::onConnectionStateChanged(ConnectionState new_state) {
    if (new_state == ConnectionState::CONNECTING) {
        auto config = config_manager_->getConfig();
        MqttLogAdapter::connection().info(
            "mqtt.connection.connecting",
            "发起 MQTT 连接",
            {
                {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                {"broker_port", tbox::fw::log::FieldValue::makeInt(config.broker_port)},
                {"keepalive_s", tbox::fw::log::FieldValue::makeInt(config.keepalive)}
            }
        );
        if (!client_manager_->connect(config)) {
            // mqtt.connection.failed event (CR-003 §4, MQTT-1002)
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
        // mqtt.connection.established event (CR-003 §4)
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
        while (!cache_queue_->empty()) {
            auto msg = cache_queue_->dequeue();
            if (msg.has_value()) {
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
        // mqtt.connection.disconnected event (CR-003 §4, MQTT-1003)
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

    bus_interface_->sendMessage(route->internal_addr, payload);
}

void MqttService::onInternalMessage(const std::string& internal_addr,
                                    const std::vector<uint8_t>& payload) {
    auto route = route_bridge_->findRouteByInternalAddr(internal_addr);
    if (!route.has_value()) {
        MqttLogAdapter::router().warn(
            "mqtt.uplink.route_failed",
            "上行消息无路由映射",
            {
                {"internal_addr", tbox::fw::log::FieldValue::makeString(internal_addr)},
                {"payload_size", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(payload.size()))},
                {"reason", tbox::fw::log::FieldValue::makeString("no_route")}
            }
        );
        return;
    }

    publish(route->mqtt_topic, payload, route->qos);
}

} // namespace mqtt
} // namespace tbox
