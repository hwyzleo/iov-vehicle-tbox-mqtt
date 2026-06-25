#include "mqtt_service.h"
#include "connection_state_machine.h"
#include "mqtt_client_manager.h"
#include "offline_cache_queue.h"
#include "route_bridge.h"
#include "config_manager.h"
#include "sec_cert_interface.h"
#include "internal_bus_interface.h"

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
                            std::shared_ptr<InternalBusInterface> bus_interface) {
    if (!sec_interface || !bus_interface) {
        return false;
    }

    sec_interface_ = sec_interface;
    bus_interface_ = bus_interface;

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
        state_machine_->onCertificateReady();
    }

    return true;
}

void MqttService::stop() {
    client_manager_->disconnect();
}

PublishResult MqttService::publish(const std::string& topic,
                                  const std::vector<uint8_t>& payload,
                                  QoS qos,
                                  Priority priority) {
    PublishResult result;

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
    if (new_state == ConnectionState::CONNECTED) {
        while (!cache_queue_->empty()) {
            auto msg = cache_queue_->dequeue();
            if (msg.has_value()) {
                client_manager_->publish(msg->topic, msg->payload, msg->qos);
            }
        }

        auto downstream_routes = route_bridge_->getDownstreamRoutes();
        for (const auto& route : downstream_routes) {
            client_manager_->subscribe(route.mqtt_topic, route.qos);
        }
    }
}

void MqttService::onMessageReceived(const std::string& topic,
                                   const std::vector<uint8_t>& payload) {
    auto route = route_bridge_->findRouteByMqttTopic(topic);
    if (!route.has_value()) {
        return;
    }

    bus_interface_->sendMessage(route->internal_addr, payload);
}

void MqttService::onInternalMessage(const std::string& internal_addr,
                                   const std::vector<uint8_t>& payload) {
    auto route = route_bridge_->findRouteByInternalAddr(internal_addr);
    if (!route.has_value()) {
        return;
    }

    publish(route->mqtt_topic, payload, route->qos);
}

} // namespace mqtt
} // namespace tbox
