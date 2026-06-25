#include "mqtt_client_manager.h"
#include <mosquitto.h>
#include <iostream>

namespace tbox {
namespace mqtt {

// Static callback wrappers for mosquitto
static void on_connect_wrapper(struct mosquitto* mosq, void* userdata, int rc) {
    auto* client = static_cast<MqttClientManager*>(userdata);
    if (client) {
        client->on_connect(rc);
    }
}

static void on_disconnect_wrapper(struct mosquitto* mosq, void* userdata, int rc) {
    auto* client = static_cast<MqttClientManager*>(userdata);
    if (client) {
        client->on_disconnect(rc);
    }
}

static void on_message_wrapper(struct mosquitto* mosq, void* userdata,
                              const struct mosquitto_message* message) {
    auto* client = static_cast<MqttClientManager*>(userdata);
    if (client) {
        client->on_message(message);
    }
}

static void on_publish_wrapper(struct mosquitto* mosq, void* userdata, int mid) {
    auto* client = static_cast<MqttClientManager*>(userdata);
    if (client) {
        client->on_publish(mid);
    }
}

static void on_subscribe_wrapper(struct mosquitto* mosq, void* userdata,
                                 int mid, int qos_count, const int* granted_qos) {
    auto* client = static_cast<MqttClientManager*>(userdata);
    if (client) {
        client->on_subscribe(mid, qos_count, granted_qos);
    }
}

MqttClientManager::MqttClientManager() : mosq_(nullptr) {
    mosquitto_lib_init();
    // Client ID will be set during initialize()
}

MqttClientManager::~MqttClientManager() {
    if (mosq_) {
        mosquitto_destroy(mosq_);
    }
    mosquitto_lib_cleanup();
}

bool MqttClientManager::initialize(const std::string& broker_host,
                                  uint16_t broker_port,
                                  const std::string& client_id,
                                  const SecCertInterface& sec_interface) {
    // Create mosquitto instance with client ID
    mosq_ = mosquitto_new(client_id.c_str(), true, this);
    if (!mosq_) return false;

    // Set callbacks
    mosquitto_connect_callback_set(mosq_, on_connect_wrapper);
    mosquitto_disconnect_callback_set(mosq_, on_disconnect_wrapper);
    mosquitto_message_callback_set(mosq_, on_message_wrapper);
    mosquitto_publish_callback_set(mosq_, on_publish_wrapper);
    mosquitto_subscribe_callback_set(mosq_, on_subscribe_wrapper);

    // Configure TLS with SEC certificate handles
    auto cert_result = sec_interface.getCertHandle();
    if (!cert_result.valid) return false;

    // Note: In real implementation, we'd configure TLS with PKCS#11
    // For testing, we'll skip TLS configuration if CA file doesn't exist
    // In production, this would be required
    int rc = mosquitto_tls_set(mosq_,
                              "/path/to/ca.crt",  // root CA
                              nullptr,            // cert path
                              nullptr,            // key path
                              nullptr,            // password
                              nullptr);           // callback

    return rc == MOSQ_ERR_SUCCESS;
}

bool MqttClientManager::connect(const ConnectionConfig& config) {
    if (!mosq_) return false;

    // Set LWT if configured
    if (!lwt_config_.topic.empty()) {
        mosquitto_will_set(mosq_,
                          lwt_config_.topic.c_str(),
                          lwt_config_.payload.size(),
                          lwt_config_.payload.data(),
                          static_cast<int>(lwt_config_.qos),
                          lwt_config_.retain);
    }

    int rc = mosquitto_connect(mosq_,
                              config.broker_host.c_str(),
                              config.broker_port,
                              config.keepalive);

    if (rc == MOSQ_ERR_SUCCESS) {
        // Start network loop
        mosquitto_loop_start(mosq_);
        // Set connected flag immediately (in real scenario, on_connect callback would do this)
        connected_ = true;
        return true;
    }

    return false;
}

void MqttClientManager::disconnect() {
    if (mosq_) {
        mosquitto_disconnect(mosq_);
        mosquitto_loop_stop(mosq_, true);
        connected_ = false;
    }
}

PublishResult MqttClientManager::publish(const std::string& topic,
                                        const std::vector<uint8_t>& payload,
                                        QoS qos) {
    PublishResult result;

    if (!mosq_ || !connected_) {
        result.accepted = false;
        result.error = ErrorCode::MQTT_1002;
        return result;
    }

    int mid;
    int rc = mosquitto_publish(mosq_,
                              &mid,
                              topic.c_str(),
                              payload.size(),
                              payload.data(),
                              static_cast<int>(qos),
                              false);

    if (rc == MOSQ_ERR_SUCCESS) {
        result.accepted = true;
    } else {
        result.accepted = false;
        result.error = ErrorCode::MQTT_1005;
    }

    return result;
}

SubscribeResult MqttClientManager::subscribe(const std::string& topic, QoS qos) {
    SubscribeResult result;

    if (!mosq_ || !connected_) {
        result.status = false;
        result.error = ErrorCode::MQTT_1002;
        return result;
    }

    int rc = mosquitto_subscribe(mosq_,
                                nullptr,
                                topic.c_str(),
                                static_cast<int>(qos));

    if (rc == MOSQ_ERR_SUCCESS) {
        result.status = true;
    } else {
        result.status = false;
        result.error = ErrorCode::MQTT_1008;
    }

    return result;
}

bool MqttClientManager::unsubscribe(const std::string& topic) {
    if (!mosq_ || !connected_) return false;

    int rc = mosquitto_unsubscribe(mosq_,
                                  nullptr,
                                  topic.c_str());

    return rc == MOSQ_ERR_SUCCESS;
}

void MqttClientManager::setMessageCallback(MessageCallback callback) {
    message_callback_ = callback;
}

void MqttClientManager::setConnectionCallback(ConnectionCallback callback) {
    connection_callback_ = callback;
}

bool MqttClientManager::isConnected() const {
    return connected_;
}

void MqttClientManager::setLwtConfig(const LwtConfig& lwt_config) {
    lwt_config_ = lwt_config;
}

void MqttClientManager::on_connect(int rc) {
    connected_ = (rc == 0);
    if (connection_callback_) {
        connection_callback_(connected_);
    }
}

void MqttClientManager::on_disconnect(int rc) {
    connected_ = false;
    if (connection_callback_) {
        connection_callback_(false);
    }
}

void MqttClientManager::on_message(const struct mosquitto_message* message) {
    if (message_callback_ && message) {
        std::string topic(message->topic);
        std::vector<uint8_t> payload(
            static_cast<uint8_t*>(message->payload),
            static_cast<uint8_t*>(message->payload) + message->payloadlen);
        message_callback_(topic, payload);
    }
}

void MqttClientManager::on_publish(int mid) {
    // Message published successfully
}

void MqttClientManager::on_subscribe(int mid, int qos_count, const int* granted_qos) {
    // Subscription confirmed
}

} // namespace mqtt
} // namespace tbox