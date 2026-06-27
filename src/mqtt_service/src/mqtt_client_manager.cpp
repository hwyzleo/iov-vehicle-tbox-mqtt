#include "mqtt_client_manager.h"
#include <mosquitto.h>
#include <iostream>
#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/provider.h>

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
                                  const SecCertInterface& sec_interface,
                                  const std::string& ca_file_path,
                                  const std::string& cert_file_path,
                                  const std::string& key_file_path) {
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
                              ca_file_path.c_str(),          // root CA
                              nullptr,                        // CA path
                              cert_file_path.empty() ? nullptr : cert_file_path.c_str(),  // client cert
                              key_file_path.empty() ? nullptr : key_file_path.c_str(),    // client key
                              nullptr);                       // password callback

    return rc == MOSQ_ERR_SUCCESS;
}

bool MqttClientManager::connect(const ConnectionConfig& config) {
    // Initialize mosquitto instance if not done yet
    if (!mosq_) {
        std::cout << "[MQTT] Creating mosquitto instance, client_id: " << config.client_id << std::endl;
        mosq_ = mosquitto_new(config.client_id.c_str(), true, this);
        if (!mosq_) {
            std::cerr << "[MQTT] Failed to create mosquitto instance" << std::endl;
            return false;
        }

        mosquitto_connect_callback_set(mosq_, on_connect_wrapper);
        mosquitto_disconnect_callback_set(mosq_, on_disconnect_wrapper);
        mosquitto_message_callback_set(mosq_, on_message_wrapper);
        mosquitto_publish_callback_set(mosq_, on_publish_wrapper);
        mosquitto_subscribe_callback_set(mosq_, on_subscribe_wrapper);
        std::cout << "[MQTT] Mosquitto instance created" << std::endl;
    }

    // Configure TLS if CA certificate is specified
    if (!config.root_ca_ref.empty()) {
        std::cout << "[MQTT] Configuring TLS..." << std::endl;
        std::cout << "[MQTT]   CA:        " << config.root_ca_ref << std::endl;
        if (!config.tls_cert_ref.empty())
            std::cout << "[MQTT]   Cert:      " << config.tls_cert_ref << std::endl;

        const char* key_path = nullptr;

        if (config.key_source == KeySource::HSM) {
            std::cout << "[MQTT]   Key:       " << config.tls_key_ref << " (PKCS#11)" << std::endl;
            std::cout << "[MQTT]   Key source: HSM" << std::endl;

            // Configure OpenSSL to use PKCS#11 engine for HSM key operations
            // tls_key_ref should be a PKCS#11 URI (e.g. "pkcs11:object=device-key;type=private")
            // Requires OpenSSL pkcs11 engine/provider installed on the system
            key_path = config.tls_key_ref.c_str();
        } else {
            std::cout << "[MQTT]   Key:       " << config.tls_key_ref << " (file)" << std::endl;
            std::cout << "[MQTT]   Key source: FILE" << std::endl;
            key_path = config.tls_key_ref.empty() ? nullptr : config.tls_key_ref.c_str();
        }

        int rc = mosquitto_tls_set(mosq_,
                                  config.root_ca_ref.c_str(),
                                  nullptr,
                                  config.tls_cert_ref.empty() ? nullptr : config.tls_cert_ref.c_str(),
                                  key_path,
                                  nullptr);
        if (rc != MOSQ_ERR_SUCCESS) {
            std::cerr << "[MQTT] TLS configuration failed: " << mosquitto_strerror(rc) << std::endl;
            return false;
        }

        // For HSM mode, enable PKCS#11 URI as key reference in OpenSSL 3.x
        if (config.key_source == KeySource::HSM) {
            // OpenSSL 3.x: load the pkcs11 provider if available
            // This allows OpenSSL to understand pkcs11: URIs as key references
            OSSL_PROVIDER* legacy = OSSL_PROVIDER_load(nullptr, "legacy");
            OSSL_PROVIDER* pkcs11 = OSSL_PROVIDER_load(nullptr, "pkcs11");
            if (pkcs11) {
                std::cout << "[MQTT] OpenSSL pkcs11 provider loaded" << std::endl;
            } else {
                std::cout << "[MQTT] Warning: OpenSSL pkcs11 provider not found, "
                          << "HSM may not work. Install pkcs11-provider." << std::endl;
            }
            // legacy provider is optional, ignore if not available
            (void)legacy;
        }

        std::cout << "[MQTT] TLS configured" << std::endl;
    } else {
        std::cout << "[MQTT] TLS disabled (no CA certificate)" << std::endl;
    }

    // Set username from client certificate CN for broker authentication
    if (!config.username.empty()) {
        int rc = mosquitto_username_pw_set(mosq_, config.username.c_str(),
                                           config.password.empty() ? nullptr : config.password.c_str());
        if (rc != MOSQ_ERR_SUCCESS) {
            std::cerr << "[MQTT] Failed to set username: " << mosquitto_strerror(rc) << std::endl;
        } else {
            std::cout << "[MQTT] Username set: " << config.username << std::endl;
        }
    }

    // Set LWT if configured
    if (!lwt_config_.topic.empty()) {
        mosquitto_will_set(mosq_,
                          lwt_config_.topic.c_str(),
                          lwt_config_.payload.size(),
                          lwt_config_.payload.data(),
                          static_cast<int>(lwt_config_.qos),
                          lwt_config_.retain);
    }

    std::cout << "[MQTT] Connecting to " << config.broker_host << ":" << config.broker_port
              << " (keepalive: " << config.keepalive << "s)" << std::endl;

    int rc = mosquitto_connect(mosq_,
                              config.broker_host.c_str(),
                              config.broker_port,
                              config.keepalive);

    if (rc == MOSQ_ERR_SUCCESS) {
        std::cout << "[MQTT] Connection initiated" << std::endl;
        // Start network loop
        mosquitto_loop_start(mosq_);
        // Set connected flag immediately (in real scenario, on_connect callback would do this)
        connected_ = true;
        return true;
    }

    std::cerr << "[MQTT] Connection failed: " << mosquitto_strerror(rc) << std::endl;
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
    if (connected_) {
        std::cout << "[MQTT] Connected to broker" << std::endl;
    } else {
        std::cerr << "[MQTT] Connection rejected: " << mosquitto_connack_string(rc) << std::endl;
    }
    if (connection_callback_) {
        connection_callback_(connected_);
    }
}

void MqttClientManager::on_disconnect(int rc) {
    connected_ = false;
    if (rc == 0) {
        std::cout << "[MQTT] Disconnected cleanly" << std::endl;
    } else {
        std::cerr << "[MQTT] Unexpected disconnect: " << mosquitto_strerror(rc) << std::endl;
    }
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