#include "mqtt_client_manager.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <mosquitto.h>
#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/provider.h>
#include <chrono>

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

    int rc = mosquitto_tls_set(mosq_,
                              ca_file_path.c_str(),
                              nullptr,
                              cert_file_path.empty() ? nullptr : cert_file_path.c_str(),
                              key_file_path.empty() ? nullptr : key_file_path.c_str(),
                              nullptr);

    return rc == MOSQ_ERR_SUCCESS;
}

bool MqttClientManager::connect(const ConnectionConfig& config) {
    auto start_time = std::chrono::steady_clock::now();

    // Initialize mosquitto instance if not done yet
    if (!mosq_) {
        MqttLogAdapter::connection().debug(
            "mqtt.client.creating",
            "创建 mosquitto 实例",
            {{"client_id_hash", tbox::fw::log::FieldValue::makeString(clientIdHash(config.client_id))}}
        );
        mosq_ = mosquitto_new(config.client_id.c_str(), true, this);
        if (!mosq_) {
            MqttLogAdapter::connection().error(
                "mqtt.connection.failed",
                "mosquitto 实例创建失败",
                {
                    {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                    {"reason_code", tbox::fw::log::FieldValue::makeString("mosq_new_failed")},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1002")}
                }
            );
            return false;
        }

        mosquitto_connect_callback_set(mosq_, on_connect_wrapper);
        mosquitto_disconnect_callback_set(mosq_, on_disconnect_wrapper);
        mosquitto_message_callback_set(mosq_, on_message_wrapper);
        mosquitto_publish_callback_set(mosq_, on_publish_wrapper);
        mosquitto_subscribe_callback_set(mosq_, on_subscribe_wrapper);
    }

    // Configure TLS if CA certificate is specified
    if (!config.root_ca_ref.empty()) {
        // Note: key_source and tls_key_ref are logged without exposing the actual key path value
        // as they may contain PKCS#11 URIs or file paths (CR-003 §5)
        std::string key_source_str = (config.key_source == KeySource::HSM) ? "HSM" : "FILE";

        MqttLogAdapter::tls().info(
            "mqtt.tls.configuring",
            "配置 TLS",
            {
                {"key_source", tbox::fw::log::FieldValue::makeString(key_source_str)},
                {"has_ca", tbox::fw::log::FieldValue::makeBool(!config.root_ca_ref.empty())},
                {"has_cert", tbox::fw::log::FieldValue::makeBool(!config.tls_cert_ref.empty())}
            }
        );

        const char* key_path = nullptr;

        if (config.key_source == KeySource::HSM) {
            key_path = config.tls_key_ref.c_str();
        } else {
            key_path = config.tls_key_ref.empty() ? nullptr : config.tls_key_ref.c_str();
        }

        int rc = mosquitto_tls_set(mosq_,
                                  config.root_ca_ref.c_str(),
                                  nullptr,
                                  config.tls_cert_ref.empty() ? nullptr : config.tls_cert_ref.c_str(),
                                  key_path,
                                  nullptr);
        if (rc != MOSQ_ERR_SUCCESS) {
            // mqtt.tls.handshake_failed event (CR-003 §4, MQTT-1001)
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time);
            MqttLogAdapter::tls().error(
                "mqtt.tls.handshake_failed",
                "TLS 配置失败",
                {
                    {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                    {"reason_code", tbox::fw::log::FieldValue::makeString(mosquitto_strerror(rc))},
                    {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                    {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
                }
            );
            return false;
        }

        // For HSM mode, enable PKCS#11 URI as key reference in OpenSSL 3.x
        if (config.key_source == KeySource::HSM) {
            OSSL_PROVIDER* legacy = OSSL_PROVIDER_load(nullptr, "legacy");
            OSSL_PROVIDER* pkcs11 = OSSL_PROVIDER_load(nullptr, "pkcs11");
            if (pkcs11) {
                MqttLogAdapter::tls().info(
                    "mqtt.tls.pkcs11_loaded",
                    "OpenSSL pkcs11 provider 已加载"
                );
            } else {
                MqttLogAdapter::tls().warn(
                    "mqtt.tls.pkcs11_missing",
                    "OpenSSL pkcs11 provider 未找到，HSM 可能无法工作"
                );
            }
            (void)legacy;
        }
    } else {
        MqttLogAdapter::tls().warn(
            "mqtt.tls.disabled",
            "TLS 未启用（无 CA 证书）"
        );
    }

    // Set username from client certificate CN for broker authentication
    if (!config.username.empty()) {
        int rc = mosquitto_username_pw_set(mosq_, config.username.c_str(),
                                           config.password.empty() ? nullptr : config.password.c_str());
        if (rc != MOSQ_ERR_SUCCESS) {
            MqttLogAdapter::connection().warn(
                "mqtt.auth.username_set_failed",
                "用户名设置失败",
                {{"reason_code", tbox::fw::log::FieldValue::makeString(mosquitto_strerror(rc))}}
            );
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

    MqttLogAdapter::connection().info(
        "mqtt.connection.connecting",
        "正在连接 Broker",
        {
            {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
            {"broker_port", tbox::fw::log::FieldValue::makeInt(config.broker_port)},
            {"keepalive_s", tbox::fw::log::FieldValue::makeInt(config.keepalive)}
        }
    );

    int rc = mosquitto_connect(mosq_,
                              config.broker_host.c_str(),
                              config.broker_port,
                              config.keepalive);

    if (rc == MOSQ_ERR_SUCCESS) {
        mosquitto_loop_start(mosq_);
        connected_ = true;
        return true;
    }

    // mqtt.connection.failed event (CR-003 §4, MQTT-1002)
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time);
    MqttLogAdapter::connection().error(
        "mqtt.connection.failed",
        "连接 Broker 失败",
        {
            {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
            {"reason_code", tbox::fw::log::FieldValue::makeString(mosquitto_strerror(rc))},
            {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
            {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1002")}
        }
    );
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
    auto start_time = std::chrono::steady_clock::now();

    if (!mosq_ || !connected_) {
        result.accepted = false;
        result.error = ErrorCode::MQTT_1002;
        return result;
    }

    // Topic validation (CR-003 §4: mqtt.publish.rejected MQTT-1004)
    if (topic.empty()) {
        MqttLogAdapter::publish().warn(
            "mqtt.publish.rejected",
            "topic 非法，拒绝发布",
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
        // mqtt.publish.acknowledged event (CR-003 §4, DEBUG level)
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        MqttLogAdapter::publish().debug(
            "mqtt.publish.acknowledged",
            "QoS1 PUBACK 成功",
            {
                {"msg_id_hash", tbox::fw::log::FieldValue::makeString(
                    msgIdHash("mid_" + std::to_string(mid)))},
                {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
                {"qos", tbox::fw::log::FieldValue::makeString(qosToString(qos))},
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())}
            }
        );
    } else {
        result.accepted = false;
        result.error = ErrorCode::MQTT_1005;
        // mqtt.publish.failed event (CR-003 §4, MQTT-1005)
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        MqttLogAdapter::publish().error(
            "mqtt.publish.failed",
            "发布超时或重试耗尽",
            {
                {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
                {"qos", tbox::fw::log::FieldValue::makeString(qosToString(qos))},
                {"reason_code", tbox::fw::log::FieldValue::makeString(mosquitto_strerror(rc))},
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1005")}
            }
        );
    }

    return result;
}

SubscribeResult MqttClientManager::subscribe(const std::string& topic, QoS qos) {
    SubscribeResult result;
    auto start_time = std::chrono::steady_clock::now();

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
        // mqtt.subscription.failed event (CR-003 §4, MQTT-1008)
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.failed",
            "订阅请求失败",
            {
                {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
                {"qos", tbox::fw::log::FieldValue::makeString(qosToString(qos))},
                {"reason_code", tbox::fw::log::FieldValue::makeString(mosquitto_strerror(rc))},
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1008")}
            }
        );
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
        // mqtt.connection.established event (CR-003 §4)
        MqttLogAdapter::connection().info(
            "mqtt.connection.established",
            "MQTT CONNECT/CONNACK 成功",
            {
                {"session_present", tbox::fw::log::FieldValue::makeBool(rc == 0)}
            }
        );
    } else {
        // mqtt.connection.failed event (CR-003 §4, MQTT-1002)
        MqttLogAdapter::connection().error(
            "mqtt.connection.failed",
            "Broker 拒绝连接",
            {
                {"reason_code", tbox::fw::log::FieldValue::makeString(
                    std::to_string(rc))},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1002")}
            }
        );
    }
    if (connection_callback_) {
        connection_callback_(connected_);
    }
}

void MqttClientManager::on_disconnect(int rc) {
    connected_ = false;
    // mqtt.connection.disconnected event (CR-003 §4, MQTT-1003)
    MqttLogAdapter::connection().warn(
        "mqtt.connection.disconnected",
        "连接异常断开",
        {
            {"reason_code", tbox::fw::log::FieldValue::makeString(
                rc == 0 ? "clean" : std::to_string(rc))},
            {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1003")}
        }
    );
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
    // PUBACK callback - already logged in publish() for immediate ack
    // This callback confirms the publish was received by broker
}

void MqttClientManager::on_subscribe(int mid, int qos_count, const int* granted_qos) {
    // Subscription confirmed by broker
    MqttLogAdapter::subscribe().debug(
        "mqtt.subscribe.acknowledged",
        "订阅确认",
        {{"mid", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(mid))}}
    );
}

} // namespace mqtt
} // namespace tbox
