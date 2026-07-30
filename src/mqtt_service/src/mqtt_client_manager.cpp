#include "mqtt_client_manager.h"
#include "credential_manager.h"
#include "sec_tls_key_adapter.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <mosquitto.h>
#include <openssl/ssl.h>
#include <chrono>

namespace tbox {
namespace mqtt {

// Static callback wrappers for mosquitto
static void on_connect_with_flags_wrapper(struct mosquitto* mosq, void* userdata,
                                             int rc, int flags) {
    auto* client = static_cast<MqttClientManager*>(userdata);
    if (client) {
        client->on_connect(rc, flags);
    }
}

static void on_unsubscribe_wrapper(struct mosquitto* mosq, void* userdata, int mid) {
    auto* client = static_cast<MqttClientManager*>(userdata);
    if (client) {
        client->on_unsubscribe(mid);
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

bool MqttClientManager::initialize(CredentialManager& cred_mgr,
                                   SecTlsKeyAdapter& tls_adapter) {
    // CR-005 §5: TLS 凭据全部经 CredentialManager/SecTlsKeyAdapter 从 SEC 获取
    cred_mgr_ = &cred_mgr;
    tls_adapter_ = &tls_adapter;
    return true;
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

        mosquitto_connect_with_flags_callback_set(mosq_, on_connect_with_flags_wrapper);
        mosquitto_disconnect_callback_set(mosq_, on_disconnect_wrapper);
        mosquitto_message_callback_set(mosq_, on_message_wrapper);
        mosquitto_publish_callback_set(mosq_, on_publish_wrapper);
        mosquitto_subscribe_callback_set(mosq_, on_subscribe_wrapper);
        mosquitto_unsubscribe_callback_set(mosq_, on_unsubscribe_wrapper);
    }

    // Configure TLS via SEC credential (CR-005 §5)
    // TLS 凭据全部从 SEC 获取，不再使用文件路径
    if (!cred_mgr_ || !tls_adapter_) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        MqttLogAdapter::tls().error(
            "mqtt.tls.handshake_failed",
            "CredentialManager 或 SecTlsKeyAdapter 未初始化",
            {
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
    }

    // Get current credential snapshot (thread-safe, CR-005 §7)
    ActiveTlsCredential credential = cred_mgr_->getSnapshot();
    if (!credential.ready) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        MqttLogAdapter::tls().warn(
            "mqtt.certificate.waiting",
            "SEC 凭据未就绪，无法发起 TLS 连接",
            {
                {"profile", tbox::fw::log::FieldValue::makeString(config.credential_profile)},
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
    }

    // Build SSL_CTX from SEC credential (CR-005 §5.1)
    // root CA bundle -> X509_STORE, client cert chain -> SSL_CTX, private key -> ECDSA_METHOD
    SSL_CTX* ssl_ctx = tls_adapter_->buildTlsContext(
        credential, *cred_mgr_, config.min_tls_version, config.max_tls_version);
    if (!ssl_ctx) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        MqttLogAdapter::tls().error(
            "mqtt.tls.handshake_failed",
            "SSL_CTX 构建失败",
            {
                {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                {"reason", tbox::fw::log::FieldValue::makeString(tls_adapter_->getLastError())},
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
    }

    // Inject SSL_CTX into mosquitto (CR-005 §5)
    // MOSQ_OPT_SSL_CTX_WITH_DEFAULTS = 0: we configure everything ourselves
    mosquitto_int_option(mosq_, MOSQ_OPT_SSL_CTX_WITH_DEFAULTS, 0);
    int tls_rc = mosquitto_void_option(mosq_, MOSQ_OPT_SSL_CTX, ssl_ctx);
    if (tls_rc != MOSQ_ERR_SUCCESS) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        MqttLogAdapter::tls().error(
            "mqtt.tls.handshake_failed",
            "SSL_CTX 注入 mosquitto 失败",
            {
                {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(config.broker_host))},
                {"reason_code", tbox::fw::log::FieldValue::makeString(mosquitto_strerror(tls_rc))},
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
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

    int mid = 0;
    int rc = mosquitto_subscribe(mosq_,
                                &mid,
                                topic.c_str(),
                                static_cast<int>(qos));

    if (rc == MOSQ_ERR_SUCCESS) {
        result.status = true;
        std::lock_guard<std::mutex> lock(ack_mutex_);
        pending_subscribe_mids_[mid] = topic;
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

    int mid = 0;
    int rc = mosquitto_unsubscribe(mosq_,
                                  &mid,
                                  topic.c_str());

    if (rc == MOSQ_ERR_SUCCESS) {
        std::lock_guard<std::mutex> lock(ack_mutex_);
        pending_unsubscribe_mids_[mid] = topic;
        return true;
    }
    return false;
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

void MqttClientManager::on_connect(int rc, int flags) {
    connected_ = (rc == 0);
    session_present_ = (flags & 0x01) != 0;  // MQTT_CONNECT_FLAG_SESSION_PRESENT
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

void MqttClientManager::setSubscribeAckCallback(SubscribeAckCallback callback) {
    subscribe_ack_callback_ = std::move(callback);
}

void MqttClientManager::setUnsubscribeAckCallback(SubscribeAckCallback callback) {
    unsubscribe_ack_callback_ = std::move(callback);
}

void MqttClientManager::on_subscribe(int mid, int qos_count, const int* granted_qos) {
    std::string topic;
    {
        std::lock_guard<std::mutex> lock(ack_mutex_);
        auto it = pending_subscribe_mids_.find(mid);
        if (it != pending_subscribe_mids_.end()) {
            topic = it->second;
            pending_subscribe_mids_.erase(it);
        }
    }
    // success: granted_qos 不含 0x80 (Subscription Ack failure)
    bool success = (qos_count > 0 && granted_qos != nullptr && (granted_qos[0] & 0x80) == 0);
    if (!topic.empty() && subscribe_ack_callback_) {
        subscribe_ack_callback_(topic, success);
    }
    MqttLogAdapter::subscribe().debug(
        "mqtt.subscribe.acknowledged",
        "订阅确认",
        {{"mid", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(mid))},
         {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
         {"success", tbox::fw::log::FieldValue::makeBool(success)}}
    );
}

void MqttClientManager::on_unsubscribe(int mid) {
    std::string topic;
    {
        std::lock_guard<std::mutex> lock(ack_mutex_);
        auto it = pending_unsubscribe_mids_.find(mid);
        if (it != pending_unsubscribe_mids_.end()) {
            topic = it->second;
            pending_unsubscribe_mids_.erase(it);
        }
    }
    if (!topic.empty() && unsubscribe_ack_callback_) {
        unsubscribe_ack_callback_(topic, true);
    }
}

} // namespace mqtt
} // namespace tbox
