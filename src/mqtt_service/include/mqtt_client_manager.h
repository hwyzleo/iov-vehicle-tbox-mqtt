#pragma once

#include "mqtt_types.h"
#include <functional>
#include <map>
#include <memory>
#include <mutex>

struct mosquitto;
struct mosquitto_message;

namespace tbox {
namespace mqtt {

// Forward declarations (CR-005)
class CredentialManager;
class SecTlsKeyAdapter;

// MQTT client manager (Design §2, CR-005 §5)
// TLS 凭据全部经 CredentialManager/SecTlsKeyAdapter 从 SEC 获取，不再使用文件路径。
class MqttClientManager {
public:
    using MessageCallback = std::function<void(const std::string& topic,
                                               const std::vector<uint8_t>& payload)>;
    using ConnectionCallback = std::function<void(bool connected)>;
    // CR-007 §9: SUBACK/UNSUBACK 跟踪（topic 关联）
    using SubscribeAckCallback = std::function<void(const std::string& topic, bool success)>;

    MqttClientManager();
    ~MqttClientManager();

    // Initialize client with SEC credential manager and TLS adapter (CR-005 §5)
    bool initialize(CredentialManager& cred_mgr,
                    SecTlsKeyAdapter& tls_adapter);

    // Connect to broker
    bool connect(const ConnectionConfig& config);

    // Disconnect from broker
    void disconnect();

    // Publish message (Design §5)
    PublishResult publish(const std::string& topic,
                         const std::vector<uint8_t>& payload,
                         QoS qos);

    // Subscribe to topic (Design §5)
    SubscribeResult subscribe(const std::string& topic, QoS qos);

    // Unsubscribe from topic
    bool unsubscribe(const std::string& topic);

    // Set message callback
    void setMessageCallback(MessageCallback callback);

    // Set connection callback
    void setConnectionCallback(ConnectionCallback callback);

    // CR-007 §9: SUBACK/UNSUBACK 回调（reconciler 跟踪订阅状态）
    void setSubscribeAckCallback(SubscribeAckCallback callback);
    void setUnsubscribeAckCallback(SubscribeAckCallback callback);

    // Check if connected
    bool isConnected() const;

    // CR-007 §8: CONNACK session_present
    bool getSessionPresent() const { return session_present_; }

    // Set LWT configuration (Design §3)
    void setLwtConfig(const LwtConfig& lwt_config);

    // Public callback handlers (called from static wrappers)
    void on_connect(int rc, int flags);
    void on_disconnect(int rc);
    void on_message(const ::mosquitto_message* message);
    void on_publish(int mid);
    void on_subscribe(int mid, int qos_count, const int* granted_qos);
    void on_unsubscribe(int mid);  // CR-007 §9

private:
    struct mosquitto* mosq_;  // mosquitto instance
    bool connected_ = false;
    MessageCallback message_callback_;
    ConnectionCallback connection_callback_;
    LwtConfig lwt_config_;

    // SEC credential & TLS adapter (CR-005 §5)
    CredentialManager* cred_mgr_ = nullptr;
    SecTlsKeyAdapter* tls_adapter_ = nullptr;

    // CR-007 §9: SUBACK/UNSUBACK mid->topic 映射与回调
    SubscribeAckCallback subscribe_ack_callback_;
    SubscribeAckCallback unsubscribe_ack_callback_;
    std::mutex ack_mutex_;
    std::map<int, std::string> pending_subscribe_mids_;
    std::map<int, std::string> pending_unsubscribe_mids_;
    bool session_present_ = false;  // CONNACK session_present
};

} // namespace mqtt
} // namespace tbox