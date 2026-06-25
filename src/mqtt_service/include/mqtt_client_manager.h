#pragma once

#include "mqtt_types.h"
#include "sec_cert_interface.h"
#include <functional>
#include <memory>

struct mosquitto;
struct mosquitto_message;

namespace tbox {
namespace mqtt {

// MQTT client manager (Design §2)
class MqttClientManager {
public:
    using MessageCallback = std::function<void(const std::string& topic,
                                               const std::vector<uint8_t>& payload)>;
    using ConnectionCallback = std::function<void(bool connected)>;

    MqttClientManager();
    ~MqttClientManager();

    // Initialize client with SEC interface
    bool initialize(const std::string& broker_host,
                   uint16_t broker_port,
                   const std::string& client_id,
                   const SecCertInterface& sec_interface);

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

    // Check if connected
    bool isConnected() const;

    // Set LWT configuration (Design §3)
    void setLwtConfig(const LwtConfig& lwt_config);

    // Public callback handlers (called from static wrappers)
    void on_connect(int rc);
    void on_disconnect(int rc);
    void on_message(const ::mosquitto_message* message);
    void on_publish(int mid);
    void on_subscribe(int mid, int qos_count, const int* granted_qos);

private:
    struct mosquitto* mosq_;  // mosquitto instance
    bool connected_ = false;
    std::string client_id_;
    MessageCallback message_callback_;
    ConnectionCallback connection_callback_;
    LwtConfig lwt_config_;
};

} // namespace mqtt
} // namespace tbox