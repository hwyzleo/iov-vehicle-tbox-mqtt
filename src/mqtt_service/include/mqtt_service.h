#pragma once

#include "mqtt_types.h"
#include "store.h"
#include <functional>
#include <memory>

namespace tbox {
namespace mqtt {

// Forward declarations
class ConnectionStateMachine;
class MqttClientManager;
class OfflineCacheQueue;
class RouteBridge;
class ConfigManager;
class SecCertInterface;
class InternalBusInterface;

// Main MQTT service interface (Design §5)
class MqttService {
public:
    MqttService();
    ~MqttService();

    // Initialize service with dependencies and framework paths (Design §5)
    bool initialize(std::shared_ptr<SecCertInterface> sec_interface,
                    std::shared_ptr<InternalBusInterface> bus_interface,
                    const std::string& config_root,
                    const std::string& store_root);

    // Start service
    bool start();

    // Stop service
    void stop();

    // API: Publish message (Design §5)
    PublishResult publish(const std::string& topic,
                          const std::vector<uint8_t>& payload,
                          QoS qos,
                          Priority priority = Priority::NORMAL);

    // API: Subscribe to topic (Design §5)
    SubscribeResult subscribe(const std::string& topic,
                              QoS qos,
                              const std::string& target_service);

    // API: Register route (Design §5)
    bool registerRoute(const std::string& internal_addr,
                       const std::string& mqtt_topic,
                       RouteEntry::Direction direction);

    // API: Get connection state (Design §5)
    ConnectionStateResult getConnectionState() const;

    // API: Apply configuration (Design §5)
    bool applyConfig(const ConnectionConfig& config,
                     ConnectionConfig::Source source);

private:
    // Connection state changed callback
    void onConnectionStateChanged(ConnectionState new_state);

    // Message received from cloud
    void onMessageReceived(const std::string& topic,
                           const std::vector<uint8_t>& payload);

    // Internal message from bus
    void onInternalMessage(const std::string& internal_addr,
                           const std::vector<uint8_t>& payload);

    std::unique_ptr<ConnectionStateMachine> state_machine_;
    std::unique_ptr<MqttClientManager> client_manager_;
    std::unique_ptr<OfflineCacheQueue> cache_queue_;
    std::unique_ptr<RouteBridge> route_bridge_;
    std::unique_ptr<ConfigManager> config_manager_;
    std::shared_ptr<SecCertInterface> sec_interface_;
    std::shared_ptr<InternalBusInterface> bus_interface_;
    std::shared_ptr<hwyz::store::Store> store_;
};

} // namespace mqtt
} // namespace tbox
