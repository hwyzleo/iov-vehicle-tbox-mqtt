#pragma once

#include <string>
#include <vector>
#include <functional>
#include <map>
#include <mutex>

namespace tbox {
namespace mqtt {

// Internal bus interface (Design §1, §5)
class InternalBusInterface {
public:
    using MessageHandler = std::function<void(const std::string& service,
                                             const std::vector<uint8_t>& payload)>;

    InternalBusInterface();
    ~InternalBusInterface();

    // Register service on bus (Design §5)
    bool registerService(const std::string& service_name,
                        const std::string& address);

    // Unregister service from bus
    bool unregisterService(const std::string& service_name);

    // Send message to service (Design §5)
    bool sendMessage(const std::string& service_name,
                    const std::vector<uint8_t>& payload);

    // Set message handler for service
    void setMessageHandler(const std::string& service_name,
                          MessageHandler handler);

    // Get registered services
    std::vector<std::string> getRegisteredServices() const;

    // Simulate incoming message (for testing)
    void simulateIncomingMessage(const std::string& service_name,
                                const std::vector<uint8_t>& payload);

private:
    struct ServiceInfo {
        std::string address;
        MessageHandler handler;
    };

    std::map<std::string, ServiceInfo> services_;
    mutable std::mutex mutex_;
};

} // namespace mqtt
} // namespace tbox
