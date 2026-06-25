#include "internal_bus_interface.h"

namespace tbox {
namespace mqtt {

InternalBusInterface::InternalBusInterface() {
}

InternalBusInterface::~InternalBusInterface() {
}

bool InternalBusInterface::registerService(const std::string& service_name,
                                          const std::string& address) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (services_.find(service_name) != services_.end()) {
        return false;  // Service already registered
    }

    ServiceInfo info;
    info.address = address;
    services_[service_name] = info;

    return true;
}

bool InternalBusInterface::unregisterService(const std::string& service_name) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = services_.find(service_name);
    if (it == services_.end()) {
        return false;
    }

    services_.erase(it);
    return true;
}

bool InternalBusInterface::sendMessage(const std::string& service_name,
                                      const std::vector<uint8_t>& payload) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = services_.find(service_name);
    if (it == services_.end()) {
        return false;
    }

    // In real implementation, this would send via IPC (DDS/SOME-IP/etc.)
    // For now, simulate successful send
    return true;
}

void InternalBusInterface::setMessageHandler(const std::string& service_name,
                                            MessageHandler handler) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = services_.find(service_name);
    if (it != services_.end()) {
        it->second.handler = handler;
    } else {
        // Create a new service entry with just the handler
        ServiceInfo info;
        info.handler = handler;
        services_[service_name] = info;
    }
}

std::vector<std::string> InternalBusInterface::getRegisteredServices() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> result;
    for (const auto& pair : services_) {
        result.push_back(pair.first);
    }
    return result;
}

void InternalBusInterface::simulateIncomingMessage(const std::string& service_name,
                                                   const std::vector<uint8_t>& payload) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = services_.find(service_name);
    if (it != services_.end() && it->second.handler) {
        it->second.handler(service_name, payload);
    }
}

} // namespace mqtt
} // namespace tbox
