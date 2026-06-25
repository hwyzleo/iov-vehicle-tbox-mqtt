#include "route_bridge.h"

namespace tbox {
namespace mqtt {

RouteBridge::RouteBridge() {
}

RouteBridge::~RouteBridge() {
}

bool RouteBridge::registerRoute(const RouteEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check for existing routes
    if (internal_to_mqtt_.find(entry.internal_addr) != internal_to_mqtt_.end()) {
        return false;  // Route already exists
    }

    if (mqtt_to_internal_.find(entry.mqtt_topic) != mqtt_to_internal_.end()) {
        return false;  // Topic already mapped
    }

    // Register route
    internal_to_mqtt_[entry.internal_addr] = entry;
    mqtt_to_internal_[entry.mqtt_topic] = entry;

    return true;
}

bool RouteBridge::removeRoute(const std::string& internal_addr) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = internal_to_mqtt_.find(internal_addr);
    if (it == internal_to_mqtt_.end()) {
        return false;
    }

    // Remove from both maps
    mqtt_to_internal_.erase(it->second.mqtt_topic);
    internal_to_mqtt_.erase(it);

    return true;
}

std::optional<RouteEntry> RouteBridge::findRouteByInternalAddr(
    const std::string& internal_addr) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = internal_to_mqtt_.find(internal_addr);
    if (it != internal_to_mqtt_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::optional<RouteEntry> RouteBridge::findRouteByMqttTopic(
    const std::string& mqtt_topic) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = mqtt_to_internal_.find(mqtt_topic);
    if (it != mqtt_to_internal_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<RouteEntry> RouteBridge::getUpstreamRoutes() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<RouteEntry> result;
    for (const auto& pair : internal_to_mqtt_) {
        if (pair.second.direction == RouteEntry::Direction::UP) {
            result.push_back(pair.second);
        }
    }
    return result;
}

std::vector<RouteEntry> RouteBridge::getDownstreamRoutes() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<RouteEntry> result;
    for (const auto& pair : internal_to_mqtt_) {
        if (pair.second.direction == RouteEntry::Direction::DOWN) {
            result.push_back(pair.second);
        }
    }
    return result;
}

std::vector<RouteEntry> RouteBridge::getAllRoutes() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<RouteEntry> result;
    for (const auto& pair : internal_to_mqtt_) {
        result.push_back(pair.second);
    }
    return result;
}

void RouteBridge::clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    internal_to_mqtt_.clear();
    mqtt_to_internal_.clear();
}

size_t RouteBridge::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return internal_to_mqtt_.size();
}

} // namespace mqtt
} // namespace tbox
