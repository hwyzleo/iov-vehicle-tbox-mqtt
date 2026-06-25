#pragma once

#include "mqtt_types.h"
#include <map>
#include <vector>
#include <optional>
#include <mutex>

namespace tbox {
namespace mqtt {

// Route bridge (Design §4.2, §5)
class RouteBridge {
public:
    RouteBridge();
    ~RouteBridge();

    // Register route mapping (Design §5)
    bool registerRoute(const RouteEntry& entry);

    // Remove route by internal address
    bool removeRoute(const std::string& internal_addr);

    // Find route by internal address
    std::optional<RouteEntry> findRouteByInternalAddr(const std::string& internal_addr) const;

    // Find route by MQTT topic
    std::optional<RouteEntry> findRouteByMqttTopic(const std::string& mqtt_topic) const;

    // Get all upstream routes (internal -> MQTT)
    std::vector<RouteEntry> getUpstreamRoutes() const;

    // Get all downstream routes (MQTT -> internal)
    std::vector<RouteEntry> getDownstreamRoutes() const;

    // Get all routes
    std::vector<RouteEntry> getAllRoutes() const;

    // Clear all routes
    void clear();

    // Get route count
    size_t size() const;

private:
    std::map<std::string, RouteEntry> internal_to_mqtt_;
    std::map<std::string, RouteEntry> mqtt_to_internal_;
    mutable std::mutex mutex_;
};

} // namespace mqtt
} // namespace tbox
