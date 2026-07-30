#pragma once

#include "mqtt_types.h"
#include "config.h"
#include "store.h"
#include <functional>
#include <mutex>
#include <memory>

namespace tbox {
namespace mqtt {

class ConfigManager {
public:
    using ChangeCallback = std::function<void()>;

    ConfigManager();
    ~ConfigManager();

    // Load static config from framework-config (read-only, Design §5)
    bool loadFromFramework(const std::string& config_root);

    // Load OTA-updatable params from framework-store (Design §5)
    bool loadFromStore(hwyz::store::Store& store);

    // Apply config: source=OTA writes updatable params to framework-store (Design §5)
    bool applyConfig(const ConnectionConfig& config, ConnectionConfig::Source source);
    ConnectionConfig getConfig() const;
    uint32_t getCurrentVersion() const;
    bool validateConfig(const ConnectionConfig& config) const;
    void setChangeCallback(ChangeCallback callback);

    // Set framework-store reference for OTA param persistence
    void setStore(std::shared_ptr<hwyz::store::Store> store);

    // Get framework-config snapshot (for OfflineCacheQueue etc.)
    std::shared_ptr<const hwyz::config::ImmutableConfigView> getSnapshot() const;

    // Get offline queue config (sourced from framework-config)
    OfflineQueueConfig getOfflineQueueConfig() const;

    // Get subscription snapshot config (CR-007 §6)
    SubscriptionConfig getSubscriptionConfig() const;

private:
    void notifyChange();
    OfflineQueueConfig readOfflineQueueConfig() const;
    SubscriptionConfig readSubscriptionConfig() const;

    ConnectionConfig current_config_;
    uint32_t current_version_ = 0;
    ChangeCallback change_callback_;
    std::shared_ptr<hwyz::store::Store> store_;
    std::shared_ptr<const hwyz::config::ImmutableConfigView> config_snapshot_;
    OfflineQueueConfig offline_queue_config_;
    SubscriptionConfig subscription_config_;
    mutable std::mutex mutex_;
};

} // namespace mqtt
} // namespace tbox
