#pragma once

#include "mqtt_types.h"
#include <functional>
#include <mutex>

namespace tbox {
namespace mqtt {

class ConfigManager {
public:
    using ChangeCallback = std::function<void()>;

    ConfigManager();
    ~ConfigManager();

    bool loadDefaultConfig();
    bool applyConfig(const ConnectionConfig& config, ConnectionConfig::Source source);
    ConnectionConfig getConfig() const;
    uint32_t getCurrentVersion() const;
    bool validateConfig(const ConnectionConfig& config) const;
    void setChangeCallback(ChangeCallback callback);
    bool saveToFile(const std::string& filepath) const;
    bool loadFromFile(const std::string& filepath);

private:
    void notifyChange();

    ConnectionConfig current_config_;
    uint32_t current_version_ = 0;
    ChangeCallback change_callback_;
    mutable std::mutex mutex_;
};

} // namespace mqtt
} // namespace tbox
