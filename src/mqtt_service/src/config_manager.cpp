#include "config_manager.h"
#include <fstream>

namespace tbox {
namespace mqtt {

ConfigManager::ConfigManager() {
}

ConfigManager::~ConfigManager() {
}

bool ConfigManager::loadDefaultConfig() {
    std::lock_guard<std::mutex> lock(mutex_);

    current_config_.broker_host = "localhost";
    current_config_.broker_port = 1883;
    current_config_.keepalive = 60;
    current_config_.clean_start = false;
    current_config_.session_expiry = 3600;
    current_config_.source = ConnectionConfig::Source::FACTORY;
    current_config_.version = 0;

    return true;
}

bool ConfigManager::applyConfig(const ConnectionConfig& config,
                               ConnectionConfig::Source source) {
    ChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!validateConfig(config)) {
            return false;
        }

        if (source == ConnectionConfig::Source::OTA &&
            config.version <= current_version_) {
            return false;
        }

        current_config_ = config;
        current_version_ = config.version;
        cb = change_callback_;
    }

    if (cb) cb();
    return true;
}

ConnectionConfig ConfigManager::getConfig() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_config_;
}

uint32_t ConfigManager::getCurrentVersion() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_version_;
}

bool ConfigManager::validateConfig(const ConnectionConfig& config) const {
    if (config.broker_host.empty()) {
        return false;
    }

    if (config.broker_port == 0 || config.broker_port > 65535) {
        return false;
    }

    if (config.client_id.empty()) {
        return false;
    }

    if (config.keepalive == 0) {
        return false;
    }

    return true;
}

void ConfigManager::setChangeCallback(ChangeCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    change_callback_ = callback;
}

bool ConfigManager::saveToFile(const std::string& filepath) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return false;
}

bool ConfigManager::loadFromFile(const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mutex_);
    return false;
}

void ConfigManager::notifyChange() {
    if (change_callback_) {
        change_callback_();
    }
}

} // namespace mqtt
} // namespace tbox
