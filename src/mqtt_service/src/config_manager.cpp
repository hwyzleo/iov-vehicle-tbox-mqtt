#include "config_manager.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>

namespace tbox {
namespace mqtt {

using json = nlohmann::json;

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

    json j;
    j["broker_host"] = current_config_.broker_host;
    j["broker_port"] = current_config_.broker_port;
    j["client_id"] = current_config_.client_id;
    j["keepalive"] = current_config_.keepalive;
    j["clean_start"] = current_config_.clean_start;
    j["session_expiry"] = current_config_.session_expiry;
    j["root_ca_ref"] = current_config_.root_ca_ref;
    j["tls_cert_ref"] = current_config_.tls_cert_ref;
    j["tls_key_ref"] = current_config_.tls_key_ref;
    j["key_source"] = (current_config_.key_source == KeySource::HSM) ? "hsm" : "file";

    std::ofstream ofs(filepath);
    if (!ofs.is_open()) {
        std::cerr << "Failed to open config file for writing: " << filepath << std::endl;
        return false;
    }

    ofs << j.dump(4) << std::endl;
    return true;
}

bool ConfigManager::loadFromFile(const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ifstream ifs(filepath);
    if (!ifs.is_open()) {
        std::cerr << "Config file not found: " << filepath << std::endl;
        return false;
    }

    json j;
    try {
        ifs >> j;
    } catch (const json::parse_error& e) {
        std::cerr << "Failed to parse config file: " << e.what() << std::endl;
        return false;
    }

    ConnectionConfig config;
    if (j.contains("broker_host")) config.broker_host = j["broker_host"].get<std::string>();
    if (j.contains("broker_port")) config.broker_port = j["broker_port"].get<uint16_t>();
    if (j.contains("client_id")) config.client_id = j["client_id"].get<std::string>();
    if (j.contains("keepalive")) config.keepalive = j["keepalive"].get<uint16_t>();
    if (j.contains("clean_start")) config.clean_start = j["clean_start"].get<bool>();
    if (j.contains("session_expiry")) config.session_expiry = j["session_expiry"].get<uint32_t>();
    if (j.contains("root_ca_ref")) config.root_ca_ref = j["root_ca_ref"].get<std::string>();
    if (j.contains("tls_cert_ref")) config.tls_cert_ref = j["tls_cert_ref"].get<std::string>();
    if (j.contains("tls_key_ref")) config.tls_key_ref = j["tls_key_ref"].get<std::string>();
    if (j.contains("key_source")) {
        std::string ks = j["key_source"].get<std::string>();
        config.key_source = (ks == "hsm") ? KeySource::HSM : KeySource::FILE;
    }

    if (!validateConfig(config)) {
        std::cerr << "Invalid config from file: " << filepath << std::endl;
        return false;
    }

    current_config_ = config;
    current_version_++;
    std::cout << "Loaded config from: " << filepath << std::endl;
    return true;
}

void ConfigManager::notifyChange() {
    if (change_callback_) {
        change_callback_();
    }
}

} // namespace mqtt
} // namespace tbox
