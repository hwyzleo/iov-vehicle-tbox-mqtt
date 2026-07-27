#include "config_manager.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"

namespace tbox {
namespace mqtt {

ConfigManager::ConfigManager() {
}

ConfigManager::~ConfigManager() {
}

bool ConfigManager::loadFromFramework(const std::string& config_root) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto& fw_config = hwyz::config::ConfigManager::instance();
    auto err = fw_config.load("mqtt", config_root);
    if (err != hwyz::config::ConfigError::kOk) {
        auto info = fw_config.getLastError();
        MqttLogAdapter::ipc().error(
            "mqtt.config.load_failed",
            "framework-config 加载失败",
            {
                {"config_root", tbox::fw::log::FieldValue::makeString(config_root)},
                {"reason", tbox::fw::log::FieldValue::makeString(info.message)}
            }
        );
        return false;
    }

    config_snapshot_ = fw_config.getSnapshot();
    if (!config_snapshot_) {
        MqttLogAdapter::ipc().error(
            "mqtt.config.snapshot_null",
            "配置快照为空"
        );
        return false;
    }

    // Read static config fields from framework-config snapshot
    current_config_.root_ca_ref = config_snapshot_->getString("mqtt.root_ca_ref", "");
    current_config_.client_id = config_snapshot_->getString("mqtt.client_id", "");
    current_config_.keepalive = static_cast<uint16_t>(config_snapshot_->getInt("mqtt.keepalive", 60));
    current_config_.clean_start = config_snapshot_->getBool("mqtt.clean_start", false);
    current_config_.session_expiry = static_cast<uint32_t>(config_snapshot_->getInt("mqtt.session_expiry", 3600));
    current_config_.tls_cert_ref = config_snapshot_->getString("mqtt.tls_cert_ref", "");
    current_config_.tls_key_ref = config_snapshot_->getString("mqtt.tls_key_ref", "");

    std::string ks = config_snapshot_->getString("mqtt.key_source", "file");
    current_config_.key_source = (ks == "hsm") ? KeySource::HSM : KeySource::FILE;

    current_config_.username = config_snapshot_->getString("mqtt.username", "");

    // Read default broker values from framework-config (may be overridden by store)
    current_config_.broker_host = config_snapshot_->getString("mqtt.broker_host", "localhost");
    current_config_.broker_port = static_cast<uint16_t>(config_snapshot_->getInt("mqtt.broker_port", 1883));

    current_config_.source = ConnectionConfig::Source::FACTORY;
    current_config_.version = 0;

    // Read offline queue config
    offline_queue_config_ = readOfflineQueueConfig();

    MqttLogAdapter::ipc().info(
        "mqtt.config.loaded",
        "配置已从 framework-config 加载",
        {
            {"config_root", tbox::fw::log::FieldValue::makeString(config_root)},
            {"broker_hash", tbox::fw::log::FieldValue::makeString(brokerHash(current_config_.broker_host))},
            {"queue_max_size", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(offline_queue_config_.max_size))}
        }
    );
    return true;
}

bool ConfigManager::loadFromStore(hwyz::store::Store& store) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!store.isReady()) {
        MqttLogAdapter::ipc().warn(
            "mqtt.store.not_ready",
            "framework-store 未就绪，跳过 store 加载"
        );
        return false;
    }

    // Load OTA-updatable params from framework-store
    if (store.has(StoreKeys::kBrokerHost)) {
        current_config_.broker_host = store.loadOr<std::string>(StoreKeys::kBrokerHost, current_config_.broker_host);
        current_config_.source = ConnectionConfig::Source::OTA;
    }

    if (store.has(StoreKeys::kBrokerPort)) {
        int port = store.loadOr<int>(StoreKeys::kBrokerPort, static_cast<int>(current_config_.broker_port));
        current_config_.broker_port = static_cast<uint16_t>(port);
        current_config_.source = ConnectionConfig::Source::OTA;
    }

    if (store.has(StoreKeys::kConfigVersion)) {
        current_config_.version = static_cast<uint32_t>(store.loadOr<int>(StoreKeys::kConfigVersion, 0));
        current_version_ = current_config_.version;
    }

    MqttLogAdapter::ipc().info(
        "mqtt.store.loaded",
        "OTA 参数已从 framework-store 加载",
        {
            {"source", tbox::fw::log::FieldValue::makeString(
                current_config_.source == ConnectionConfig::Source::OTA ? "OTA" : "FACTORY")},
            {"version", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(current_version_))}
        }
    );
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

        // OTA source: write updatable params to framework-store (Design §5)
        if (source == ConnectionConfig::Source::OTA && store_ && store_->isReady()) {
            try {
                store_->save<std::string>(StoreKeys::kBrokerHost, config.broker_host);
                store_->save<int>(StoreKeys::kBrokerPort, static_cast<int>(config.broker_port));
                store_->save<int>(StoreKeys::kConfigVersion, static_cast<int>(config.version));
            } catch (const hwyz::store::StoreException& e) {
                MqttLogAdapter::ipc().error(
                    "mqtt.store.write_failed",
                    "framework-store 写入失败",
                    {{"reason", tbox::fw::log::FieldValue::makeString(e.what())}}
                );
                return false;
            }
        }

        // Update in-memory config (updatable fields only for OTA)
        current_config_.broker_host = config.broker_host;
        current_config_.broker_port = config.broker_port;
        current_config_.source = source;
        current_config_.version = config.version;
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

void ConfigManager::setStore(std::shared_ptr<hwyz::store::Store> store) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_ = store;
}

std::shared_ptr<const hwyz::config::ImmutableConfigView> ConfigManager::getSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_snapshot_;
}

OfflineQueueConfig ConfigManager::getOfflineQueueConfig() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return offline_queue_config_;
}

void ConfigManager::notifyChange() {
    if (change_callback_) {
        change_callback_();
    }
}

OfflineQueueConfig ConfigManager::readOfflineQueueConfig() const {
    OfflineQueueConfig config;
    if (!config_snapshot_) {
        return config;
    }

    auto queue_section = config_snapshot_->getSection("mqtt.offline_queue");
    if (queue_section) {
        config.path = queue_section->getString("path", config.path);
        config.max_size = static_cast<size_t>(queue_section->getInt("max_size", static_cast<int>(config.max_size)));
        config.drop_policy = queue_section->getString("drop_policy", config.drop_policy);
    }

    return config;
}

} // namespace mqtt
} // namespace tbox
