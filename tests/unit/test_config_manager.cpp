#include <gtest/gtest.h>
#include "config_manager.h"
#include "store.h"
#include <fstream>
#include <filesystem>
#include <cstdio>

using namespace tbox::mqtt;

namespace {
// Helper: create a temp config directory with common.yaml and mqtt.yaml
std::string createTempConfigDir(const std::string& broker_host = "test.mqtt.com",
                                 int broker_port = 1883,
                                 const std::string& client_id_format = "{vin}-{sn}") {
    auto tmp_dir = std::filesystem::temp_directory_path() / "tbox_mqtt_test_config";
    auto conf_d = tmp_dir / "conf.d";
    std::filesystem::create_directories(conf_d);

    // common.yaml
    {
        std::ofstream ofs(tmp_dir / "common.yaml");
        ofs << "common:\n"
            << "  store:\n"
            << "    root: /tmp/tbox_mqtt_test_store\n"
            << "  log:\n"
            << "    level: debug\n"
            << "bus:\n"
            << "  address: /tmp/tbox_test_bus.sock\n";
    }

    // conf.d/mqtt.yaml
    {
        std::ofstream ofs(conf_d / "mqtt.yaml");
        ofs << "mqtt:\n"
            << "  broker_host: " << broker_host << "\n"
            << "  broker_port: " << broker_port << "\n"
            << "  identity:\n"
            << "    client_id_format: \"" << client_id_format << "\"\n"
            << "  keepalive: 30\n"
            << "  clean_start: true\n"
            << "  session_expiry: 7200\n"
            << "  tls:\n"
            << "    credential_profile: mqtt\n"
            << "    min_version: \"1.2\"\n"
            << "    max_version: \"1.3\"\n"
            << "    handshake_timeout_ms: 10000\n"
            << "  offline_queue:\n"
            << "    path: /tmp/tbox_mqtt_test_queue\n"
            << "    max_size: 500\n"
            << "    drop_policy: drop_lowest_priority\n";
    }

    return tmp_dir.string();
}

void cleanupTempConfigDir(const std::string& path) {
    std::filesystem::remove_all(path);
}

std::string createTempStoreDir() {
    auto store_dir = std::filesystem::temp_directory_path() / "tbox_mqtt_test_store";
    std::filesystem::create_directories(store_dir);
    return store_dir.string();
}

void cleanupTempStoreDir(const std::string& path) {
    std::filesystem::remove_all(path);
}
} // anonymous namespace

TEST(ConfigManagerTest, LoadFromFrameworkConfig) {
    std::string config_dir = createTempConfigDir();
    ConfigManager config;

    ASSERT_TRUE(config.loadFromFramework(config_dir));

    auto cfg = config.getConfig();
    EXPECT_EQ(cfg.broker_host, "test.mqtt.com");
    EXPECT_EQ(cfg.broker_port, 1883);
    EXPECT_EQ(cfg.client_id_format, "{vin}-{sn}");
    EXPECT_EQ(cfg.keepalive, 30);
    EXPECT_TRUE(cfg.clean_start);
    EXPECT_EQ(cfg.session_expiry, 7200);
    // CR-005: 新配置字段
    EXPECT_EQ(cfg.credential_profile, "mqtt");
    EXPECT_EQ(cfg.min_tls_version, TlsVersion::TLS_1_2);
    EXPECT_EQ(cfg.max_tls_version, TlsVersion::TLS_1_3);
    EXPECT_EQ(cfg.handshake_timeout.count(), 10000);

    cleanupTempConfigDir(config_dir);
}

TEST(ConfigManagerTest, LoadFromFrameworkConfigInvalidPath) {
    ConfigManager config;
    EXPECT_FALSE(config.loadFromFramework("/nonexistent/path/"));
}

TEST(ConfigManagerTest, GetOfflineQueueConfig) {
    std::string config_dir = createTempConfigDir();
    ConfigManager config;

    ASSERT_TRUE(config.loadFromFramework(config_dir));

    auto queue_config = config.getOfflineQueueConfig();
    EXPECT_EQ(queue_config.path, "/tmp/tbox_mqtt_test_queue");
    EXPECT_EQ(queue_config.max_size, 500);
    EXPECT_EQ(queue_config.drop_policy, "drop_lowest_priority");

    cleanupTempConfigDir(config_dir);
}

TEST(ConfigManagerTest, ApplyFactoryConfig) {
    std::string config_dir = createTempConfigDir();
    ConfigManager config;
    ASSERT_TRUE(config.loadFromFramework(config_dir));

    ConnectionConfig factory_config;
    factory_config.broker_host = "factory.mqtt.com";
    factory_config.broker_port = 8883;
    factory_config.client_id_format = "{vin}-{sn}";
    factory_config.credential_profile = "mqtt";
    factory_config.source = ConnectionConfig::Source::FACTORY;
    factory_config.version = 1;

    EXPECT_TRUE(config.applyConfig(factory_config, ConnectionConfig::Source::FACTORY));

    auto current = config.getConfig();
    EXPECT_EQ(current.broker_host, "factory.mqtt.com");

    cleanupTempConfigDir(config_dir);
}

TEST(ConfigManagerTest, ApplyOtaConfigWithStore) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    ConfigManager config;
    ASSERT_TRUE(config.loadFromFramework(config_dir));

    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    config.setStore(store);

    ConnectionConfig ota_config;
    ota_config.broker_host = "ota.mqtt.com";
    ota_config.broker_port = 8883;
    ota_config.client_id_format = "{vin}-{sn}";
    ota_config.credential_profile = "mqtt";
    ota_config.source = ConnectionConfig::Source::OTA;
    ota_config.version = 2;

    EXPECT_TRUE(config.applyConfig(ota_config, ConnectionConfig::Source::OTA));

    auto current = config.getConfig();
    EXPECT_EQ(current.broker_host, "ota.mqtt.com");
    EXPECT_EQ(current.broker_port, 8883);
    EXPECT_EQ(current.version, 2);

    cleanupTempConfigDir(config_dir);
    cleanupTempStoreDir(store_dir);
}

TEST(ConfigManagerTest, LoadFromStoreAfterOTA) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    // First: load framework config and apply OTA config to store
    {
        ConfigManager config;
        ASSERT_TRUE(config.loadFromFramework(config_dir));

        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        config.setStore(store);

        ConnectionConfig ota_config;
        ota_config.broker_host = "ota-loaded.mqtt.com";
        ota_config.broker_port = 9999;
        ota_config.client_id_format = "{vin}-{sn}";
        ota_config.credential_profile = "mqtt";
        ota_config.source = ConnectionConfig::Source::OTA;
        ota_config.version = 5;

        ASSERT_TRUE(config.applyConfig(ota_config, ConnectionConfig::Source::OTA));
    }

    // Second: load framework config then load from store
    {
        ConfigManager config;
        ASSERT_TRUE(config.loadFromFramework(config_dir));

        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ASSERT_TRUE(config.loadFromStore(*store));

        auto cfg = config.getConfig();
        EXPECT_EQ(cfg.broker_host, "ota-loaded.mqtt.com");
        EXPECT_EQ(cfg.broker_port, 9999);
        EXPECT_EQ(cfg.source, ConnectionConfig::Source::OTA);
        EXPECT_EQ(cfg.version, 5);
    }

    cleanupTempConfigDir(config_dir);
    cleanupTempStoreDir(store_dir);
}

TEST(ConfigManagerTest, ConfigValidation) {
    ConfigManager config;

    ConnectionConfig invalid_config;
    invalid_config.broker_host = "";
    invalid_config.broker_port = 0;

    EXPECT_FALSE(config.validateConfig(invalid_config));
}

TEST(ConfigManagerTest, ConfigChangeCallback) {
    std::string config_dir = createTempConfigDir();
    ConfigManager config;
    ASSERT_TRUE(config.loadFromFramework(config_dir));

    bool callback_called = false;
    config.setChangeCallback([&callback_called]() {
        callback_called = true;
    });

    ConnectionConfig new_config;
    new_config.broker_host = "new.mqtt.com";
    new_config.client_id_format = "{vin}-{sn}";
    new_config.credential_profile = "mqtt";
    config.applyConfig(new_config, ConnectionConfig::Source::FACTORY);

    EXPECT_TRUE(callback_called);

    cleanupTempConfigDir(config_dir);
}

TEST(ConfigManagerTest, OtaConfigVersionCheck) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    ConfigManager config;
    ASSERT_TRUE(config.loadFromFramework(config_dir));

    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    config.setStore(store);

    // Apply version 2
    ConnectionConfig v2_config;
    v2_config.broker_host = "v2.mqtt.com";
    v2_config.broker_port = 8883;
    v2_config.client_id_format = "{vin}-{sn}";
    v2_config.credential_profile = "mqtt";
    v2_config.version = 2;
    ASSERT_TRUE(config.applyConfig(v2_config, ConnectionConfig::Source::OTA));

    // Try to apply version 1 (should fail - older version)
    ConnectionConfig v1_config;
    v1_config.broker_host = "v1.mqtt.com";
    v1_config.broker_port = 1883;
    v1_config.client_id_format = "{vin}-{sn}";
    v1_config.credential_profile = "mqtt";
    v1_config.version = 1;
    EXPECT_FALSE(config.applyConfig(v1_config, ConnectionConfig::Source::OTA));

    // Current should still be v2
    auto current = config.getConfig();
    EXPECT_EQ(current.broker_host, "v2.mqtt.com");

    cleanupTempConfigDir(config_dir);
    cleanupTempStoreDir(store_dir);
}
