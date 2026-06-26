#include <gtest/gtest.h>
#include "config_manager.h"
#include <fstream>
#include <cstdio>

using namespace tbox::mqtt;

TEST(ConfigManagerTest, LoadDefaultConfig) {
    ConfigManager config;
    EXPECT_TRUE(config.loadDefaultConfig());

    auto config_data = config.getConfig();
    EXPECT_FALSE(config_data.broker_host.empty());
}

TEST(ConfigManagerTest, ApplyFactoryConfig) {
    ConfigManager config;

    ConnectionConfig factory_config;
    factory_config.broker_host = "factory.mqtt.com";
    factory_config.broker_port = 8883;
    factory_config.client_id = "VIN123SN456";
    factory_config.source = ConnectionConfig::Source::FACTORY;
    factory_config.version = 1;

    EXPECT_TRUE(config.applyConfig(factory_config, ConnectionConfig::Source::FACTORY));

    auto current = config.getConfig();
    EXPECT_EQ(current.broker_host, "factory.mqtt.com");
}

TEST(ConfigManagerTest, ApplyOtaConfig) {
    ConfigManager config;

    ConnectionConfig ota_config;
    ota_config.broker_host = "ota.mqtt.com";
    ota_config.broker_port = 8883;
    ota_config.client_id = "VIN123SN456";
    ota_config.source = ConnectionConfig::Source::OTA;
    ota_config.version = 2;

    EXPECT_TRUE(config.applyConfig(ota_config, ConnectionConfig::Source::OTA));

    auto current = config.getConfig();
    EXPECT_EQ(current.broker_host, "ota.mqtt.com");
}

TEST(ConfigManagerTest, ConfigVersionTracking) {
    ConfigManager config;

    ConnectionConfig config1;
    config1.broker_host = "host1.mqtt.com";
    config1.client_id = "VIN123SN456";
    config1.version = 1;
    config.applyConfig(config1, ConnectionConfig::Source::FACTORY);

    ConnectionConfig config2;
    config2.broker_host = "host2.mqtt.com";
    config2.client_id = "VIN123SN456";
    config2.version = 2;
    config.applyConfig(config2, ConnectionConfig::Source::OTA);

    EXPECT_EQ(config.getCurrentVersion(), 2);
}

TEST(ConfigManagerTest, ConfigValidation) {
    ConfigManager config;

    ConnectionConfig invalid_config;
    invalid_config.broker_host = "";  // Invalid: empty host
    invalid_config.broker_port = 0;   // Invalid: port 0

    EXPECT_FALSE(config.validateConfig(invalid_config));
}

TEST(ConfigManagerTest, ConfigChangeCallback) {
    ConfigManager config;

    bool callback_called = false;
    config.setChangeCallback([&callback_called]() {
        callback_called = true;
    });

    ConnectionConfig new_config;
    new_config.broker_host = "new.mqtt.com";
    new_config.client_id = "VIN123SN456";
    config.applyConfig(new_config, ConnectionConfig::Source::FACTORY);

    EXPECT_TRUE(callback_called);
}

TEST(ConfigManagerTest, SaveAndLoadFromFile) {
    const std::string test_file = "/tmp/test_mqtt_config.json";

    // Save config
    ConfigManager saver;
    ConnectionConfig save_config;
    save_config.broker_host = "test.mqtt.com";
    save_config.broker_port = 1883;
    save_config.client_id = "TEST123";
    save_config.keepalive = 30;
    save_config.clean_start = true;
    save_config.session_expiry = 7200;
    save_config.root_ca_ref = "/path/to/ca.crt";
    save_config.tls_cert_ref = "/path/to/client.crt";
    saver.applyConfig(save_config, ConnectionConfig::Source::FACTORY);

    ASSERT_TRUE(saver.saveToFile(test_file));

    // Load config
    ConfigManager loader;
    ASSERT_TRUE(loader.loadFromFile(test_file));

    auto loaded = loader.getConfig();
    EXPECT_EQ(loaded.broker_host, "test.mqtt.com");
    EXPECT_EQ(loaded.broker_port, 1883);
    EXPECT_EQ(loaded.client_id, "TEST123");
    EXPECT_EQ(loaded.keepalive, 30);
    EXPECT_EQ(loaded.clean_start, true);
    EXPECT_EQ(loaded.session_expiry, 7200);
    EXPECT_EQ(loaded.root_ca_ref, "/path/to/ca.crt");
    EXPECT_EQ(loaded.tls_cert_ref, "/path/to/client.crt");

    std::remove(test_file.c_str());
}

TEST(ConfigManagerTest, LoadFromFileNotFound) {
    ConfigManager config;
    EXPECT_FALSE(config.loadFromFile("/nonexistent/path/config.json"));
}

TEST(ConfigManagerTest, LoadFromFileInvalidJson) {
    const std::string test_file = "/tmp/test_invalid_config.json";
    {
        std::ofstream ofs(test_file);
        ofs << "{ invalid json }";
    }

    ConfigManager config;
    EXPECT_FALSE(config.loadFromFile(test_file));

    std::remove(test_file.c_str());
}

TEST(ConfigManagerTest, LoadFromFileInvalidConfig) {
    const std::string test_file = "/tmp/test_invalid_mqtt_config.json";
    {
        std::ofstream ofs(test_file);
        ofs << R"({"broker_host": "", "broker_port": 0})";
    }

    ConfigManager config;
    EXPECT_FALSE(config.loadFromFile(test_file));

    std::remove(test_file.c_str());
}
