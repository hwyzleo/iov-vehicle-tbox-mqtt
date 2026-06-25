#include <gtest/gtest.h>
#include "config_manager.h"

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
