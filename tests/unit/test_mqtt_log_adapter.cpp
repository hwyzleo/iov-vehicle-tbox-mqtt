#include <gtest/gtest.h>
#include "mqtt_log_adapter.h"
#include "log_types.h"

using namespace tbox::mqtt;
using namespace tbox::fw::log;

class MqttLogAdapterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset state before each test
    }

    void TearDown() override {
        // Cleanup after each test
    }
};

TEST_F(MqttLogAdapterTest, InitSuccess) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;

    auto result = MqttLogAdapter::init("mqtt_test", config);
    EXPECT_EQ(result.error, LogError::kOk);
}

TEST_F(MqttLogAdapterTest, GetConnectionLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::connection();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, GetTlsLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::tls();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, GetPublishLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::publish();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, GetSubscribeLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::subscribe();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, GetOfflineQueueLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::offline_queue();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, GetRouterLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::router();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, GetIpcLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::ipc();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, GetServiceLogger) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    auto logger = MqttLogAdapter::service();
    EXPECT_NO_THROW(logger.info("test.event", "test message"));
}

TEST_F(MqttLogAdapterTest, AllModuleLoggersCallable) {
    LogConfig config;
    config.level = LogLevel::kDebug;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    // All 8 module loggers should be callable without throwing
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info("test", "msg");
        MqttLogAdapter::tls().info("test", "msg");
        MqttLogAdapter::publish().info("test", "msg");
        MqttLogAdapter::subscribe().info("test", "msg");
        MqttLogAdapter::offline_queue().info("test", "msg");
        MqttLogAdapter::router().info("test", "msg");
        MqttLogAdapter::ipc().info("test", "msg");
        MqttLogAdapter::service().info("test", "msg");
    });
}
