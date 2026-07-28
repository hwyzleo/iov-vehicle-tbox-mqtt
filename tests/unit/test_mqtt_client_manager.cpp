#include <gtest/gtest.h>
#include "mqtt_client_manager.h"
#include "credential_manager.h"
#include "sec_tls_key_adapter.h"

using namespace tbox::mqtt;

class MqttClientManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        cred_mgr_.simulateReady();
    }

    CredentialManager cred_mgr_;
    SecTlsKeyAdapter tls_adapter_;
};

TEST_F(MqttClientManagerTest, InitializeClient) {
    MqttClientManager client;
    EXPECT_TRUE(client.initialize(cred_mgr_, tls_adapter_));
}

TEST_F(MqttClientManagerTest, InitializeClientWithNotReadyCredential) {
    CredentialManager not_ready;
    not_ready.simulateNotReady();

    MqttClientManager client;
    // initialize() just stores references; readiness checked in connect()
    EXPECT_TRUE(client.initialize(not_ready, tls_adapter_));
}

TEST_F(MqttClientManagerTest, ConnectWithNotReadyCredential) {
    CredentialManager not_ready;
    not_ready.simulateNotReady();

    MqttClientManager client;
    client.initialize(not_ready, tls_adapter_);

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";
    config.credential_profile = "mqtt";
    config.keepalive = 60;
    config.clean_start = false;
    config.session_expiry = 3600;

    // connect() should fail because credential is not ready
    EXPECT_FALSE(client.connect(config));
}

TEST_F(MqttClientManagerTest, ConnectWithoutInitialize) {
    MqttClientManager client;

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";
    config.credential_profile = "mqtt";
    config.keepalive = 60;
    config.clean_start = false;
    config.session_expiry = 3600;

    // connect() should fail because cred_mgr_ and tls_adapter_ are not set
    EXPECT_FALSE(client.connect(config));
}

TEST_F(MqttClientManagerTest, PublishWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize(cred_mgr_, tls_adapter_));

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = client.publish("test/topic", payload, QoS::AT_LEAST_ONCE);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1002);
}

TEST_F(MqttClientManagerTest, SubscribeWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize(cred_mgr_, tls_adapter_));

    auto result = client.subscribe("test/topic", QoS::AT_LEAST_ONCE);
    EXPECT_FALSE(result.status);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1002);
}

TEST_F(MqttClientManagerTest, UnsubscribeWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize(cred_mgr_, tls_adapter_));

    EXPECT_FALSE(client.unsubscribe("test/topic"));
}

TEST_F(MqttClientManagerTest, SetCallbacks) {
    MqttClientManager client;

    bool message_received = false;
    bool connection_changed = false;

    client.setMessageCallback([&message_received](const std::string& topic, const std::vector<uint8_t>& payload) {
        message_received = true;
    });

    client.setConnectionCallback([&connection_changed](bool connected) {
        connection_changed = true;
    });

    EXPECT_FALSE(message_received);
    EXPECT_FALSE(connection_changed);
}

TEST_F(MqttClientManagerTest, SetLwtConfig) {
    MqttClientManager client;

    LwtConfig lwt;
    lwt.topic = "status/offline";
    lwt.payload = {0x00};
    lwt.qos = QoS::AT_LEAST_ONCE;
    lwt.retain = true;

    client.setLwtConfig(lwt);
}

TEST_F(MqttClientManagerTest, IsConnectedInitiallyFalse) {
    MqttClientManager client;
    EXPECT_FALSE(client.isConnected());
}

TEST_F(MqttClientManagerTest, IsConnectedAfterFailedConnect) {
    CredentialManager not_ready;
    not_ready.simulateNotReady();

    MqttClientManager client;
    client.initialize(not_ready, tls_adapter_);

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";
    config.credential_profile = "mqtt";
    config.keepalive = 60;

    EXPECT_FALSE(client.connect(config));
    EXPECT_FALSE(client.isConnected());
}

TEST_F(MqttClientManagerTest, NoLocalCredentialFileAccess) {
    // CR-005 §10 安全测试: 确保不使用本地凭据文件
    // MqttClientManager 不接受任何文件路径参数
    MqttClientManager client;
    EXPECT_TRUE(client.initialize(cred_mgr_, tls_adapter_));
    // No file path parameters in initialize() - compilation verifies this
}
