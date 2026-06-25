#include <gtest/gtest.h>
#include "mqtt_client_manager.h"
#include "sec_cert_interface.h"
#include <fstream>
#include <filesystem>

using namespace tbox::mqtt;

class MqttClientManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary directory for test files
        temp_dir_ = std::filesystem::temp_directory_path() / "mqtt_test";
        std::filesystem::create_directories(temp_dir_);
        
        // Create a temporary CA file for testing
        ca_file_ = temp_dir_ / "test_ca.crt";
        std::ofstream ca_file(ca_file_);
        ca_file << "-----BEGIN CERTIFICATE-----\n";
        ca_file << "MIIBkTCB+wIJAL...\n";  // Dummy certificate content
        ca_file << "-----END CERTIFICATE-----\n";
        ca_file.close();
        
        // Initialize SEC interface
        sec_.simulateCertificateReady();
    }

    void TearDown() override {
        // Clean up temporary files
        std::filesystem::remove_all(temp_dir_);
    }

    std::filesystem::path temp_dir_;
    std::filesystem::path ca_file_;
    SecCertInterface sec_;
};

TEST_F(MqttClientManagerTest, InitializeClient) {
    MqttClientManager client;
    // TLS configuration should succeed with valid CA file
    EXPECT_TRUE(client.initialize("test.mosquitto.org", 8883, "test_client", sec_, ca_file_.string()));
}

TEST_F(MqttClientManagerTest, InitializeClientWithInvalidCAFile) {
    MqttClientManager client;
    // TLS configuration should fail with invalid CA file path
    EXPECT_FALSE(client.initialize("test.mosquitto.org", 8883, "test_client", sec_, "/nonexistent/path/ca.crt"));
}

TEST_F(MqttClientManagerTest, InitializeClientWithInvalidCert) {
    SecCertInterface invalid_sec;
    // Don't call simulateCertificateReady() - certificate not ready
    
    MqttClientManager client;
    EXPECT_FALSE(client.initialize("test.mosquitto.org", 8883, "test_client", invalid_sec, ca_file_.string()));
}

TEST_F(MqttClientManagerTest, ConnectWithoutInitialize) {
    MqttClientManager client;
    
    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";

    // Should fail because client is not initialized
    EXPECT_FALSE(client.connect(config));
}

TEST_F(MqttClientManagerTest, PublishWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize("test.mosquitto.org", 8883, "test_client", sec_, ca_file_.string()));

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = client.publish("test/topic", payload, QoS::AT_LEAST_ONCE);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1002);
}

TEST_F(MqttClientManagerTest, SubscribeWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize("test.mosquitto.org", 8883, "test_client", sec_, ca_file_.string()));

    auto result = client.subscribe("test/topic", QoS::AT_LEAST_ONCE);
    EXPECT_FALSE(result.status);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1002);
}

TEST_F(MqttClientManagerTest, UnsubscribeWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize("test.mosquitto.org", 8883, "test_client", sec_, ca_file_.string()));

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
    
    // Callbacks should be set (we can't easily test they're called without a real broker)
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
    // LWT config should be set (we can't easily test it's used without a real broker)
}

TEST_F(MqttClientManagerTest, IsConnectedInitiallyFalse) {
    MqttClientManager client;
    EXPECT_FALSE(client.isConnected());
}

TEST_F(MqttClientManagerTest, IsConnectedAfterFailedInitialize) {
    MqttClientManager client;
    // Initialize with invalid CA file should fail
    EXPECT_FALSE(client.initialize("test.mosquitto.org", 8883, "test_client", sec_, "/nonexistent/path/ca.crt"));
    EXPECT_FALSE(client.isConnected());
}
