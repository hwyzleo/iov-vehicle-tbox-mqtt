#include <gtest/gtest.h>
#include "mqtt_client_manager.h"
#include "sec_cert_interface.h"
#include <fstream>
#include <filesystem>

using namespace tbox::mqtt;

class MqttClientManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create unique temporary directory per test to avoid race conditions
        static int test_counter = 0;
        temp_dir_ = std::filesystem::temp_directory_path() / ("mqtt_test_" + std::to_string(++test_counter) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(temp_dir_);
        
        // Create a temporary CA file for testing with proper PEM format
        ca_file_ = temp_dir_ / "test_ca.crt";
        std::ofstream ca_file(ca_file_);
        ca_file << "-----BEGIN CERTIFICATE-----\n";
        ca_file << "MIICpDCCAYwCCQDU+pQ4pHgSpDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls\n";
        ca_file << "b2NhbGhvc3QwHhcNMjMwMTAxMDAwMDAwWhcNMjQwMTAxMDAwMDAwWjAUMRIwEAYD\n";
        ca_file << "VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC7\n";
        ca_file << "o3zdQHiaj0LqjqFGKA1R8vDPHGBj+JFnBR7MKb0VDlJPGz9BJWRIrEjEYF3bL8S3\n";
        ca_file << "Kx0JEVGjjHKCOQ9Xy3hxv+cJMnqFK3+kfVnM3F5M5dJqM8SqS3F03ldwEJH3N0Rl\n";
        ca_file << "Xq2L0i8s1GmXA1WBXYkCgYEA0YkPNzfGA6Fj0HFIk0FT8mKqFJPMD+6iJGCE3fUJ\n";
        ca_file << "Xp2JfEd8vB2lQ3jT0+j8V3nLN3MJYVSEi+z3MO6aF0PX3PChH0yK3L1/F6v1O0jD\n";
        ca_file << "L0g/In+4n9+L5OJ8k0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I\n";
        ca_file << "8w0CAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAp3VDYJ4O8x5a5JvF3w3F5d6h7g8J\n";
        ca_file << "9k0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5\n";
        ca_file << "O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I\n";
        ca_file << "8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0S5O8v3I8w0=\n";
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
    config.keepalive = 60;
    config.clean_start = false;
    config.session_expiry = 3600;

    // connect() now auto-initializes the mosquitto instance
    EXPECT_TRUE(client.connect(config));
    client.disconnect();
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
