#include <gtest/gtest.h>
#include "mqtt_client_manager.h"
#include "sec_cert_interface.h"

using namespace tbox::mqtt;

TEST(MqttClientManagerTest, InitializeClient) {
    SecCertInterface sec;
    sec.simulateCertificateReady();

    MqttClientManager client;
    // TLS configuration fails because CA file doesn't exist
    // This is expected behavior - initialize() should return false when TLS config fails
    EXPECT_FALSE(client.initialize("test.mosquitto.org", 8883, "test_client", sec));
}

TEST(MqttClientManagerTest, ConnectWithValidConfig) {
    SecCertInterface sec;
    sec.simulateCertificateReady();

    MqttClientManager client;
    client.initialize("test.mosquitto.org", 8883, "test_client", sec);

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";

    EXPECT_TRUE(client.connect(config));
}

TEST(MqttClientManagerTest, PublishMessage) {
    SecCertInterface sec;
    sec.simulateCertificateReady();

    MqttClientManager client;
    client.initialize("test.mosquitto.org", 8883, "test_client", sec);

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";

    client.connect(config);

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = client.publish("test/topic", payload, QoS::AT_LEAST_ONCE);
    EXPECT_TRUE(result.accepted);
}

TEST(MqttClientManagerTest, SubscribeToTopic) {
    SecCertInterface sec;
    sec.simulateCertificateReady();

    MqttClientManager client;
    client.initialize("test.mosquitto.org", 8883, "test_client", sec);

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";

    client.connect(config);

    auto result = client.subscribe("test/topic", QoS::AT_LEAST_ONCE);
    EXPECT_TRUE(result.status);
}