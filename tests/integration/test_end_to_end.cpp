#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "sec_cert_interface.h"
#include "internal_bus_interface.h"
#include <thread>
#include <chrono>

using namespace tbox::mqtt;

class EndToEndTest : public ::testing::Test {
protected:
    void SetUp() override {
        service = std::make_unique<MqttService>();
        sec = std::make_shared<SecCertInterface>();
        bus = std::make_shared<InternalBusInterface>();

        sec->simulateCertificateReady();

        service->initialize(sec, bus);
        service->start();

        // Wait for connection
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    void TearDown() override {
        service->stop();
    }

    std::unique_ptr<MqttService> service;
    std::shared_ptr<SecCertInterface> sec;
    std::shared_ptr<InternalBusInterface> bus;
};

TEST_F(EndToEndTest, PublishAndSubscribe) {
    // Subscribe to topic (this registers a downstream route internally)
    auto sub_result = service->subscribe("vehicle/diag/status",
                                        QoS::AT_LEAST_ONCE,
                                        "service_diag");
    EXPECT_TRUE(sub_result.status);

    // Publish message
    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto pub_result = service->publish("vehicle/diag/status",
                                      payload,
                                      QoS::AT_LEAST_ONCE);
    EXPECT_TRUE(pub_result.accepted);
}

TEST_F(EndToEndTest, OfflineCaching) {
    // Stop service to simulate offline
    service->stop();

    // Publish message while offline
    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = service->publish("vehicle/diag/status",
                                  payload,
                                  QoS::AT_LEAST_ONCE);
    EXPECT_TRUE(result.accepted);

    // Restart service
    service->start();
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Message should be delivered
}

TEST_F(EndToEndTest, MessageRouting) {
    // Register upstream route
    service->registerRoute("service_diag", "vehicle/diag/status",
                          RouteEntry::Direction::UP);

    // Register downstream route
    service->registerRoute("service_control", "vehicle/control/cmd",
                          RouteEntry::Direction::DOWN);

    // Subscribe to downstream
    service->subscribe("vehicle/control/cmd",
                      QoS::AT_LEAST_ONCE,
                      "service_control");

    // Simulate upstream message
    std::vector<uint8_t> upstream_payload = {0x01, 0x02, 0x03};
    service->publish("vehicle/diag/status",
                    upstream_payload,
                    QoS::AT_LEAST_ONCE);
}

TEST_F(EndToEndTest, ConnectionStateMonitoring) {
    auto state = service->getConnectionState();
    // Service is in CONNECTING state since no real MQTT broker is available
    EXPECT_EQ(state.state, ConnectionState::CONNECTING);
}

TEST_F(EndToEndTest, ConfigurationUpdate) {
    ConnectionConfig new_config;
    new_config.broker_host = "new.mqtt.com";
    new_config.broker_port = 8883;
    new_config.client_id = "VIN123SN456";
    new_config.version = 2;

    EXPECT_TRUE(service->applyConfig(new_config, ConnectionConfig::Source::OTA));
}
