#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "sec_cert_interface.h"
#include "internal_bus_interface.h"

using namespace tbox::mqtt;

TEST(MqttServiceIntegrationTest, ServiceInitialization) {
    MqttService service;

    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    sec->simulateCertificateReady();

    EXPECT_TRUE(service.initialize(sec, bus));
}

TEST(MqttServiceIntegrationTest, ServiceStartStop) {
    MqttService service;

    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    sec->simulateCertificateReady();

    service.initialize(sec, bus);
    EXPECT_TRUE(service.start());

    service.stop();
}

TEST(MqttServiceIntegrationTest, PublishMessage) {
    MqttService service;

    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    sec->simulateCertificateReady();

    service.initialize(sec, bus);
    service.start();

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = service.publish("test/topic", payload, QoS::AT_LEAST_ONCE);
    EXPECT_TRUE(result.accepted);

    service.stop();
}

TEST(MqttServiceIntegrationTest, SubscribeToTopic) {
    MqttService service;

    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    sec->simulateCertificateReady();

    service.initialize(sec, bus);
    service.start();

    auto result = service.subscribe("test/topic", QoS::AT_LEAST_ONCE, "service_diag");
    EXPECT_TRUE(result.status);

    service.stop();
}

TEST(MqttServiceIntegrationTest, RegisterRoute) {
    MqttService service;

    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    service.initialize(sec, bus);

    EXPECT_TRUE(service.registerRoute("service_diag", "vehicle/diag/status",
                                     RouteEntry::Direction::UP));
}

TEST(MqttServiceIntegrationTest, GetConnectionState) {
    MqttService service;

    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    service.initialize(sec, bus);

    auto state = service.getConnectionState();
    EXPECT_EQ(state.state, ConnectionState::INIT);
}

TEST(MqttServiceIntegrationTest, ApplyConfiguration) {
    MqttService service;

    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    service.initialize(sec, bus);

    ConnectionConfig config;
    config.broker_host = "test.mqtt.com";
    config.broker_port = 8883;
    config.client_id = "VIN123SN456";

    EXPECT_TRUE(service.applyConfig(config, ConnectionConfig::Source::FACTORY));
}
