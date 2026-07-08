#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "sec_cert_interface.h"
#include "internal_bus_interface.h"
#include "config.h"
#include <filesystem>
#include <fstream>

using namespace tbox::mqtt;

namespace {
std::string createTempConfigDir() {
    auto tmp_dir = std::filesystem::temp_directory_path() / "tbox_mqtt_integration_config";
    auto conf_d = tmp_dir / "conf.d";
    std::filesystem::create_directories(conf_d);

    {
        std::ofstream ofs(tmp_dir / "common.yaml");
        ofs << "store:\n"
            << "  root: /tmp/tbox_mqtt_integration_store\n"
            << "bus:\n"
            << "  address: /tmp/tbox_test_bus.sock\n"
            << "log:\n"
            << "  level: info\n";
    }

    {
        std::ofstream ofs(conf_d / "mqtt.yaml");
        ofs << "mqtt:\n"
            << "  broker_host: localhost\n"
            << "  broker_port: 1883\n"
            << "  client_id: TEST_INTEGRATION\n"
            << "  keepalive: 60\n"
            << "  clean_start: false\n"
            << "  session_expiry: 3600\n"
            << "  root_ca_ref: /path/to/ca.crt\n"
            << "  tls_cert_ref: /path/to/client.crt\n"
            << "  tls_key_ref: /path/to/client.key\n"
            << "  key_source: file\n"
            << "  offline_queue:\n"
            << "    max_size: 100\n"
            << "    drop_policy: drop_oldest\n";
    }

    return tmp_dir.string();
}

std::string createTempStoreDir() {
    auto dir = std::filesystem::temp_directory_path() / "tbox_mqtt_integration_store";
    std::filesystem::create_directories(dir);
    return dir.string();
}

void cleanupTempDirs() {
    std::filesystem::remove_all(std::filesystem::temp_directory_path() / "tbox_mqtt_integration_config");
    std::filesystem::remove_all(std::filesystem::temp_directory_path() / "tbox_mqtt_integration_store");
}
} // anonymous namespace

TEST(MqttServiceIntegrationTest, ServiceInitialization) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();
    sec->simulateCertificateReady();

    EXPECT_TRUE(service.initialize(sec, bus, config_dir, store_dir));

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ServiceStartStop) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();
    sec->simulateCertificateReady();

    service.initialize(sec, bus, config_dir, store_dir);
    EXPECT_TRUE(service.start());

    service.stop();

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, PublishMessage) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();
    sec->simulateCertificateReady();

    service.initialize(sec, bus, config_dir, store_dir);
    service.start();

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = service.publish("test/topic", payload, QoS::AT_LEAST_ONCE);
    EXPECT_TRUE(result.accepted);

    service.stop();

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, SubscribeToTopic) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();
    sec->simulateCertificateReady();

    service.initialize(sec, bus, config_dir, store_dir);
    service.start();

    auto result = service.subscribe("test/topic", QoS::AT_LEAST_ONCE, "service_diag");
    EXPECT_TRUE(result.status);

    service.stop();

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, RegisterRoute) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    service.initialize(sec, bus, config_dir, store_dir);

    EXPECT_TRUE(service.registerRoute("service_diag", "vehicle/diag/status",
                                      RouteEntry::Direction::UP));

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, GetConnectionState) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    service.initialize(sec, bus, config_dir, store_dir);

    auto state = service.getConnectionState();
    EXPECT_EQ(state.state, ConnectionState::INIT);

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ApplyFactoryConfiguration) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    service.initialize(sec, bus, config_dir, store_dir);

    ConnectionConfig config;
    config.broker_host = "test.mqtt.com";
    config.broker_port = 8883;
    config.client_id = "TEST_INTEGRATION";

    EXPECT_TRUE(service.applyConfig(config, ConnectionConfig::Source::FACTORY));

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ApplyOtaConfiguration) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

    service.initialize(sec, bus, config_dir, store_dir);

    ConnectionConfig config;
    config.broker_host = "ota.mqtt.com";
    config.broker_port = 8883;
    config.client_id = "TEST_INTEGRATION";
    config.source = ConnectionConfig::Source::OTA;
    config.version = 1;

    EXPECT_TRUE(service.applyConfig(config, ConnectionConfig::Source::OTA));

    cleanupTempDirs();
}
