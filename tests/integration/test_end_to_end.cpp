#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "sec_cert_interface.h"
#include "config.h"
#include <thread>
#include <chrono>
#include <filesystem>
#include <fstream>

using namespace tbox::mqtt;

namespace {
std::string createE2EConfigDir() {
    auto tmp_dir = std::filesystem::temp_directory_path() / "tbox_mqtt_e2e_config";
    auto conf_d = tmp_dir / "conf.d";
    std::filesystem::create_directories(conf_d);

    {
        std::ofstream ofs(tmp_dir / "common.yaml");
        ofs << "store:\n"
            << "  root: /tmp/tbox_mqtt_e2e_store\n"
            << "log:\n"
            << "  level: info\n";
    }

    {
        std::ofstream ofs(conf_d / "mqtt.yaml");
        ofs << "mqtt:\n"
            << "  broker_host: localhost\n"
            << "  broker_port: 1883\n"
            << "  client_id: VIN123SN456\n"
            << "  keepalive: 60\n"
            << "  clean_start: false\n"
            << "  session_expiry: 3600\n"
            << "  root_ca_ref: /path/to/ca.crt\n"
            << "  tls_cert_ref: /path/to/client.crt\n"
            << "  tls_key_ref: /path/to/client.key\n"
            << "  key_source: file\n";
    }

    return tmp_dir.string();
}

std::string createE2EStoreDir() {
    auto dir = std::filesystem::temp_directory_path() / "tbox_mqtt_e2e_store";
    std::filesystem::create_directories(dir);
    return dir.string();
}

void cleanupE2EDirs() {
    std::filesystem::remove_all(std::filesystem::temp_directory_path() / "tbox_mqtt_e2e_config");
    std::filesystem::remove_all(std::filesystem::temp_directory_path() / "tbox_mqtt_e2e_store");
}
} // anonymous namespace

class EndToEndTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_dir = createE2EConfigDir();
        store_dir = createE2EStoreDir();

        service = std::make_unique<MqttService>();
        sec = std::make_shared<SecCertInterface>("");

        sec->simulateCertificateReady();

        service->initialize(sec, config_dir, store_dir);
        service->start();

        // Wait for connection
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    void TearDown() override {
        service->stop();
        cleanupE2EDirs();
    }

    std::string config_dir;
    std::string store_dir;
    std::unique_ptr<MqttService> service;
    std::shared_ptr<SecCertInterface> sec;
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
    // Connection attempt fails (fake cert paths / no real broker), state transitions to RECONNECTING
    EXPECT_TRUE(state.state == ConnectionState::CONNECTING ||
                state.state == ConnectionState::RECONNECTING);
}

TEST_F(EndToEndTest, ConfigurationUpdate) {
    ConnectionConfig new_config;
    new_config.broker_host = "new.mqtt.com";
    new_config.broker_port = 8883;
    new_config.client_id = "VIN123SN456";
    new_config.version = 2;

    EXPECT_TRUE(service->applyConfig(new_config, ConnectionConfig::Source::OTA));
}
