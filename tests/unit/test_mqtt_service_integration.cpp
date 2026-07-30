#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
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
        ofs << "common:\n"
            << "  store:\n"
            << "    root: /tmp/tbox_mqtt_integration_store\n"
            << "  log:\n"
            << "    level: info\n"
            << "bus:\n"
            << "  address: /tmp/tbox_test_bus.sock\n";
    }

    {
        std::ofstream ofs(conf_d / "mqtt.yaml");
        ofs << "mqtt:\n"
            << "  broker_host: localhost\n"
            << "  broker_port: 1883\n"
            << "  identity:\n"
            << "    client_id_format: \"{vin}-{sn}\"\n"
            << "  keepalive: 60\n"
            << "  clean_start: false\n"
            << "  session_expiry: 3600\n"
            << "  tls:\n"
            << "    credential_profile: mqtt\n"
            << "    min_version: \"1.2\"\n"
            << "    max_version: \"1.3\"\n"
            << "    handshake_timeout_ms: 10000\n"
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

std::shared_ptr<ProvisionIdentityManager> createSimulatedProvMgr() {
    auto mgr = std::make_shared<ProvisionIdentityManager>();
    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "TESTVIN0000000001";
    snapshot.ecu_uid = "TESTECU0000000001";
    snapshot.sn = "TESTSN00000001";
    snapshot.state = tbox::prov::ProvisionState::BOUND;
    snapshot.locked = true;
    mgr->simulateBinding(snapshot);
    return mgr;
}
} // anonymous namespace

TEST(MqttServiceIntegrationTest, ServiceInitialization) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto prov_mgr = createSimulatedProvMgr();

    EXPECT_TRUE(service.initialize(prov_mgr, cred_mgr, config_dir, store_dir));

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ServiceStartStop) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto prov_mgr = createSimulatedProvMgr();

    service.initialize(prov_mgr, cred_mgr, config_dir, store_dir);
    EXPECT_TRUE(service.start());

    service.stop();

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, PublishMessage) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto prov_mgr = createSimulatedProvMgr();

    service.initialize(prov_mgr, cred_mgr, config_dir, store_dir);
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
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto prov_mgr = createSimulatedProvMgr();

    service.initialize(prov_mgr, cred_mgr, config_dir, store_dir);
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
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    auto prov_mgr = createSimulatedProvMgr();

    service.initialize(prov_mgr, cred_mgr, config_dir, store_dir);

    EXPECT_TRUE(service.registerRoute("service_diag", "vehicle/diag/status",
                                      RouteEntry::Direction::UP));

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, GetConnectionState) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    auto prov_mgr = createSimulatedProvMgr();

    service.initialize(prov_mgr, cred_mgr, config_dir, store_dir);

    auto state = service.getConnectionState();
    EXPECT_EQ(state.state, ConnectionState::INIT);

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ApplyFactoryConfiguration) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    auto prov_mgr = createSimulatedProvMgr();

    service.initialize(prov_mgr, cred_mgr, config_dir, store_dir);

    ConnectionConfig config;
    config.broker_host = "test.mqtt.com";
    config.broker_port = 8883;
    config.client_id_format = "{vin}-{sn}";
    config.credential_profile = "mqtt";

    EXPECT_TRUE(service.applyConfig(config, ConnectionConfig::Source::FACTORY));

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ApplyOtaConfiguration) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    MqttService service;
    auto cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    auto prov_mgr = createSimulatedProvMgr();

    service.initialize(prov_mgr, cred_mgr, config_dir, store_dir);

    ConnectionConfig config;
    config.broker_host = "ota.mqtt.com";
    config.broker_port = 8883;
    config.client_id_format = "{vin}-{sn}";
    config.credential_profile = "mqtt";
    config.source = ConnectionConfig::Source::OTA;
    config.version = 1;

    EXPECT_TRUE(service.applyConfig(config, ConnectionConfig::Source::OTA));

    cleanupTempDirs();
}
