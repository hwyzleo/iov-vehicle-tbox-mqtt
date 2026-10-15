#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "config.h"
#include "store.h"
#include "sec_tls_key_adapter.h"
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

/// CR-017: TSP 快照包含 catalog-governed keyprov.uplink（模板由 catalog 冻结）
OwnerSubscriptionSnapshot makeKeyprovUplinkSnapshot(uint64_t gen) {
    OwnerSubscriptionSnapshot s;
    s.owner = "tsp";
    s.generation = gen;
    s.content_digest = "digest-" + std::to_string(gen);
    s.registration_complete = true;
    s.contract_version = kSubscriptionCatalogContractVersion;
    SubscriptionItem up;
    up.route_id = kKeyprovUplinkRouteId;
    up.direction = SnapshotDirection::UP;
    up.qos = 1;
    up.enabled = true;
    up.readiness_scope = kCapabilitySecurityKeyprov;
    up.criticality = SubscriptionCriticality::CAPABILITY_MANDATORY;
    s.items.push_back(up);
    return s;
}

/// CR-017: 普通（非 keyprov）owner 自定义上行 route
OwnerSubscriptionSnapshot makeFotaUplinkSnapshot(uint64_t gen) {
    OwnerSubscriptionSnapshot s;
    s.owner = "tsp";
    s.generation = gen;
    s.content_digest = "fota-" + std::to_string(gen);
    s.registration_complete = true;
    SubscriptionItem up;
    up.route_id = "fota.uplink";
    up.topic_template = "vehicle/{ecu_uid}/up/fota";
    up.direction = SnapshotDirection::UP;
    up.qos = 1;
    up.target = "tsp.fota";
    s.items.push_back(up);
    return s;
}

struct MqttServiceDeps {
    std::shared_ptr<hwyz::store::Store> store;
    std::shared_ptr<ProvisionIdentityManager> prov_mgr;
    std::shared_ptr<CredentialManager> cred_mgr;
    std::unique_ptr<SecTlsKeyAdapter> tls_adapter;
};

MqttServiceDeps createMqttDeps(const std::string& config_dir, const std::string& store_dir) {
    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    MqttServiceDeps deps;
    deps.store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    deps.prov_mgr = createSimulatedProvMgr();
    deps.cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    deps.cred_mgr->simulateReady();
    deps.tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    return deps;
}
} // anonymous namespace

TEST(MqttServiceIntegrationTest, ServiceInitialization) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    EXPECT_TRUE(service.initializeLocal());

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ServiceStartStop) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();
    EXPECT_TRUE(service.startExternal());

    service.stop();

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, PublishMessage) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();
    service.startExternal();

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = service.publish("test/topic", payload, QoS::AT_LEAST_ONCE);
    EXPECT_TRUE(result.accepted);

    service.stop();

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, SubscribeToTopic) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();
    service.startExternal();

    auto result = service.subscribe("test/topic", QoS::AT_LEAST_ONCE, "service_diag");
    EXPECT_TRUE(result.status);

    service.stop();

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, RegisterRoute) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();

    EXPECT_TRUE(service.registerRoute("service_diag", "vehicle/diag/status",
                                      RouteEntry::Direction::UP));

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, GetConnectionState) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();

    auto state = service.getConnectionState();
    EXPECT_EQ(state.state, ConnectionState::INIT);

    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, ApplyFactoryConfiguration) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();

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

    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();

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

// ============================================================
// CR-017 §5: keyprov 上行会话绑定 guard
// ============================================================

TEST(MqttServiceIntegrationTest, KeyprovPublishRequiresExpectedSession) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();
    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();
    ASSERT_EQ(service.onReplaceSubscriptionSnapshot(makeKeyprovUplinkSnapshot(1)).status,
              SnapshotAcceptStatus::ACCEPTED);

    RoutePublishRequest req;
    req.key.owner = "tsp";
    req.key.route_id = kKeyprovUplinkRouteId;
    req.msg_id = "kp-missing";
    req.payload = {0x01, 0x02};
    auto res = service.publishRoute(req);
    EXPECT_FALSE(res.accepted);
    EXPECT_EQ(res.reason_code, session_reason::kPublishContextRequired);
    EXPECT_EQ(res.error, ErrorCode::MQTT_1004);
    EXPECT_FALSE(res.retryable);

    service.stop();
    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, KeyprovPublishRejectsStaleSession) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();
    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();
    ASSERT_EQ(service.onReplaceSubscriptionSnapshot(makeKeyprovUplinkSnapshot(1)).status,
              SnapshotAcceptStatus::ACCEPTED);

    RoutePublishRequest req;
    req.key.owner = "tsp";
    req.key.route_id = kKeyprovUplinkRouteId;
    req.msg_id = "kp-stale";
    req.payload = {0x01};
    // 无有效 TLS 会话身份 -> validateSession 失败 -> STALE（需刷新快照后对账）
    SessionContext sc;
    sc.daemon_epoch = "deadbeefdeadbeef";
    sc.connection_generation = 1;
    sc.cert_session_generation = 1;
    req.expected_session = sc;

    auto res = service.publishRoute(req);
    EXPECT_FALSE(res.accepted);
    EXPECT_EQ(res.reason_code, session_reason::kPublishContextStale);
    EXPECT_EQ(res.error, ErrorCode::MQTT_1009);
    EXPECT_TRUE(res.retryable);

    service.stop();
    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, KeyprovPublishRejectsUnsupportedEmptyEpoch) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();
    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();
    ASSERT_EQ(service.onReplaceSubscriptionSnapshot(makeKeyprovUplinkSnapshot(1)).status,
              SnapshotAcceptStatus::ACCEPTED);

    RoutePublishRequest req;
    req.key.owner = "tsp";
    req.key.route_id = kKeyprovUplinkRouteId;
    req.msg_id = "kp-unsupported";
    SessionContext sc;  // 空 epoch = 结构非法/契约不支持
    req.expected_session = sc;

    auto res = service.publishRoute(req);
    EXPECT_FALSE(res.accepted);
    EXPECT_EQ(res.reason_code, session_reason::kPublishContextUnsupported);
    EXPECT_EQ(res.error, ErrorCode::MQTT_1004);

    service.stop();
    cleanupTempDirs();
}

TEST(MqttServiceIntegrationTest, NonKeyprovPublishNotBlockedBySessionGuard) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();
    auto deps = createMqttDeps(config_dir, store_dir);
    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    service.initializeLocal();
    ASSERT_EQ(service.onReplaceSubscriptionSnapshot(makeFotaUplinkSnapshot(1)).status,
              SnapshotAcceptStatus::ACCEPTED);

    RoutePublishRequest req;
    req.key.owner = "tsp";
    req.key.route_id = "fota.uplink";
    req.msg_id = "fota-1";
    req.payload = {0x09};
    // 普通业务不携带 expected_session：guard 不适用（不得返回 session_* reason）
    auto res = service.publishRoute(req);
    EXPECT_NE(res.reason_code, session_reason::kPublishContextRequired);
    EXPECT_NE(res.reason_code, session_reason::kPublishContextStale);

    service.stop();
    cleanupTempDirs();
}
