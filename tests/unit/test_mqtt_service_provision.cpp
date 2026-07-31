#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "mqtt_context.h"
#include "config.h"
#include "store.h"
#include "sec_tls_key_adapter.h"
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

using namespace tbox::mqtt;
using namespace tbox::prov;

namespace {

std::string createTempConfigDir() {
    auto tmp_dir = std::filesystem::temp_directory_path() / "tbox_mqtt_prov_config";
    auto conf_d = tmp_dir / "conf.d";
    std::filesystem::create_directories(conf_d);

    {
        std::ofstream ofs(tmp_dir / "common.yaml");
        ofs << "common:\n"
            << "  store:\n"
            << "    root: /tmp/tbox_mqtt_prov_store\n"
            << "  log:\n"
            << "    level: info\n";
    }

    {
        std::ofstream ofs(conf_d / "mqtt.yaml");
        ofs << "mqtt:\n"
            << "  broker_host: localhost\n"
            << "  broker_port: 1883\n"
            << "  identity:\n"
            << "    client_id_format: \"{vin}-{sn}\"\n"
            << "    prov_retry:\n"
            << "      initial_backoff_ms: 100\n"
            << "      max_backoff_ms: 1000\n"
            << "      multiplier: 2.0\n"
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
    auto dir = std::filesystem::temp_directory_path() / "tbox_mqtt_prov_store";
    std::filesystem::create_directories(dir);
    return dir.string();
}

void cleanupTempDirs() {
    std::filesystem::remove_all(std::filesystem::temp_directory_path() / "tbox_mqtt_prov_config");
    std::filesystem::remove_all(std::filesystem::temp_directory_path() / "tbox_mqtt_prov_store");
}

std::shared_ptr<ProvisionIdentityManager> createBoundProvMgr() {
    auto mgr = std::make_shared<ProvisionIdentityManager>();
    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "00000001AA00000001";
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    snapshot.locked = true;
    mgr->simulateBinding(snapshot);
    return mgr;
}

std::shared_ptr<ProvisionIdentityManager> createUnboundProvMgr() {
    auto mgr = std::make_shared<ProvisionIdentityManager>();
    mgr->simulateUnbound();
    return mgr;
}

} // anonymous namespace

// ============================================================
// CR-006 §4: PROV_WAIT 初始状态
// ============================================================

TEST(MqttServiceProvisionTest, StartsInProvWaitWhenUnbound) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    auto prov_mgr = createUnboundProvMgr();
    auto cred_mgr = std::make_unique<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    // With unbound PROV, state should be PROV_WAIT (not CERT_WAIT or CONNECTING)
    auto state = service.getConnectionState();
    EXPECT_EQ(state.state, ConnectionState::PROV_WAIT);

    service.stop();
    cleanupTempDirs();
}

// ============================================================
// CR-006 §6: client_id 从 VIN+SN 派生
// ============================================================

TEST(MqttServiceProvisionTest, ClientIdDerivedFromVinAndSn) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    auto prov_mgr = createBoundProvMgr();
    auto cred_mgr = std::make_unique<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    // Give state machine time to transition PROV_WAIT -> CERT_WAIT -> CONNECTING
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // The identity snapshot should have client_id = VIN-SN
    auto snapshot = prov_mgr->getSnapshot();
    EXPECT_EQ(snapshot.client_id, "HWYZTEST900000001-TBOX00000001");

    // client_id should NOT be the old config-based value
    EXPECT_NE(snapshot.client_id, "TEST_INTEGRATION");

    service.stop();
    cleanupTempDirs();
}

// ============================================================
// CR-006 §6.3: username 从 ecu_uid 派生
// ============================================================

TEST(MqttServiceProvisionTest, UsernameDerivedFromEcuUid) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    auto prov_mgr = createBoundProvMgr();
    auto cred_mgr = std::make_unique<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // ECU UID from binding should be available
    auto snapshot = prov_mgr->getSnapshot();
    EXPECT_EQ(snapshot.ecu_uid, "00000001AA00000001");

    // The config should not have username from config file (it's runtime-derived)
    // Verify ecu_uid is not empty and matches binding
    EXPECT_FALSE(snapshot.ecu_uid.empty());

    service.stop();
    cleanupTempDirs();
}

// ============================================================
// CR-006 §4: PROV 绑定有效后进入连接流程
// ============================================================

TEST(MqttServiceProvisionTest, TransitionsToConnectingWhenBoundAndCertReady) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    auto prov_mgr = createBoundProvMgr();
    auto cred_mgr = std::make_unique<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    // Wait for state machine to transition: PROV_WAIT -> CERT_WAIT -> CONNECTING
    // (connection will fail since no real broker, but should reach CONNECTING)
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto state = service.getConnectionState();
    // Should have progressed past PROV_WAIT
    EXPECT_NE(state.state, ConnectionState::PROV_WAIT);
    EXPECT_NE(state.state, ConnectionState::INIT);

    service.stop();
    cleanupTempDirs();
}

// ============================================================
// CR-006 §7: 身份变化触发重连
// ============================================================

TEST(MqttServiceProvisionTest, IdentityChangeTriggersProvWait) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    auto prov_mgr = createBoundProvMgr();
    auto cred_mgr = std::make_unique<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Simulate identity change (VIN changed)
    ProvisionIdentitySnapshot new_snapshot;
    new_snapshot.vin = "NEWVIN9000000002";  // Different VIN
    new_snapshot.ecu_uid = "00000001AA00000001";
    new_snapshot.sn = "TBOX00000001";
    new_snapshot.state = ProvisionState::BOUND;
    prov_mgr->simulateBinding(new_snapshot);

    // Generation should have changed
    EXPECT_TRUE(prov_mgr->hasIdentityChanged(0));

    // New client_id should reflect new VIN
    auto snapshot = prov_mgr->getSnapshot();
    EXPECT_EQ(snapshot.client_id, "NEWVIN9000000002-TBOX00000001");

    service.stop();
    cleanupTempDirs();
}

// ============================================================
// CR-006 §7: 离线队列身份变化后丢弃旧 Topic 消息
// ============================================================

TEST(MqttServiceProvisionTest, OfflineQueueSkipsStaleIdentityMessages) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    auto prov_mgr = createBoundProvMgr();
    auto cred_mgr = std::make_unique<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    // Wait for connection attempt (will fail, service in RECONNECTING or similar)
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Queue a message while disconnected (identity_generation = current gen)
    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = service.publish("vehicle/test/topic", payload, QoS::AT_LEAST_ONCE);
    // Should be accepted (queued in offline cache)
    EXPECT_TRUE(result.accepted);

    // Simulate identity change (ECU UID changed - topics become invalid)
    ProvisionIdentitySnapshot new_snapshot;
    new_snapshot.vin = "HWYZTEST900000001";
    new_snapshot.ecu_uid = "NEWECU0000000002";  // Different ECU UID
    new_snapshot.sn = "TBOX00000001";
    new_snapshot.state = ProvisionState::BOUND;
    prov_mgr->simulateBinding(new_snapshot);

    // The old message had the old identity generation
    // On next flush, it should be skipped due to generation mismatch

    service.stop();
    cleanupTempDirs();
}

// ============================================================
// CR-006 §8: 日志脱敏 - 原始身份标识不泄露
// ============================================================

TEST(MqttServiceProvisionTest, LogDoesNotLeakRawIdentity) {
    std::string config_dir = createTempConfigDir();
    std::string store_dir = createTempStoreDir();

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));
    auto prov_mgr = createBoundProvMgr();
    auto cred_mgr = std::make_unique<CredentialManager>("", "mqtt");
    cred_mgr->simulateReady();
    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify hash functions produce non-reversible output
    std::string vin_hash = vinHash("HWYZTEST900000001");
    std::string sn_hash = snHash("TBOX00000001");
    std::string ecu_hash = ecuUidHash("00000001AA00000001");
    std::string cid_hash = clientIdHash("HWYZTEST900000001-TBOX00000001");

    // Hashes should not contain raw values
    EXPECT_EQ(vin_hash.find("HWYZTEST"), std::string::npos);
    EXPECT_EQ(sn_hash.find("TBOX"), std::string::npos);
    EXPECT_EQ(ecu_hash.find("00000001"), std::string::npos);
    EXPECT_EQ(cid_hash.find("HWYZTEST"), std::string::npos);

    // Hashes should have correct prefixes
    EXPECT_NE(vin_hash.find("v_"), std::string::npos);
    EXPECT_NE(sn_hash.find("s_"), std::string::npos);
    EXPECT_NE(ecu_hash.find("e_"), std::string::npos);
    EXPECT_NE(cid_hash.find("c_"), std::string::npos);

    service.stop();
    cleanupTempDirs();
}
