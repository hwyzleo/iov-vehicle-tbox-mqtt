// CR-007 §13: 订阅快照与 Cloud Readiness 集成测试
// 覆盖快照提交/查询/幂等/冲突/替换/多 owner 聚合/状态隔离
#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "sec_tls_key_adapter.h"
#include "mqtt_context.h"
#include "config.h"
#include "store.h"
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>
#include <atomic>

using namespace tbox::mqtt;
using namespace tbox::prov;

namespace {

std::atomic<int> g_seq{0};

std::string uniqueDir(const std::string& base) {
    return (std::filesystem::temp_directory_path() /
            (base + "_" + std::to_string(++g_seq))).string();
}

std::string createTempConfigDir() {
    auto tmp = std::filesystem::path(uniqueDir("tbox_mqtt_subs_cfg"));
    auto conf_d = tmp / "conf.d";
    std::filesystem::create_directories(conf_d);
    {
        std::ofstream ofs(tmp / "common.yaml");
        ofs << "common:\n  store:\n    root: " << uniqueDir("tbox_mqtt_subs_store")
            << "\n  log:\n    level: info\n";
    }
    std::string sock = uniqueDir("tbox_mqtt_subs.sock");
    {
        std::ofstream ofs(conf_d / "mqtt.yaml");
        ofs << "mqtt:\n"
            << "  broker_host: localhost\n  broker_port: 1883\n"
            << "  identity:\n    client_id_format: \"{vin}-{sn}\"\n"
            << "  keepalive: 60\n  clean_start: false\n  session_expiry: 3600\n"
            << "  tls:\n    credential_profile: mqtt\n    min_version: \"1.2\"\n    max_version: \"1.3\"\n"
            << "  offline_queue:\n    max_size: 100\n    drop_policy: drop_oldest\n"
            << "  ipc:\n    socket_path: " << sock << "\n    dispatch_queue_size: 1024\n"
            << "    subscriber_queue_size: 128\n    slow_subscriber_policy: disconnect\n"
            << "  subscriptions:\n    required_owners: [\"tsp\"]\n"
            << "    startup_wait_timeout_ms: 5000\n    reconcile_timeout_ms: 10000\n"
            << "    suback_retry_max: 3\n    owner_lease_ms: 0\n";
    }
    return tmp.string();
}

std::string createTempStoreDir() {
    auto dir = uniqueDir("tbox_mqtt_subs_store");
    std::filesystem::create_directories(dir);
    return dir;
}

void cleanup(const std::string& cfg, const std::string& store) {
    std::filesystem::remove_all(cfg);
    std::filesystem::remove_all(store);
    // socket 残留
    std::filesystem::remove_all(std::filesystem::temp_directory_path() / "tbox_mqtt_subs.sock_1");
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

OwnerSubscriptionSnapshot makeTspSnapshot(uint64_t gen, const std::string& digest,
        bool complete, std::vector<SubscriptionItem> items) {
    OwnerSubscriptionSnapshot s;
    s.owner = "tsp";
    s.generation = gen;
    s.content_digest = digest;
    s.registration_complete = complete;
    s.items = std::move(items);
    return s;
}

SubscriptionItem makeItem(const std::string& id, const std::string& tmpl,
                          SnapshotDirection dir, uint8_t qos, bool mand) {
    SubscriptionItem it;
    it.route_id = id; it.topic_template = tmpl; it.direction = dir;
    it.qos = qos; it.target = "tsp.fota"; it.mandatory = mand;
    return it;
}

struct ServiceFixture {
    std::string cfg_dir;
    std::string store_dir;
    std::shared_ptr<hwyz::store::Store> store;
    std::shared_ptr<ProvisionIdentityManager> prov;
    std::shared_ptr<CredentialManager> cred;
    std::unique_ptr<SecTlsKeyAdapter> tls;
    std::unique_ptr<MqttService> service;
    void setup() {
        cfg_dir = createTempConfigDir();
        store_dir = createTempStoreDir();
        hwyz::config::ConfigManager::instance().load("mqtt", cfg_dir);
        store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        prov = createBoundProvMgr();
        cred = std::make_shared<CredentialManager>("", "mqtt");
        cred->simulateReady();
        tls = std::make_unique<SecTlsKeyAdapter>();
        service = std::make_unique<MqttService>(store, *prov, *cred, *tls);
        ASSERT_TRUE(service->initializeLocal());
        service->startExternal();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    void teardown() {
        if (service) service->stop();
        cleanup(cfg_dir, store_dir);
    }
};

} // anonymous namespace

TEST(MqttSubscriptionSnapshotTest, AcceptSnapshotAndQueryStatus) {
    ServiceFixture f;
    f.setup();
    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, true)
    });
    auto r = f.service->onReplaceSubscriptionSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r.accepted_generation, 1u);

    auto st = f.service->onGetSubscriptionSnapshotStatus("tsp", 1);
    EXPECT_EQ(st.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(st.generation, 1u);
    EXPECT_EQ(st.content_digest, "d1");
    EXPECT_TRUE(st.registration_complete);

    auto readiness = f.service->onGetSubscriptionReadiness();
    EXPECT_EQ(readiness.owner_generations["tsp"], 1u);
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, IdempotentResubmit) {
    ServiceFixture f;
    f.setup();
    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/a", SnapshotDirection::DOWN, 1, false)
    });
    auto r1 = f.service->onReplaceSubscriptionSnapshot(snap);
    auto r2 = f.service->onReplaceSubscriptionSnapshot(snap);
    EXPECT_EQ(r1.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r2.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r2.reason_code, "idempotent");
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, DigestConflictRejected) {
    ServiceFixture f;
    f.setup();
    f.service->onReplaceSubscriptionSnapshot(makeTspSnapshot(1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/a", SnapshotDirection::DOWN, 1, false)
    }));
    auto r = f.service->onReplaceSubscriptionSnapshot(makeTspSnapshot(1, "d2", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/b", SnapshotDirection::DOWN, 1, false)
    }));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::REJECTED);
    EXPECT_EQ(r.reason_code, "digest_conflict");
    // 当前仍是 d1
    auto st = f.service->onGetSubscriptionSnapshotStatus("tsp", 1);
    EXPECT_EQ(st.content_digest, "d1");
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, NewGenerationReplaces) {
    ServiceFixture f;
    f.setup();
    f.service->onReplaceSubscriptionSnapshot(makeTspSnapshot(1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/a", SnapshotDirection::DOWN, 1, true)
    }));
    auto r = f.service->onReplaceSubscriptionSnapshot(makeTspSnapshot(2, "d2", true, {
        makeItem("b", "vehicle/{ecu_uid}/down/b", SnapshotDirection::DOWN, 1, true)
    }));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r.accepted_generation, 2u);
    auto st = f.service->onGetSubscriptionSnapshotStatus("tsp", 2);
    EXPECT_EQ(st.generation, 2u);
    EXPECT_EQ(st.content_digest, "d2");
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, StaleGenerationReturnsCurrent) {
    ServiceFixture f;
    f.setup();
    f.service->onReplaceSubscriptionSnapshot(makeTspSnapshot(5, "d5", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/a", SnapshotDirection::DOWN, 1, false)
    }));
    auto r = f.service->onReplaceSubscriptionSnapshot(makeTspSnapshot(3, "d3", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/x", SnapshotDirection::DOWN, 1, false)
    }));
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    EXPECT_EQ(r.accepted_generation, 5u);
    EXPECT_EQ(r.reason_code, "stale_generation");
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, InvalidSchemaRejected) {
    ServiceFixture f;
    f.setup();
    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/a", SnapshotDirection::DOWN, 5, false)  // qos 非法
    });
    auto r = f.service->onReplaceSubscriptionSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::REJECTED);
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, MultipleOwnersAggregate) {
    ServiceFixture f;
    f.setup();
    // tsp 和 diag 都订阅同 topic
    f.service->onReplaceSubscriptionSnapshot(makeTspSnapshot(1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/x", SnapshotDirection::DOWN, 1, false)
    }));
    OwnerSubscriptionSnapshot diag;
    diag.owner = "diag";
    diag.generation = 1;
    diag.content_digest = "dd";
    diag.registration_complete = true;
    SubscriptionItem it;
    it.route_id = "dx"; it.topic_template = "vehicle/{ecu_uid}/down/x";
    it.direction = SnapshotDirection::DOWN; it.qos = 2; it.target = "diag.x"; it.mandatory = true;
    diag.items.push_back(it);
    auto r = f.service->onReplaceSubscriptionSnapshot(diag);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);

    auto readiness = f.service->onGetSubscriptionReadiness();
    EXPECT_EQ(readiness.owner_generations.size(), 2u);
    EXPECT_EQ(readiness.owner_generations["tsp"], 1u);
    EXPECT_EQ(readiness.owner_generations["diag"], 1u);
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, ConnectionStateNotConfusedWithCloudReady) {
    // CR-007 §13: Connected/SnapshotAccepted/SUBACK/SubscriptionsReady/CloudReady 五状态不混淆
    ServiceFixture f;
    f.setup();
    auto state = f.service->getConnectionState();
    // 无 Broker 连接，不应是 CLOUD_READY
    EXPECT_NE(state.state, ConnectionState::CLOUD_READY);

    auto readiness = f.service->onGetSubscriptionReadiness();
    // 未连接时 cloud_ready 必为 false
    EXPECT_FALSE(readiness.cloud_ready);
    f.teardown();
}

TEST(MqttSubscriptionSnapshotTest, SnapshotAcceptedBeforeBrokerConnect) {
    // CR-007 §6: 快照注册先于 Cloud Ready 可用
    ServiceFixture f;
    f.setup();
    // 即使未连接 Broker，快照仍可提交
    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("a", "vehicle/{ecu_uid}/down/a", SnapshotDirection::DOWN, 1, true)
    });
    auto r = f.service->onReplaceSubscriptionSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);
    // 但 cloud_ready 仍 false（未连接）
    EXPECT_FALSE(f.service->onGetSubscriptionReadiness().cloud_ready);
    f.teardown();
}
