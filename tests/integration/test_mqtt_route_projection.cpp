// CR-009 §13.2: Route-based 路由投影集成测试
// 覆盖 FOTA 模板实例化、PROV 延迟到达、身份变化、下行分发、Legacy 守卫
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
    auto tmp = std::filesystem::path(uniqueDir("tbox_mqtt_cr9_cfg"));
    auto conf_d = tmp / "conf.d";
    std::filesystem::create_directories(conf_d);
    {
        std::ofstream ofs(tmp / "common.yaml");
        ofs << "common:\n  store:\n    root: " << uniqueDir("tbox_mqtt_cr9_store")
            << "\n  log:\n    level: info\n";
    }
    std::string sock = uniqueDir("tbox_mqtt_cr9.sock");
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
            << "  subscriptions:\n    required_owners: []\n"
            << "    startup_wait_timeout_ms: 100\n    reconcile_timeout_ms: 10000\n"
            << "    suback_retry_max: 3\n    owner_lease_ms: 0\n";
    }
    return tmp.string();
}

std::string createTempStoreDir() {
    auto dir = uniqueDir("tbox_mqtt_cr9_store");
    std::filesystem::create_directories(dir);
    return dir;
}

void cleanupDirs(const std::string& cfg, const std::string& store) {
    std::filesystem::remove_all(cfg);
    std::filesystem::remove_all(store);
}

std::shared_ptr<ProvisionIdentityManager> createBoundProvMgr(const std::string& ecu_uid) {
    auto mgr = std::make_shared<ProvisionIdentityManager>();
    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = ecu_uid;
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    snapshot.locked = true;
    mgr->simulateBinding(snapshot);
    return mgr;
}

SubscriptionItem makeItem(const std::string& id, const std::string& tmpl,
                          SnapshotDirection dir, uint8_t qos,
                          const std::string& target, bool mand) {
    SubscriptionItem it;
    it.route_id = id; it.topic_template = tmpl; it.direction = dir;
    it.qos = qos; it.target = target; it.mandatory = mand;
    return it;
}

OwnerSubscriptionSnapshot makeTspSnapshot(uint64_t gen, const std::string& digest,
        bool complete, std::vector<SubscriptionItem> items) {
    OwnerSubscriptionSnapshot s;
    s.owner = "tsp"; s.generation = gen; s.content_digest = digest;
    s.registration_complete = complete; s.items = std::move(items);
    return s;
}

struct Cr9Fixture {
    std::string cfg_dir;
    std::string store_dir;
    std::shared_ptr<hwyz::store::Store> store;
    std::shared_ptr<ProvisionIdentityManager> prov;
    std::shared_ptr<CredentialManager> cred;
    std::unique_ptr<SecTlsKeyAdapter> tls;
    std::unique_ptr<MqttService> service;

    void setup(const std::string& ecu_uid = "ECU000000000000001") {
        cfg_dir = createTempConfigDir();
        store_dir = createTempStoreDir();
        hwyz::config::ConfigManager::instance().load("mqtt", cfg_dir);
        store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        prov = createBoundProvMgr(ecu_uid);
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
        cleanupDirs(cfg_dir, store_dir);
    }
};

} // anonymous namespace

// T-032: TSP FOTA 模板 -> PROV ECU UID -> 正确路由投影
TEST(MqttRouteProjectionTest, FotaTemplatesResolveToCorrectTopic) {
    Cr9Fixture f;
    f.setup("ECU000000000000001");

    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true),
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    });
    auto r = f.service->onReplaceSubscriptionSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);

    // 验证路由投影已构建
    auto table = f.service->getRouteTable();
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->totalRoutes(), 2u);
    EXPECT_EQ(table->activeRoutes(), 2u);

    // 上行路由展开为正确 Topic
    auto up = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(up.has_value());
    EXPECT_EQ(up->expanded_topic, "vehicle/ECU000000000000001/up/fota");
    EXPECT_EQ(up->state, RouteState::ACTIVE);

    // 下行路由展开为正确 Topic
    auto down = table->lookupRoute({"tsp", "fota.down"});
    ASSERT_TRUE(down.has_value());
    EXPECT_EQ(down->expanded_topic, "vehicle/ECU000000000000001/down/fota");

    f.teardown();
}

// T-033: PROV 延迟到达 -- 快照 ACCEPTED 但路由 UNRESOLVED
TEST(MqttRouteProjectionTest, ProvLateArrivalRoutesUnresolved) {
    Cr9Fixture f;
    f.setup("ECU000000000000001");

    // 先模拟 PROV 未绑定
    f.prov->simulateUnbound();
    f.service->rebuildRouteProjection();

    // 提交快照（模板合法）
    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    });
    auto r = f.service->onReplaceSubscriptionSnapshot(snap);
    EXPECT_EQ(r.status, SnapshotAcceptStatus::ACCEPTED);

    // PROV 未就绪 -> 路由 UNRESOLVED
    auto table = f.service->getRouteTable();
    auto route = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->state, RouteState::UNRESOLVED);
    EXPECT_TRUE(route->expanded_topic.empty());

    // publishRoute 应被拒绝（MQTT-1009）
    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "msg_late";
    req.payload = {0x01};
    auto pub = f.service->publishRoute(req);
    EXPECT_FALSE(pub.accepted);
    EXPECT_EQ(pub.error, tbox::mqtt::ErrorCode::MQTT_1009);

    // PROV 就绪后身份变化收敛
    ProvisionIdentitySnapshot identity;
    identity.vin = "HWYZTEST900000001";
    identity.ecu_uid = "ECU000000000000001";
    identity.sn = "TBOX00000001";
    identity.state = ProvisionState::BOUND;
    identity.locked = true;
    f.prov->simulateBinding(identity);
    f.service->rebuildRouteProjection();

    // 路由应变为 ACTIVE
    table = f.service->getRouteTable();
    route = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->state, RouteState::ACTIVE);
    EXPECT_EQ(route->expanded_topic, "vehicle/ECU000000000000001/up/fota");

    f.teardown();
}

// T-034: PROV 绑定变化 -- 旧 Topic 失效，新 Topic 重建
TEST(MqttRouteProjectionTest, ProvBindingChangeInvalidatesOldTopic) {
    Cr9Fixture f;
    f.setup("ECU_OLD_001");

    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    });
    f.service->onReplaceSubscriptionSnapshot(snap);

    // 验证旧 Topic
    auto table = f.service->getRouteTable();
    auto route = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->expanded_topic, "vehicle/ECU_OLD_001/up/fota");
    uint64_t old_gen = route->identity_generation;

    // PROV 绑定变化
    ProvisionIdentitySnapshot new_identity;
    new_identity.vin = "HWYZTEST900000002";
    new_identity.ecu_uid = "ECU_NEW_002";
    new_identity.sn = "TBOX00000002";
    new_identity.state = ProvisionState::BOUND;
    new_identity.locked = true;
    f.prov->simulateBinding(new_identity);
    f.service->rebuildRouteProjection();

    // 新 Topic 应使用新 ecu_uid
    table = f.service->getRouteTable();
    route = table->lookupRoute({"tsp", "fota.up"});
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->expanded_topic, "vehicle/ECU_NEW_002/up/fota");
    EXPECT_NE(route->identity_generation, old_gen);

    f.teardown();
}

// T-035: 下行路由反查 -- Topic 匹配到 owner/route_id/target
TEST(MqttRouteProjectionTest, DownlinkReverseLookupMatchesTarget) {
    Cr9Fixture f;
    f.setup("ECU000000000000001");

    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("fota.down", "vehicle/{ecu_uid}/down/fota", SnapshotDirection::DOWN, 1, "tsp.fota", true)
    });
    f.service->onReplaceSubscriptionSnapshot(snap);

    auto table = f.service->getRouteTable();

    // 下行 Topic 反查应匹配到 tsp.fota
    auto refs = table->lookupDownlink("vehicle/ECU000000000000001/down/fota");
    ASSERT_EQ(refs.size(), 1u);
    EXPECT_EQ(refs[0].key.owner, "tsp");
    EXPECT_EQ(refs[0].key.route_id, "fota.down");
    EXPECT_EQ(refs[0].target, "tsp.fota");

    // 无匹配 Topic
    auto no_refs = table->lookupDownlink("vehicle/ECU000000000000001/down/nonexistent");
    EXPECT_TRUE(no_refs.empty());

    f.teardown();
}

// T-036: Legacy 守卫 -- route API 启用后 legacy topic 需匹配 ecu_uid
TEST(MqttRouteProjectionTest, LegacyGuardRejectsWrongDeviceUid) {
    Cr9Fixture f;
    f.setup("ECU000000000000001");

    // 启用 route API（提交快照）
    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    });
    f.service->onReplaceSubscriptionSnapshot(snap);

    // Legacy publish with wrong ecu_uid -> 拒绝（守卫激活）
    PublishEnvelope env;
    env.msg_id = "msg_legacy_wrong";
    env.topic = "vehicle/WRONG_ECU/up/fota";
    env.qos = QoS::AT_LEAST_ONCE;
    env.payload = {0x01};
    auto result = f.service->publishEnvelope(env);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, tbox::mqtt::ErrorCode::MQTT_1004);

    // Legacy publish with correct ecu_uid -> 通过守卫
    env.msg_id = "msg_legacy_ok";
    env.topic = "vehicle/ECU000000000000001/up/fota";
    auto result_ok = f.service->publishEnvelope(env);
    EXPECT_TRUE(result_ok.accepted);

    f.teardown();
}

// T-036b: Route-based publish 与 legacy publish 不重复
TEST(MqttRouteProjectionTest, RoutePublishResolvesCorrectTopic) {
    Cr9Fixture f;
    f.setup("ECU000000000000001");

    auto snap = makeTspSnapshot(1, "d1", true, {
        makeItem("fota.up", "vehicle/{ecu_uid}/up/fota", SnapshotDirection::UP, 1, "tsp.fota", true)
    });
    f.service->onReplaceSubscriptionSnapshot(snap);

    // Route-based publish（离线模式，入队）
    RoutePublishRequest req;
    req.key = {"tsp", "fota.up"};
    req.msg_id = "msg_route_001";
    req.payload = {0x01, 0x02, 0x03};
    req.priority = Priority::NORMAL;
    auto result = f.service->publishRoute(req);
    EXPECT_TRUE(result.accepted);

    f.teardown();
}
