// TBOX-MQTT-DSN-CR-018：可信时间门禁 service 级集成测试
// AT-01/AT-04（部分）：强制模式 fail-closed、Fake 可信放行、旁路旧行为。
// 不依赖真实 Broker（连接尝试失败 → RECONNECTING 属预期）。
#include <gtest/gtest.h>
#include "mqtt_service.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "sec_tls_key_adapter.h"
#include "trusted_time_gate.h"
#include "config.h"
#include "store.h"
#include <filesystem>
#include <fstream>

using namespace tbox::mqtt;

namespace {

std::string createConfigDir(bool require_trusted_time) {
    auto tmp_dir = std::filesystem::temp_directory_path() / "tbox_mqtt_timegate_config";
    auto conf_d = tmp_dir / "conf.d";
    std::filesystem::create_directories(conf_d);
    {
        std::ofstream ofs(tmp_dir / "common.yaml");
        ofs << "common:\n"
            << "  store:\n"
            << "    root: /tmp/tbox_mqtt_timegate_store\n"
            << "  log:\n"
            << "    level: info\n"
            << "bus:\n"
            << "  address: /tmp/tbox_timegate_bus.sock\n";
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
            << "    drop_policy: drop_oldest\n"
            << "  trusted_time:\n"
            << "    require_trusted_time: "
            << (require_trusted_time ? "true" : "false") << "\n";
    }
    return tmp_dir.string();
}

void cleanupConfigDir() {
    std::filesystem::remove_all(
        std::filesystem::temp_directory_path() / "tbox_mqtt_timegate_config");
    std::filesystem::remove_all(
        std::filesystem::temp_directory_path() / "tbox_mqtt_timegate_store");
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

struct Deps {
    std::shared_ptr<hwyz::store::Store> store;
    std::shared_ptr<ProvisionIdentityManager> prov_mgr;
    std::shared_ptr<CredentialManager> cred_mgr;
    std::unique_ptr<SecTlsKeyAdapter> tls_adapter;
};

Deps createDeps(const std::string& config_dir) {
    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    Deps d;
    d.store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", "/tmp/tbox_mqtt_timegate_store"));
    d.prov_mgr = createSimulatedProvMgr();
    d.cred_mgr = std::make_shared<CredentialManager>("", "mqtt");
    d.cred_mgr->simulateReady();
    d.tls_adapter = std::make_unique<SecTlsKeyAdapter>();
    return d;
}

// 脚本化 provider：首次返回可信样本，后续返回不可用（模拟运行期失信）
class ScriptedTimeProvider final : public ITrustedTimeProvider {
public:
    explicit ScriptedTimeProvider(TrustedTimeSample sample) : sample_(sample) {}
    TimeQueryResult query() override {
        if (count_++ == 0) {
            TimeQueryResult r;
            r.ok = true;
            r.sample = sample_;
            return r;
        }
        TimeQueryResult r;
        r.ok = false;
        r.reason = TimeGateReason::Untrusted;
        return r;
    }
    TimeProviderInfo info() const override { return {"scripted", false}; }
private:
    TrustedTimeSample sample_;
    int count_ = 0;
};

} // namespace

// AT-01：强制模式 + 无 SEC 契约（ContractUnsupported）→ TIME_WAIT，
// 不建立云连接（无 connect）；publish 拒绝 MQTT-1022，不写队列
TEST(MqttTimeGateServiceTest, EnforcedWithoutProviderFailsClosed) {
    std::string dir = createConfigDir(/*require=*/true);
    auto deps = createDeps(dir);

    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    // 等待首次查询完成（异步 timer 线程）
    for (int i = 0; i < 100 && service.getConnectionState().state != ConnectionState::TIME_WAIT; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_EQ(service.getConnectionState().state, ConnectionState::TIME_WAIT);

    // 上行受理拒绝：MQTT-1022，retryable=true，accepted=false
    PublishEnvelope env;
    env.msg_id = "msg-t1";
    env.topic = "vehicle/TESTECU0000000001/up/fota";
    env.qos = QoS::AT_LEAST_ONCE;
    env.payload = {0x01, 0x02};
    auto r = service.publishEnvelope(env);
    EXPECT_FALSE(r.accepted);
    ASSERT_TRUE(r.error.has_value());
    EXPECT_EQ(r.error.value(), ErrorCode::MQTT_1022);
    EXPECT_TRUE(r.retryable);
    EXPECT_EQ(r.reason_code, time_reason::kPublishUnavailable);

    service.stop();
    cleanupConfigDir();
}

// AT-02（部分）：强制模式 + Fake 可信 → 门禁放行，状态离开 TIME_WAIT；
// 离线 publish accepted（受理 provenance 由 gate 判定）
TEST(MqttTimeGateServiceTest, EnforcedWithFakeTrustedOpensGate) {
    std::string dir = createConfigDir(/*require=*/true);
    auto deps = createDeps(dir);

    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    // 测试钩子：注入 Fake（生产路径不经此接口）
    TrustedTimeSample sample;
    sample.utc_now = std::chrono::system_clock::now();
    sample.trust_state = TimeTrustState::Trusted;
    sample.source = TimeSource::PlatformSync;
    sample.freshness_age = std::chrono::milliseconds(100);
    sample.uncertainty = std::chrono::milliseconds(2000);
    sample.remaining_validity = std::chrono::milliseconds(5000);
    sample.source_epoch = 1;
    sample.sample_sequence = 1;
    sample.provider_epoch = "fake";
    sample.clock_aligned = true;
    sample.contract_version = 1;
    sample.valid = true;
    service.setTimeGateProviderForTest(createFakeTimeProvider(sample));
    ASSERT_TRUE(service.startExternal());

    // 门禁放行：状态应离开 TIME_WAIT（进入 PROV_WAIT/CERT_WAIT/...）
    bool left_time_wait = false;
    for (int i = 0; i < 100; ++i) {
        auto st = service.getConnectionState().state;
        if (st != ConnectionState::TIME_WAIT && st != ConnectionState::INIT) {
            left_time_wait = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(left_time_wait);

    // 离线（未连接 Broker）受理 accepted
    PublishEnvelope env;
    env.msg_id = "msg-t2";
    env.topic = "vehicle/TESTECU0000000001/up/fota";
    env.qos = QoS::AT_LEAST_ONCE;
    env.payload = {0x0A};
    auto r = service.publishEnvelope(env);
    EXPECT_TRUE(r.accepted);

    service.stop();
    cleanupConfigDir();
}

// AT-10（部分）：旁路（require=false）保持旧行为：publish accepted，门禁标记 bypass
TEST(MqttTimeGateServiceTest, BypassKeepsOldBehavior) {
    std::string dir = createConfigDir(/*require=*/false);
    auto deps = createDeps(dir);

    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    PublishEnvelope env;
    env.msg_id = "msg-t3";
    env.topic = "vehicle/TESTECU0000000001/up/fota";
    env.qos = QoS::AT_LEAST_ONCE;
    env.payload = {0xBB};
    auto r = service.publishEnvelope(env);
    EXPECT_TRUE(r.accepted);  // 旧行为

    service.stop();
    cleanupConfigDir();
}

// AT-03：运行期失信 → 有序撤销（readiness 撤销、状态 TIME_WAIT、新发送拒绝）
TEST(MqttTimeGateServiceTest, TrustLossRevokesAndRejectsNewPublish) {
    std::string dir = createConfigDir(/*require=*/true);
    auto deps = createDeps(dir);

    MqttService service(deps.store, *deps.prov_mgr, *deps.cred_mgr, *deps.tls_adapter);
    ASSERT_TRUE(service.initializeLocal());

    TrustedTimeSample sample;
    sample.utc_now = std::chrono::system_clock::now();
    sample.trust_state = TimeTrustState::Trusted;
    sample.source = TimeSource::PlatformSync;
    sample.freshness_age = std::chrono::milliseconds(100);
    sample.uncertainty = std::chrono::milliseconds(2000);
    sample.remaining_validity = std::chrono::milliseconds(10000);
    sample.source_epoch = 1;
    sample.sample_sequence = 1;
    sample.provider_epoch = "scripted";
    sample.clock_aligned = true;
    sample.contract_version = 1;
    sample.valid = true;
    service.setTimeGateProviderForTest(
        std::make_shared<ScriptedTimeProvider>(sample));
    ASSERT_TRUE(service.startExternal());

    // 首次查询可信 → 门禁放行，状态离开 TIME_WAIT
    bool opened = false;
    for (int i = 0; i < 100; ++i) {
        auto st = service.getConnectionState().state;
        if (st != ConnectionState::TIME_WAIT && st != ConnectionState::INIT) {
            opened = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(opened);

    // 第二次查询（刷新）失败 → 运行期失信 → TIME_WAIT
    bool closed = false;
    for (int i = 0; i < 100; ++i) {
        if (service.getConnectionState().state == ConnectionState::TIME_WAIT) {
            closed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(closed);

    // 新上行受理拒绝（MQTT-1022）
    PublishEnvelope env;
    env.msg_id = "msg-lost";
    env.topic = "vehicle/TESTECU0000000001/up/fota";
    env.qos = QoS::AT_LEAST_ONCE;
    env.payload = {0x01};
    auto r = service.publishEnvelope(env);
    EXPECT_FALSE(r.accepted);
    ASSERT_TRUE(r.error.has_value());
    EXPECT_EQ(r.error.value(), ErrorCode::MQTT_1022);

    service.stop();
    cleanupConfigDir();
}
