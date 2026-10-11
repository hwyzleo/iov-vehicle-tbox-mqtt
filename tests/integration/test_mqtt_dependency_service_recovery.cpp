//
// TBOX-MQTT-DSN-CR-016 §11: MqttService 级 PROV 晚启动恢复
//
// 组合真实 ProvisionIdentityManager（真实 framework-ipc client）+ 最小 stub PROV
// Server + MqttService，验证：
//   - stub 未启动时服务停留 PROV_WAIT（不再永久自锁）
//   - stub 稍后就绪后，无需重启 daemon 即自动离开 PROV_WAIT
//

#include <gtest/gtest.h>

#include "mqtt_service.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "sec_tls_key_adapter.h"
#include "config.h"
#include "store.h"
#include "ipc.h"

#include "support/dependency_stub_protocol.h"

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

using namespace tbox::mqtt;

namespace {

// 进程级忽略 SIGPIPE（与 daemon hwyz::Application 一致）
struct IgnoreSigpipe {
    IgnoreSigpipe() { ::signal(SIGPIPE, SIG_IGN); }
};
const IgnoreSigpipe g_ignore_sigpipe{};

std::string uniqueTag() {
    return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

std::string uniqueSock(const std::string& tag) {
    return "/tmp/tbox_mqtt_cr016_svc_" + tag + ".sock";
}

std::string createConfigDir(const std::string& tag, const std::string& store_root,
                            const std::string& ipc_sock) {
    auto tmp_dir = std::filesystem::temp_directory_path() / ("tbox_mqtt_cr016_cfg_" + tag);
    auto conf_d = tmp_dir / "conf.d";
    std::filesystem::create_directories(conf_d);

    {
        std::ofstream ofs(tmp_dir / "common.yaml");
        ofs << "common:\n"
            << "  store:\n"
            << "    root: " << store_root << "\n"
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
            << "      initial_backoff_ms: 50\n"
            << "      max_backoff_ms: 200\n"
            << "      multiplier: 2.0\n"
            << "  keepalive: 60\n"
            << "  clean_start: false\n"
            << "  session_expiry: 3600\n"
            << "  tls:\n"
            << "    credential_profile: mqtt\n"
            << "    min_version: \"1.2\"\n"
            << "    max_version: \"1.3\"\n"
            << "    handshake_timeout_ms: 10000\n"
            << "  ipc:\n"
            << "    socket_path: " << ipc_sock << "\n"
            << "  offline_queue:\n"
            << "    max_size: 100\n"
            << "    drop_policy: drop_oldest\n";
    }
    return tmp_dir.string();
}

std::string provResponder(uint32_t method_id, const std::string&) {
    if (method_id == test::kProvReadBinding) {
        return R"({"status":0,"vin":"HWYZTEST900000001","ecu_uid":"00000001AA00000001","sn":"TBOX00000001","state":2,"locked":true})";
    }
    return R"({"status":0})";
}

class StubProvServer {
public:
    explicit StubProvServer(std::string sock) : sock_(std::move(sock)) {}
    void start() {
        ::unlink(sock_.c_str());
        server_ = std::make_unique<::tbox::fw::ipc::Server>(sock_);
        server_->start(
            [](uint32_t m, std::string_view p, int /*fd*/) {
                return provResponder(m, std::string(p));
            },
            {});
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    void stop() {
        if (server_) { server_->stop(); server_.reset(); }
        ::unlink(sock_.c_str());
    }
    ~StubProvServer() { stop(); }

private:
    std::string sock_;
    std::unique_ptr<::tbox::fw::ipc::Server> server_;
};

} // namespace

TEST(MqttDependencyServiceRecoveryTest, ServiceLeavesProvWaitWhenProvStartsLate) {
    std::string tag = uniqueTag();
    std::string store_root = "/tmp/tbox_mqtt_cr016_store_" + tag;
    std::string ipc_sock = "/tmp/tbox_mqtt_cr016_ipc_" + tag + ".sock";
    std::string prov_sock = uniqueSock("prov_" + tag);
    std::string config_dir = createConfigDir(tag, store_root, ipc_sock);

    hwyz::config::ConfigManager::instance().load("mqtt", config_dir);
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_root));

    // 真实 PROV client（stub 尚未启动），模拟 SEC 凭据就绪
    ProvisionIdentityManager prov_mgr(prov_sock);
    CredentialManager cred_mgr("", "mqtt");
    cred_mgr.simulateReady();
    SecTlsKeyAdapter tls_adapter;

    MqttService service(store, prov_mgr, cred_mgr, tls_adapter);
    ASSERT_TRUE(service.initializeLocal());
    ASSERT_TRUE(service.startExternal());

    // 初始应停留 PROV_WAIT（首次连接失败不永久自锁）
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(service.getConnectionState().state, ConnectionState::PROV_WAIT);

    // PROV 稍后就绪：无需重启 MQTT
    StubProvServer server(prov_sock);
    server.start();

    bool left_prov_wait = false;
    for (int i = 0; i < 50; ++i) {  // 最多 ~5s
        if (service.getConnectionState().state != ConnectionState::PROV_WAIT) {
            left_prov_wait = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_TRUE(left_prov_wait);
    EXPECT_NE(service.getConnectionState().state, ConnectionState::INIT);

    service.stop();
    server.stop();
    std::filesystem::remove_all(config_dir);
    std::filesystem::remove_all(store_root);
    ::unlink(ipc_sock.c_str());
}
