//
// TBOX-MQTT-DSN-CR-008 §9.1: 回滚验证用的旧式手写生命周期入口。
// 当 MQTT_USE_FRAMEWORK_APPLICATION=OFF 时编译。不使用 hwyz::Application，
// 自行安装信号、初始化日志、装配依赖并维护运行循环。
// 验收后删除本文件与开关（CR-008 阶段 3）。
//
// 注意：MqttService 已按 CR-008 瘦身（构造接收注入引用，initialize 拆为
// initializeLocal/startExternal），故本入口手动构造 Store/PROV/SEC/TLS 并注入。
//

#include "mqtt_service.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "sec_tls_key_adapter.h"
#include "config.h"
#include "log.h"
#include "store.h"

#include <iostream>
#include <cstring>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <chrono>
#include <memory>

using namespace tbox::mqtt;

static std::atomic<bool> shutdown_requested(false);
static volatile sig_atomic_t received_signal = 0;

extern "C" void signal_handler(int signum) {
    received_signal = signum;
    shutdown_requested.store(true, std::memory_order_relaxed);
    static const char msg[] = "\n[signal] shutdown requested\n";
    ssize_t ignored = write(STDERR_FILENO, msg, sizeof(msg) - 1);
    (void)ignored;
}

static bool install_signal_handlers() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    const int graceful_signals[] = {SIGINT, SIGTERM};
    for (int sig : graceful_signals) {
        if (sigaction(sig, &sa, nullptr) != 0) {
            std::cerr << "FATAL: sigaction failed for signal " << sig << std::endl;
            return false;
        }
    }

    struct sigaction sa_ign;
    memset(&sa_ign, 0, sizeof(sa_ign));
    sa_ign.sa_handler = SIG_IGN;
    sigemptyset(&sa_ign.sa_mask);
    if (sigaction(SIGPIPE, &sa_ign, nullptr) != 0) {
        std::cerr << "FATAL: sigaction failed for SIGPIPE" << std::endl;
        return false;
    }
    return true;
}

int main(int argc, char* argv[]) {
    if (!install_signal_handlers()) {
        return 1;
    }

    std::string config_root = "/etc/tbox/";
    if (argc > 1) config_root = argv[1];

    auto err = hwyz::config::ConfigManager::instance().load("mqtt", config_root);
    if (err != hwyz::config::ConfigError::kOk) {
        auto info = hwyz::config::ConfigManager::instance().getLastError();
        std::cerr << "FATAL: Config load failed: " << info.message
                  << " (path: " << info.path << ")" << std::endl;
        return 1;
    }

    auto cfg = hwyz::config::ConfigManager::instance().getSnapshot();
    if (!cfg) {
        std::cerr << "FATAL: Failed to get config snapshot" << std::endl;
        return 1;
    }

    // Initialize framework-log (legacy: 自行初始化)
    tbox::fw::log::LogConfig log_config;
    log_config.level = tbox::fw::log::LogLevel::kInfo;
    log_config.console_config.enabled = true;
    log_config.format = "standard";

    std::string log_level_str = cfg->getString("common.log.level", "info");
    if (log_level_str == "trace") log_config.level = tbox::fw::log::LogLevel::kTrace;
    else if (log_level_str == "debug") log_config.level = tbox::fw::log::LogLevel::kDebug;
    else if (log_level_str == "info") log_config.level = tbox::fw::log::LogLevel::kInfo;
    else if (log_level_str == "warn") log_config.level = tbox::fw::log::LogLevel::kWarn;
    else if (log_level_str == "error") log_config.level = tbox::fw::log::LogLevel::kError;

    auto log_result = MqttLogAdapter::init("mqtt", log_config);
    if (log_result.error != tbox::fw::log::LogError::kOk) {
        std::cerr << "[MQTT] framework-log init degraded: " << log_result.error_message << std::endl;
    }

    MqttLogAdapter::service().info(
        "mqtt.service.starting",
        "TBOX MQTT Service starting (legacy lifecycle)",
        {{"config_root", tbox::fw::log::FieldValue::makeString(config_root)}});

    std::string store_root = cfg->getString("common.store.root", "/var/lib/tbox");

    // 构造外部依赖（CR-008 注入）
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_root));
    if (!store->isReady()) {
        std::cerr << "FATAL: Store open failed: " << store_root << std::endl;
        return 1;
    }

    std::string prov_socket_path = cfg->getString("prov.socket_path", "/tmp/tbox-prov.sock");
    auto prov_mgr = std::make_unique<ProvisionIdentityManager>(prov_socket_path);

#ifdef SIMULATE_PROV
    {
        ProvisionIdentitySnapshot sim_snapshot;
        sim_snapshot.vin = "HWYZTEST900000001";
        sim_snapshot.ecu_uid = "00000001AA00000001";
        sim_snapshot.sn = "TBOX00000001";
        sim_snapshot.state = tbox::prov::ProvisionState::BOUND;
        sim_snapshot.locked = true;
        prov_mgr->simulateBinding(sim_snapshot);
    }
#endif

    std::string sec_socket_path = cfg->getString("sec.socket_path", "/tmp/tbox-sec.sock");
    std::string credential_profile = cfg->getString("mqtt.tls.credential_profile", "mqtt");
    auto cred_mgr = std::make_unique<CredentialManager>(sec_socket_path, credential_profile);

#ifdef SIMULATE_CERT
    cred_mgr->simulateReady();
#endif

    auto tls_adapter = std::make_unique<SecTlsKeyAdapter>();

    // 构造 MqttService（注入）
    MqttService service(store, *prov_mgr, *cred_mgr, *tls_adapter);

    if (!service.initializeLocal()) {
        MqttLogAdapter::service().error(
            "mqtt.service.init_failed",
            "Failed to initialize MQTT service (local)");
        return 1;
    }

    // 注册路由（兼容入口）
    service.registerRoute("service_diag", "vehicle/diag/status",
                          RouteEntry::Direction::UP);
    service.registerRoute("service_control", "vehicle/control/cmd",
                          RouteEntry::Direction::DOWN);

    if (!service.startExternal()) {
        MqttLogAdapter::service().error(
            "mqtt.service.start_failed",
            "Failed to start MQTT service (external)");
        return 1;
    }

    MqttLogAdapter::service().info(
        "mqtt.service.started",
        "TBOX MQTT Service started (legacy lifecycle)");

    // 主循环
    while (!shutdown_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    MqttLogAdapter::service().info(
        "mqtt.service.shutting_down",
        "TBOX MQTT Service shutting down",
        {tbox::fw::log::Field("signal",
            tbox::fw::log::FieldValue::makeInt(static_cast<int>(received_signal)))});

    service.stop();

    MqttLogAdapter::service().info(
        "mqtt.service.stopped",
        "TBOX MQTT Service stopped");
    return 0;
}
