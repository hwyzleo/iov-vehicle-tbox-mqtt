#include "mqtt_service.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "sec_cert_interface.h"
#include "config.h"
#include "log_types.h"
#include <iostream>
#include <signal.h>
#include <memory>
#include <thread>
#include <chrono>
#include <atomic>

using namespace tbox::mqtt;

std::atomic<bool> g_running{true};
std::unique_ptr<MqttService> g_service;

void signal_handler(int signal) {
    // 信号处理仍使用 stderr，因为 Logger 可能正在关闭
    std::cerr << "\n[signal] received signal " << signal << ", requesting shutdown" << std::endl;
    g_running = false;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);

    // Load configuration via framework-config (Design §5, CR-003 §2, CR-004 §4)
    // 启动顺序: Config::load("mqtt") -> Logger::init -> Store::open -> ...
    std::string config_root = "/etc/tbox/";
    if (argc > 1) config_root = argv[1];

    auto& fw_config = hwyz::config::ConfigManager::instance();
    auto err = fw_config.load("mqtt", config_root);
    if (err != hwyz::config::ConfigError::kOk) {
        auto info = fw_config.getLastError();
        std::cerr << "Config load failed: " << info.message
                  << " (path: " << info.path << ")" << std::endl;
        return 1;
    }

    auto snapshot = fw_config.getSnapshot();
    if (!snapshot) {
        std::cerr << "Failed to get config snapshot" << std::endl;
        return 1;
    }

    // Read store root path from store.root (Design §5)
    std::string store_root = snapshot->getString("common.store.root", "/var/lib/tbox");

    // Initialize framework-log (CR-003 §2, CR-004 §4)
    tbox::fw::log::LogConfig log_config;
    log_config.level = tbox::fw::log::LogLevel::kInfo;
    log_config.console_config.enabled = true;
    log_config.format = "standard";

    std::string log_level_str = snapshot->getString("common.log.level", "info");
    if (log_level_str == "trace") log_config.level = tbox::fw::log::LogLevel::kTrace;
    else if (log_level_str == "debug") log_config.level = tbox::fw::log::LogLevel::kDebug;
    else if (log_level_str == "info") log_config.level = tbox::fw::log::LogLevel::kInfo;
    else if (log_level_str == "warn") log_config.level = tbox::fw::log::LogLevel::kWarn;
    else if (log_level_str == "error") log_config.level = tbox::fw::log::LogLevel::kError;

    std::string log_file = snapshot->getString("common.log.file", "");
    if (!log_file.empty()) {
        log_config.file_config.enabled = true;
        auto last_slash = log_file.find_last_of('/');
        if (last_slash != std::string::npos) {
            log_config.file_config.root = log_file.substr(0, last_slash);
        }
        log_config.file_config.max_file_size_mb = static_cast<uint32_t>(
            snapshot->getInt("common.log.max_size_mb", 20));
        log_config.file_config.max_files = static_cast<uint32_t>(
            snapshot->getInt("common.log.max_files", 5));
    }

    auto mqtt_log_section = snapshot->getSection("mqtt.log");
    if (mqtt_log_section) {
        std::string mqtt_level = mqtt_log_section->getString("level", "");
        if (!mqtt_level.empty()) {
            if (mqtt_level == "trace") log_config.level = tbox::fw::log::LogLevel::kTrace;
            else if (mqtt_level == "debug") log_config.level = tbox::fw::log::LogLevel::kDebug;
            else if (mqtt_level == "info") log_config.level = tbox::fw::log::LogLevel::kInfo;
            else if (mqtt_level == "warn") log_config.level = tbox::fw::log::LogLevel::kWarn;
            else if (mqtt_level == "error") log_config.level = tbox::fw::log::LogLevel::kError;
        }
    }

    auto log_result = MqttLogAdapter::init("mqtt", log_config);
    if (log_result.error != tbox::fw::log::LogError::kOk) {
        std::cerr << "[MQTT] framework-log init degraded: " << log_result.error_message << std::endl;
    }

    MqttLogAdapter::service().info(
        "mqtt.service.log_initialized",
        "MQTT 日志系统初始化",
        {
            {"service", tbox::fw::log::FieldValue::makeString("mqtt")},
            {"sink_mode", tbox::fw::log::FieldValue::makeString(
                log_config.file_config.enabled ? "file+console" : "console")},
            {"log_level", tbox::fw::log::FieldValue::makeString(log_level_str)}
        }
    );

    MqttLogAdapter::service().info(
        "mqtt.service.starting",
        "TBOX MQTT Service starting",
        {
            {"config_root", tbox::fw::log::FieldValue::makeString(config_root)},
            {"store_root", tbox::fw::log::FieldValue::makeString(store_root)}
        }
    );

    // CR-004 §4 生命周期: 初始化离线队列与 tbox::sec_client
    std::string sec_socket_path = snapshot->getString("sec.socket_path", "/tmp/tbox-sec.sock");
    auto sec = std::make_shared<SecCertInterface>(sec_socket_path);

#ifdef SIMULATE_CERT
    sec->simulateCertificateReady();
#endif

    // CR-004 §4: 构造 MqttService 并初始化
    g_service = std::make_unique<MqttService>();

    if (!g_service->initialize(sec, config_root, store_root)) {
        MqttLogAdapter::service().error(
            "mqtt.service.init_failed",
            "Failed to initialize MQTT service"
        );
        return 1;
    }

    // Register routes (CR-004 §3)
    g_service->registerRoute("service_diag", "vehicle/diag/status",
                             RouteEntry::Direction::UP);
    g_service->registerRoute("service_control", "vehicle/control/cmd",
                             RouteEntry::Direction::DOWN);

    // Start service (state machine + certificate flow)
    if (!g_service->start()) {
        MqttLogAdapter::service().error(
            "mqtt.service.start_failed",
            "Failed to start MQTT service"
        );
        return 1;
    }

    // CR-004 §4: 构造 IpcConfig(common.ipc.* + mqtt.ipc.*) -> 注册 MqttIpcDispatcher
    // -> ipc::Server::start
    MqttIpcConfig ipc_config;
    ipc_config.socket_path = snapshot->getString("mqtt.ipc.socket_path", "/tmp/tbox-mqtt.sock");
    ipc_config.dispatch_queue_size = static_cast<uint32_t>(
        snapshot->getInt("mqtt.ipc.dispatch_queue_size", 1024));
    ipc_config.subscriber_queue_size = static_cast<uint32_t>(
        snapshot->getInt("mqtt.ipc.subscriber_queue_size", 128));
    ipc_config.slow_subscriber_policy =
        snapshot->getString("mqtt.ipc.slow_subscriber_policy", "disconnect");

    if (!g_service->startIpcServer(ipc_config)) {
        MqttLogAdapter::service().error(
            "mqtt.service.ipc_start_failed",
            "Failed to start IPC server"
        );
        return 1;
    }

    MqttLogAdapter::service().info(
        "mqtt.service.started",
        "TBOX MQTT Service started (framework-ipc bus)"
    );

    // Main loop
    ConnectionState last_logged_state = ConnectionState::INIT;
    bool first_iteration = true;
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto state = g_service->getConnectionState();
        if (first_iteration || state.state != last_logged_state) {
            MqttLogAdapter::connection().info(
                "mqtt.connection.state_report",
                "Connection state report",
                {
                    {"state", tbox::fw::log::FieldValue::makeString(
                        connectionStateToString(state.state))},
                    {"retry_count", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(state.retry_count))},
                    {"last_error", tbox::fw::log::FieldValue::makeString(state.last_error)}
                }
            );
            last_logged_state = state.state;
            first_iteration = false;
        }
    }

    // CR-004 §4: 停止时先拒绝新请求 -> 停 IPC Server -> 等 client 线程
    // -> 停 Broker 网络循环 -> flush 离线队列/store/log
    MqttLogAdapter::service().info(
        "mqtt.service.shutting_down",
        "TBOX MQTT Service shutting down"
    );
    g_service->stop();
    g_service.reset();
    return 0;
}
