#include "mqtt_service.h"
#include "sec_cert_interface.h"
#include "internal_bus_interface.h"
#include "config.h"
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
    g_running = false;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    std::cout << "Starting TBOX MQTT Service..." << std::endl;

    // Load configuration via framework-config (Design §5)
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
    std::string store_root = snapshot->getString("store.root", "/var/lib/tbox");
    std::cout << "Config loaded from: " << config_root << std::endl;
    std::cout << "Store root: " << store_root << std::endl;

    // Create dependencies
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

#ifdef SIMULATE_CERT
    sec->simulateCertificateReady();
#endif

    // Create and initialize service with framework paths
    g_service = std::make_unique<MqttService>();

    if (!g_service->initialize(sec, bus, config_root, store_root)) {
        std::cerr << "Failed to initialize MQTT service" << std::endl;
        return 1;
    }

    // Register routes
    g_service->registerRoute("service_diag", "vehicle/diag/status",
                             RouteEntry::Direction::UP);
    g_service->registerRoute("service_control", "vehicle/control/cmd",
                             RouteEntry::Direction::DOWN);

    // Start service
    if (!g_service->start()) {
        std::cerr << "Failed to start MQTT service" << std::endl;
        return 1;
    }

    std::cout << "TBOX MQTT Service started successfully" << std::endl;

    // Main loop
    ConnectionState last_logged_state = ConnectionState::INIT;
    bool first_iteration = true;
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto state = g_service->getConnectionState();
        if (first_iteration || state.state != last_logged_state) {
            std::cout << "Connection state: " << static_cast<int>(state.state)
                      << " - " << state.last_error << std::endl;
            last_logged_state = state.state;
            first_iteration = false;
        }
    }

    std::cout << "Shutting down..." << std::endl;
    g_service->stop();
    g_service.reset();
    return 0;
}
