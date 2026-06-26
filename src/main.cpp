#include "mqtt_service.h"
#include "sec_cert_interface.h"
#include "internal_bus_interface.h"
#include "config_manager.h"
#include <iostream>
#include <signal.h>
#include <memory>
#include <thread>
#include <chrono>
#include <atomic>
#include <filesystem>

using namespace tbox::mqtt;

std::atomic<bool> g_running{true};
std::unique_ptr<MqttService> g_service;

void signal_handler(int signal) {
    g_running = false;
}

int main(int argc, char* argv[]) {
    // Setup signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    std::cout << "Starting TBOX MQTT Service..." << std::endl;

    // Create dependencies
    auto sec = std::make_shared<SecCertInterface>();
    auto bus = std::make_shared<InternalBusInterface>();

#ifdef SIMULATE_CERT
    sec->simulateCertificateReady();
#endif

    // Create and initialize service
    g_service = std::make_unique<MqttService>();

    if (!g_service->initialize(sec, bus)) {
        std::cerr << "Failed to initialize MQTT service" << std::endl;
        return 1;
    }

    // Load configuration from file or use defaults
    ConnectionConfig config;
    const std::string config_path = "./mqtt_config.json";

    if (std::filesystem::exists(config_path)) {
        ConfigManager config_manager;
        if (config_manager.loadFromFile(config_path)) {
            config = config_manager.getConfig();
            std::cout << "Using config from: " << config_path << std::endl;
        } else {
            std::cerr << "Failed to load config from " << config_path
                      << ", using defaults" << std::endl;
            config.broker_host = "localhost";
            config.broker_port = 1883;
            config.client_id = "VIN123SN456";
        }
    } else {
        std::cout << "Config file not found: " << config_path
                  << ", using defaults" << std::endl;
        config.broker_host = "localhost";
        config.broker_port = 1883;
        config.client_id = "VIN123SN456";
    }

    config.source = ConnectionConfig::Source::FACTORY;
    config.version = 1;

    if (!g_service->applyConfig(config, ConnectionConfig::Source::FACTORY)) {
        std::cerr << "Failed to apply configuration" << std::endl;
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

        // Monitor connection state - only log on transitions
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
