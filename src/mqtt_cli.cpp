// TBOX-MQTT CLI client tool (CR-004: 测试用 IPC client)
//
// 用法:
//   mqtt_cli [-s <socket_path>] <command> [args...]
// 命令:
//   publish <topic> <payload_file> [qos]        - 上行发布
//   subscribe <topic> [qos]                     - 订阅下行（阻塞直至 Ctrl-C）
//   register_route <internal_addr> <mqtt_topic> <UP|DOWN> [qos]
//   state                                       - 查询连接状态

#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <csignal>

using namespace tbox::mqtt;

static std::atomic<bool> g_running{true};

static void signal_handler(int) {
    g_running = false;
}

static std::string read_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

static void print_usage() {
    std::cout << "TBOX MQTT CLI (IPC Client, CR-004)\n"
              << "Usage: mqtt_cli [-s <socket_path>] <command> [args...]\n\n"
              << "Options:\n"
              << "  -s, --socket <path>  IPC socket (default: /tmp/tbox-mqtt.sock)\n\n"
              << "Commands:\n"
              << "  publish <topic> <payload_file> [qos]        上行发布\n"
              << "  subscribe <topic> [qos]                     订阅下行\n"
              << "  register_route <internal_addr> <topic> <UP|DOWN> [qos]\n"
              << "  state                                       查询连接状态\n";
}

int main(int argc, char* argv[]) {
    std::string socket_path = "/tmp/tbox-mqtt.sock";

    // 解析 -s 选项
    int argi = 1;
    while (argi < argc && std::string(argv[argi]) == "-s") {
        if (argi + 1 >= argc) {
            print_usage();
            return 1;
        }
        socket_path = argv[argi + 1];
        argi += 2;
    }

    if (argi >= argc) {
        print_usage();
        return 1;
    }

    std::string command = argv[argi++];

    Client client(socket_path);

    if (command == "state") {
        auto state = client.getConnectionState();
        std::cout << "ConnectionState: " << connectionStateToString(state) << std::endl;
        return 0;
    }

    if (command == "publish") {
        if (argi + 1 >= argc) {
            print_usage();
            return 1;
        }
        std::string topic = argv[argi++];
        std::string payload_file = argv[argi++];
        int qos = (argi < argc) ? std::atoi(argv[argi++]) : 1;

        std::string payload_str = read_file(payload_file);
        std::vector<uint8_t> payload(payload_str.begin(), payload_str.end());

        PublishEnvelope env;
        env.msg_id = "cli-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        env.topic = topic;
        env.qos = static_cast<QoS>(qos);
        env.payload = payload;
        env.content_type = "application/octet-stream";

        auto result = client.publish(env);
        std::cout << "publish accepted=" << (result.accepted ? "true" : "false")
                  << " outcome=" << (result.outcome == PublishOutcome::ACCEPTED ? "ACCEPTED" : "UNKNOWN");
        if (result.error) {
            std::cout << " error=" << errorCodeToString(result.error.value());
        }
        std::cout << std::endl;
        return result.accepted ? 0 : 1;
    }

    if (command == "subscribe") {
        if (argi >= argc) {
            print_usage();
            return 1;
        }
        std::string topic = argv[argi++];
        int qos = (argi < argc) ? std::atoi(argv[argi++]) : 1;

        signal(SIGINT, signal_handler);
        signal(SIGTERM, signal_handler);

        Route route;
        route.topic = topic;
        route.qos = static_cast<QoS>(qos);

        auto sub = client.subscribe(route, [](const std::string& t, const std::vector<uint8_t>& p) {
            std::cout << "[downlink] topic=" << t << " bytes=" << p.size() << std::endl;
        });

        if (!sub.isActive()) {
            std::cerr << "subscribe failed" << std::endl;
            return 1;
        }
        std::cout << "subscribed to " << topic << ", waiting for messages (Ctrl-C to exit)..." << std::endl;
        while (g_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return 0;
    }

    if (command == "register_route") {
        if (argi + 2 >= argc) {
            print_usage();
            return 1;
        }
        RouteEntry entry;
        entry.internal_addr = argv[argi++];
        entry.mqtt_topic = argv[argi++];
        std::string dir = argv[argi++];
        entry.direction = (dir == "DOWN") ? RouteEntry::Direction::DOWN : RouteEntry::Direction::UP;
        entry.qos = static_cast<QoS>((argi < argc) ? std::atoi(argv[argi++]) : 1);

        auto result = client.registerRoute(entry);
        std::cout << "registerRoute ok=" << (result.ok ? "true" : "false");
        if (result.error) {
            std::cout << " error=" << errorCodeToString(result.error.value());
        }
        std::cout << std::endl;
        return result.ok ? 0 : 1;
    }

    print_usage();
    return 1;
}
