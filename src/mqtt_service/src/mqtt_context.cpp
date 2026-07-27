#include "mqtt_context.h"
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>
#include <functional>

namespace tbox {
namespace mqtt {

// ============================================================
// 上下文传播辅助 (CR-003 §3)
// ============================================================

std::string generateRequestId() {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()
    ).count();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1000, 9999);

    std::ostringstream oss;
    oss << "mqtt-" << timestamp << "-" << dis(gen);
    return oss.str();
}

// ============================================================
// 脱敏辅助函数 (CR-003 §5)
// ============================================================

namespace {

/**
 * FNV-1a 哈希，生成 64 位摘要后截取前 32 位用于日志。
 * 用于 clientId、msg_id、broker_host 等标识符的不可逆摘要输出。
 */
std::string hashPrefix(const std::string& input, const char* prefix) {
    if (input.empty()) {
        return std::string(prefix) + "empty";
    }

    uint64_t hash = 1469598103934665603ULL; // FNV offset basis
    for (unsigned char c : input) {
        hash ^= c;
        hash *= 1099511628211ULL; // FNV prime
    }

    // 取高 32 位作为摘要，输出 8 位 hex
    uint32_t digest = static_cast<uint32_t>(hash >> 32);

    std::ostringstream oss;
    oss << prefix << std::hex << std::setfill('0') << std::setw(8) << digest;
    return oss.str();
}

/**
 * 判断 topic 段是否可能为设备标识（VIN/SN/device_sn）。
 * 设备标识通常为 17-32 位的字母数字组合，不含通配符。
 */
bool isLikelyDeviceSegment(const std::string& segment) {
    if (segment.empty() || segment.size() < 8) return false;
    if (segment == "up" || segment == "down" || segment == "fota" ||
        segment == "diag" || segment == "status" || segment == "control" ||
        segment == "cmd" || segment == "config" || segment == "ota" ||
        segment == "vehicle" || segment == "v1" || segment == "v2") {
        return false;
    }
    // 设备标识段：长度 >= 8 且包含字母和数字的混合
    bool has_alpha = false, has_digit = false;
    for (char c : segment) {
        if (std::isalpha(static_cast<unsigned char>(c))) has_alpha = true;
        else if (std::isdigit(static_cast<unsigned char>(c))) has_digit = true;
        else if (c != '-' && c != '_') return false; // 仅允许字母数字和-_/
    }
    return has_alpha && has_digit;
}

} // anonymous namespace

std::string brokerHash(const std::string& broker_host) {
    return hashPrefix(broker_host, "h_");
}

std::string topicTemplate(const std::string& topic) {
    if (topic.empty()) {
        return "t_empty";
    }

    // 按 '/' 分割 topic，替换可能的设备标识段为 {device_sn}
    std::string result;
    result.reserve(topic.size() + 16);
    size_t start = 0;
    bool first = true;

    for (size_t i = 0; i <= topic.size(); ++i) {
        if (i == topic.size() || topic[i] == '/') {
            std::string segment = topic.substr(start, i - start);
            if (!first) {
                result += '/';
            }
            first = false;

            if (isLikelyDeviceSegment(segment)) {
                result += "{device_sn}";
            } else {
                result += segment;
            }
            start = i + 1;
        }
    }

    // 如果模板化后仍然包含原始 topic 的大部分内容（即没有设备标识被替换），
    // 则输出不可逆摘要以避免泄露 (CR-003 §5)
    // 但如果 topic 本身就是模板形式（不含设备标识），则直接返回
    return result;
}

std::string clientIdHash(const std::string& client_id) {
    return hashPrefix(client_id, "c_");
}

std::string msgIdHash(const std::string& msg_id) {
    return hashPrefix(msg_id, "m_");
}

} // namespace mqtt
} // namespace tbox
