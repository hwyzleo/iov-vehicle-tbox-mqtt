#pragma once

//
// TBOX-MQTT-DSN-CR-016 §3, §6, §7
//
// SEC/PROV IPC 依赖恢复的共享类型与有界退避工具。
//
// 设计约定：
//   - 每个依赖（SEC/PROV）拥有独立的 client、mutex/strand、timer、retry state
//     与 generation（本头只描述状态/退避；client 生命周期由各自 Manager 持有）。
//   - 同一依赖最多允许一个 armed timer 与一个 connect attempt。
//   - 退避复用既有 initial/multiplier/max 语义并叠加受控 jitter，
//     封顶后 retry_count 仍继续增长（设计 §6）。
//

#include <chrono>
#include <cstdint>
#include <random>
#include <string>

#include "mqtt_types.h"  // Duration

namespace tbox {
namespace mqtt {

/// 依赖种类（设计 §3）
enum class Dependency : uint8_t {
    SEC = 0,
    PROV = 1
};

/// IPC transport 状态（设计 §3）
enum class IpcState : uint8_t {
    DISCONNECTED = 0,
    CONNECTING = 1,
    CONNECTED = 2
};

inline const char* dependencyName(Dependency d) {
    switch (d) {
        case Dependency::SEC:  return "sec";
        case Dependency::PROV: return "prov";
    }
    return "unknown";
}

inline const char* ipcStateName(IpcState s) {
    switch (s) {
        case IpcState::DISCONNECTED: return "DISCONNECTED";
        case IpcState::CONNECTING:   return "CONNECTING";
        case IpcState::CONNECTED:    return "CONNECTED";
    }
    return "UNKNOWN";
}

/// 单依赖恢复状态（设计 §3）
///
/// - retry_count 在每次实际进入一次恢复尝试时递增，成功离开等待态后重置为 0
/// - total_attempts 在 daemon epoch 内保持累计
/// - client_generation 每次 invalidate/recreate transport 时递增
/// - timer_armed 表示当前依赖是否已有一次已调度的恢复尝试
struct RetryState {
    uint64_t cycle_id = 0;
    uint64_t retry_count = 0;
    uint64_t total_attempts = 0;
    Duration next_backoff{0};
    uint64_t client_generation = 0;
    bool timer_armed = false;
};

/// 是否为需要集中落盘的关键依赖事件（设计 §10）。
/// 仅在连接失败、查询传输失败、长期未就绪等 warn/error 路径 flush；
/// connect_attempt/connected/ready 等常规事件不触发刷盘。
inline bool isCriticalDependencyEvent(const char* event) {
    if (event == nullptr) return false;
    return std::string(event) == "mqtt_dependency_ipc_connect_failed" ||
           std::string(event) == "mqtt_dependency_query_failed" ||
           std::string(event) == "mqtt_dependency_not_ready";
}

/// 有界指数退避（设计 §6）：
///   delay = initial * 2^min(retry_index, 6)，封顶 max，叠加 ±10% jitter。
/// retry_index 为本次尝试之前已完成的连续失败次数（0 基）。
inline Duration computeDependencyBackoff(uint64_t retry_index,
                                        Duration initial,
                                        Duration max_backoff,
                                        std::mt19937& rng) {
    uint64_t shift = retry_index > 6 ? 6 : retry_index;
    int64_t delay_ms = static_cast<int64_t>(initial.count());
    for (uint64_t i = 0; i < shift; ++i) {
        if (delay_ms >= max_backoff.count()) {
            delay_ms = static_cast<int64_t>(max_backoff.count());
            break;
        }
        delay_ms *= 2;
    }
    if (delay_ms > static_cast<int64_t>(max_backoff.count())) {
        delay_ms = static_cast<int64_t>(max_backoff.count());
    }
    if (delay_ms < 0) {
        delay_ms = 0;
    }

    int64_t jitter_ms = delay_ms / 10;
    if (jitter_ms > 0) {
        std::uniform_int_distribution<int64_t> dist(-jitter_ms, jitter_ms);
        delay_ms += dist(rng);
    }
    if (delay_ms < 0) {
        delay_ms = 0;
    }
    return Duration(delay_ms);
}

} // namespace mqtt
} // namespace tbox
