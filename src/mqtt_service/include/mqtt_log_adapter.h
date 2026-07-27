#pragma once

#include "log.h"
#include "log_types.h"
#include <string>

namespace tbox {
namespace mqtt {

/**
 * TBOX-MQTT 日志适配器 (CR-003 §2)
 * 封装 framework-log 的初始化和模块 Logger 获取。
 *
 * 模块划分（CR-003 §2）：
 * - connection:   连接状态机、证书等待、连接建立/失败/断开
 * - tls:          mTLS 握手、TLS 配置
 * - publish:      上行发布、PUBACK、发布超时/拒绝
 * - subscribe:    订阅请求与失败
 * - offline_queue: 离线缓存入队/丢弃
 * - router:       下行路由、路由失败
 * - ipc:          内部总线消息、IPC
 */
class MqttLogAdapter {
public:
    // 初始化日志系统（在 main.cpp 中调用一次，CR-003 §2）
    static tbox::fw::log::InitResult init(
        const std::string& service,
        const tbox::fw::log::LogConfig& config
    );

    // 获取各模块的 Logger 实例（CR-003 §2）
    static tbox::fw::log::Logger connection();
    static tbox::fw::log::Logger tls();
    static tbox::fw::log::Logger publish();
    static tbox::fw::log::Logger subscribe();
    static tbox::fw::log::Logger offline_queue();
    static tbox::fw::log::Logger router();
    static tbox::fw::log::Logger ipc();

    // 获取 service 级别 Logger（用于初始化成功等生命周期事件）
    static tbox::fw::log::Logger service();

private:
    static bool s_initialized;
};

} // namespace mqtt
} // namespace tbox
