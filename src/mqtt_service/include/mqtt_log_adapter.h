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
 * - provision:    PROV 绑定、身份
 * - dependency:   SEC/PROV IPC 依赖恢复事件 (CR-016 §8)
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
    static tbox::fw::log::Logger provision();
    // CR-016 §8: SEC/PROV IPC 依赖恢复事件（mqtt_dependency_*）
    static tbox::fw::log::Logger dependency();

    // 获取 service 级别 Logger（用于初始化成功等生命周期事件）
    static tbox::fw::log::Logger service();

    // CR-016 §10: 关键 warn/error 路径集中落盘。
    // 只使用 framework-log 公开 API（共享 sink 的 flush），不直接访问私有 sink；
    // 调用点集中在依赖连接失败、长期等待等少数路径，禁止零散刷盘。
    static void flushCritical();

private:
    static bool s_initialized;
};

} // namespace mqtt
} // namespace tbox
