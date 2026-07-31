#pragma once

// TBOX-MQTT 客户端 facade (CR-004 §7.1)
//
// 调用方（车内业务服务）通过此接口与 MQTT daemon 交互。
// IPC 传输细节封装在库内部，对调用方不可见。
// 内部使用 framework-ipc Client + MqttRetryPolicy。
//
// 边界（CR-004 §5.2, §7.1）：
// - publish 只有在相同 msg_id 可去重时允许一次重试；否则返回 unknown outcome。
// - accepted 不表示 Broker 已 PUBACK；QoS 重发由 MQTT daemon 管理。
// - getConnectionState、registerRoute（同 route key 覆盖）允许一次传输重试。
// - 每个 subscribe 创建独立 framework Subscription（独立 socket）。

#include <memory>
#include <string>
#include "tbox/mqtt/types.h"
#include "tbox/mqtt/errors.h"
#include "ipc.h"  // framework-ipc Subscription (RAII 句柄)

namespace tbox {
namespace mqtt {

/// Routed downlink 订阅 RAII 句柄 (CR-010 §2.2)
///
/// 由 Client::subscribeRoutedDownlink 返回。move-only；析构幂等 cancel()，
/// 等待内部 reader/dispatcher 线程有界退出。cancel 后不再启动新 callback。
/// 内部拥有独立 framework-ipc 事件连接 + 每 owner 有界 dispatch queue。
class RoutedSubscription {
public:
    RoutedSubscription();
    ~RoutedSubscription();

    RoutedSubscription(RoutedSubscription&&) noexcept;
    RoutedSubscription& operator=(RoutedSubscription&&) noexcept;

    RoutedSubscription(const RoutedSubscription&) = delete;
    RoutedSubscription& operator=(const RoutedSubscription&) = delete;

    /// 幂等取消：停止读取线程、关闭连接、停止 dispatcher、等待在途 callback 完成。
    void cancel();

    /// 是否仍在活跃接收事件。
    bool isActive() const;

private:
    class Impl;
    std::shared_ptr<Impl> impl_;
    friend class Client;
    explicit RoutedSubscription(std::shared_ptr<Impl> impl);
};

/// MQTT 客户端 facade (CR-004 §7.1)
class Client {
public:
    /// @param socket_path MQTT daemon IPC socket 路径（默认 /tmp/tbox-mqtt.sock）
    explicit Client(const std::string& socket_path = "/tmp/tbox-mqtt.sock");
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) = delete;
    Client& operator=(Client&&) = delete;

    /// 显式连接（也可惰性连接，CR-004 §7.1）
    bool connect();
    void disconnect();
    bool is_connected() const;

    /// 上行发布 (CR-004 §5)
    /// @return accepted 仅表示 daemon 已接管；outcome=UNKNOWN 表示响应丢失需用相同 msg_id 查询/重试。
    PublishResult publish(const PublishEnvelope& message);

    /// CR-009 §6.1: Route-based 上行发布
    /// 业务服务只需 owner + route_id + msg_id + payload，无需访问 PROV 或传完整 Topic。
    /// @return accepted 仅表示 daemon 已接管；outcome=UNKNOWN 表示响应丢失需用相同 msg_id 查询/重试。
    PublishResult publishRoute(const std::string& owner,
                               const std::string& route_id,
                               const std::string& msg_id,
                               const std::vector<uint8_t>& payload,
                               QoS qos = QoS::AT_LEAST_ONCE,
                               Priority priority = Priority::NORMAL,
                               const std::string& content_type = "",
                               const std::string& trace_id = "",
                               const std::string& request_id = "");

    /// 订阅下行路由 (CR-004 §6)
    /// 每次 subscribe 建立独立连接，完成订阅 Response 后进入 event-only loop。
    /// 返回 RAII Subscription 句柄，析构时自动取消。
    /// @return 活跃 Subscription；失败返回 isActive()==false 的空句柄。
    /// @deprecated CR-010 §7: 保留一个发布周期；route-aware 调用方改用 subscribeRoutedDownlink。
    ::tbox::fw::ipc::Subscription subscribe(const Route& route, MessageCallback callback);

    /// 维护内部地址 <-> MQTT topic 映射 (CR-004 §3, 设计 §5)
    /// 同 route key 覆盖；允许一次传输重试。
    Result<void> registerRoute(const RouteEntry& route);

    /// 只读连接状态查询 (CR-004 §3)
    /// 允许一次传输重试。
    ConnectionState getConnectionState();

    // ---- CR-010 §2.2: Routed downlink 订阅 ----
    /// 订阅 routed downlink 事件（仅建立 IPC 事件通道，不触发 Broker SUBSCRIBE/registerRoute）。
    /// Broker 订阅继续由 replaceSubscriptionSnapshot() 的 route 投影驱动。
    /// @param owner    订阅 owner，非空且满足命名规则
    /// @param callback typed 事件回调（在 SDK 内部锁外调用）
    /// @return 活跃 RoutedSubscription；owner 互斥冲突或参数非法返回 isActive()==false 的空句柄。
    RoutedSubscription subscribeRoutedDownlink(std::string_view owner,
                                               RoutedDownlinkCallback callback);

    // ---- CR-007 §4: 订阅快照与 Cloud Readiness ----
    /// 原子提交完整订阅快照。ACCEPTED 仅表示 MQTT daemon 已接受本地投影，
    /// 不等于 Broker SUBACK 或 Cloud Ready。
    ReplaceSnapshotResult replaceSubscriptionSnapshot(const OwnerSubscriptionSnapshot& snapshot);

    /// 查询指定 owner/generation 的接受状态（响应丢失后幂等收敛）
    SnapshotStatusResult getSubscriptionSnapshotStatus(const std::string& owner, uint64_t generation);

    /// Cloud Ready 诊断查询
    SubscriptionReadiness getSubscriptionReadiness();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace mqtt
} // namespace tbox
