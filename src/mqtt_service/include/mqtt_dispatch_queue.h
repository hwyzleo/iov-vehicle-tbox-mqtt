#pragma once

#include "mqtt_types.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace tbox {
namespace mqtt {

/// 有界下行分发队列 (CR-004 §4, §6)
///
/// 将 Broker 网络 callback 与 framework-ipc push_event 解耦：
/// - Broker callback 调用 enqueue()（非阻塞，有界），立即返回，不阻塞网络线程。
/// - 独立 worker 线程按 FIFO 出队调用 push 回调（Server::push_event）。
/// - 队列满时按策略丢弃并记录 MQTT-1007，不无限积压。
class MqttDispatchQueue {
public:
    using PushCallback = std::function<void(const std::string& topic,
                                            const std::vector<uint8_t>& payload,
                                            const std::optional<RoutedDownlinkEnvelope>& route_event)>;;

    /// @param max_size    队列容量（dispatch_queue_size）
    /// @param push_cb     出队时调用的推送回调
    MqttDispatchQueue(size_t max_size, PushCallback push_cb);
    ~MqttDispatchQueue();

    MqttDispatchQueue(const MqttDispatchQueue&) = delete;
    MqttDispatchQueue& operator=(const MqttDispatchQueue&) = delete;

    /// 启动 worker 线程
    void start();
    /// 停止 worker 线程（flush 剩余项后退出）
    void stop();

    /// 入队下行消息（非阻塞，有界）
    /// @return true 入队成功；false 队列满已丢弃
    bool enqueue(const std::string& topic, const std::vector<uint8_t>& payload);

    /// CR-009 §7: 入队 route-based 下行事件（非阻塞，有界）
    /// @return true 入队成功；false 队列满已丢弃
    bool enqueueRouted(const RoutedDownlinkEnvelope& event);

    /// 当前队列深度
    size_t size() const;

    /// 累计丢弃数（用于测试/指标）
    uint64_t dropped_count() const;

private:
    struct Item {
        std::string topic;
        std::vector<uint8_t> payload;
        std::optional<RoutedDownlinkEnvelope> route_event;  // CR-009 §7
    };

    const size_t max_size_;
    PushCallback push_cb_;
    std::queue<Item> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread worker_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> dropped_{0};
};

} // namespace mqtt
} // namespace tbox
