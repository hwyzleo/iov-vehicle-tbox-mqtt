#include "mqtt_dispatch_queue.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"

namespace tbox {
namespace mqtt {

MqttDispatchQueue::MqttDispatchQueue(size_t max_size, PushCallback push_cb)
    : max_size_(max_size == 0 ? 1 : max_size),
      push_cb_(std::move(push_cb)) {
}

MqttDispatchQueue::~MqttDispatchQueue() {
    stop();
}

void MqttDispatchQueue::start() {
    if (running_.exchange(true)) {
        return;  // already running
    }
    worker_ = std::thread([this]() {
        while (running_.load() || true) {
            Item item;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]() {
                    return !queue_.empty() || !running_.load();
                });
                if (queue_.empty() && !running_.load()) {
                    break;
                }
                if (!queue_.empty()) {
                    item = std::move(queue_.front());
                    queue_.pop();
                } else {
                    continue;
                }
            }
            try {
                if (push_cb_) {
                    push_cb_(item.topic, item.payload, item.route_event);
                }
            } catch (const std::exception& e) {
                MqttLogAdapter::ipc().error(
                    "mqtt.ipc.dispatch_push_failed",
                    "Dispatch push callback exception",
                    {{"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(item.topic))},
                     {"what", tbox::fw::log::FieldValue::makeString(e.what())}}
                );
            }
        }
    });
}

void MqttDispatchQueue::stop() {
    if (!running_.exchange(false)) {
        return;  // not running
    }
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
}

bool MqttDispatchQueue::enqueue(const std::string& topic,
                                const std::vector<uint8_t>& payload) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= max_size_) {
            dropped_.fetch_add(1);
            // 队列满：记录 MQTT-1007 并丢弃 (CR-004 §6)
            MqttLogAdapter::router().warn(
                "mqtt.downlink.route_failed",
                "分发队列已满，下行消息被丢弃",
                {{"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))},
                 {"payload_size", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(payload.size()))},
                 {"queue_depth", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(queue_.size()))},
                 {"queue_capacity", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(max_size_))},
                 {"reason", tbox::fw::log::FieldValue::makeString("dispatch_queue_full")},
                 {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1007")}}
            );
            return false;
        }
        queue_.push(Item{topic, payload, std::nullopt});
    }
    cv_.notify_one();
    return true;
}

bool MqttDispatchQueue::enqueueRouted(const RoutedDownlinkEnvelope& event) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= max_size_) {
            dropped_.fetch_add(1);
            // CR-015 §18.6: capability 下行背压使用稳定 reason，且不写磁盘重试队列。
            const bool capability =
                !event.readiness_scope.empty();
            MqttLogAdapter::router().warn(
                "mqtt.downlink.route_failed",
                capability
                    ? "分发队列已满，capability 下行消息被拒绝（不落盘）"
                    : "分发队列已满，route-based 下行消息被丢弃",
                {{"route_id_hash", tbox::fw::log::FieldValue::makeString(
                     topicTemplate(event.key.route_id))},
                 {"payload_size", tbox::fw::log::FieldValue::makeInt(
                     static_cast<int64_t>(event.payload.size()))},
                 {"queue_depth", tbox::fw::log::FieldValue::makeInt(
                     static_cast<int64_t>(queue_.size()))},
                 {"queue_capacity", tbox::fw::log::FieldValue::makeInt(
                     static_cast<int64_t>(max_size_))},
                 {"reason", tbox::fw::log::FieldValue::makeString(
                     capability ? capability_reason::kKeyprovDownlinkBackpressure
                                : "dispatch_queue_full")},
                 {"reason_code", tbox::fw::log::FieldValue::makeString(
                     capability ? capability_reason::kKeyprovDownlinkBackpressure
                                : "dispatch_queue_full")},
                 {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1007")}}
            );
            return false;
        }
        Item item;
        item.topic = "";  // route-based 不携带完整 topic
        item.payload = event.payload;
        item.route_event = event;
        queue_.push(std::move(item));
    }
    cv_.notify_one();
    return true;
}

size_t MqttDispatchQueue::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

uint64_t MqttDispatchQueue::dropped_count() const {
    return dropped_.load();
}

} // namespace mqtt
} // namespace tbox
