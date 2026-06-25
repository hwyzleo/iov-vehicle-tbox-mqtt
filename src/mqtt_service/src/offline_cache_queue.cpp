#include "offline_cache_queue.h"
#include <algorithm>

namespace tbox {
namespace mqtt {

OfflineCacheQueue::OfflineCacheQueue(size_t max_size)
    : max_size_(max_size) {
}

OfflineCacheQueue::~OfflineCacheQueue() {
}

bool OfflineCacheQueue::enqueue(const OutboundMessage& message) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if queue is full
    if (queue_.size() >= max_size_) {
        // Drop strategy: drop oldest or lowest priority
        dropOldestMessage();
    }

    // Add to queue and map
    queue_.push(message);
    message_map_[message.msg_id] = message;

    return true;
}

std::optional<OutboundMessage> OfflineCacheQueue::dequeue() {
    std::lock_guard<std::mutex> lock(mutex_);

    while (!queue_.empty()) {
        auto message = queue_.top();
        queue_.pop();

        // Check if message still exists in map (may have been updated)
        auto it = message_map_.find(message.msg_id);
        if (it != message_map_.end()) {
            // Get current state from map (single source of truth)
            auto current_message = it->second;
            message_map_.erase(it);
            return current_message;
        }
        // Message was already removed (e.g., dropped), skip it
    }

    return std::nullopt;
}

size_t OfflineCacheQueue::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

bool OfflineCacheQueue::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

OutboundMessage OfflineCacheQueue::getMessage(const std::string& msg_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = message_map_.find(msg_id);
    if (it != message_map_.end()) {
        return it->second;
    }
    return OutboundMessage{};
}

void OfflineCacheQueue::updateMessageState(const std::string& msg_id,
                                          MessageState new_state) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = message_map_.find(msg_id);
    if (it != message_map_.end()) {
        it->second.state = new_state;
    }
}

void OfflineCacheQueue::clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    while (!queue_.empty()) {
        queue_.pop();
    }
    message_map_.clear();
}

std::vector<OutboundMessage> OfflineCacheQueue::getMessagesByState(MessageState state) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<OutboundMessage> result;
    for (const auto& pair : message_map_) {
        if (pair.second.state == state) {
            result.push_back(pair.second);
        }
    }
    return result;
}

OfflineCacheQueue::QueueStats OfflineCacheQueue::getStats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    QueueStats stats = {0, 0, 0, 0, 0};
    stats.total_count = message_map_.size();

    for (const auto& pair : message_map_) {
        switch (pair.second.state) {
            case MessageState::PENDING:
                stats.pending_count++;
                break;
            case MessageState::INFLIGHT:
                stats.inflight_count++;
                break;
            case MessageState::ACKED:
                stats.acked_count++;
                break;
            case MessageState::DROPPED:
                stats.dropped_count++;
                break;
        }
    }

    return stats;
}

void OfflineCacheQueue::dropOldestMessage() {
    if (queue_.empty()) return;

    // Find oldest message (queue_.top() returns highest priority, oldest first)
    while (!queue_.empty()) {
        auto oldest_msg = queue_.top();
        queue_.pop();

        // Check if message still exists in map
        auto it = message_map_.find(oldest_msg.msg_id);
        if (it != message_map_.end()) {
            message_map_.erase(it);
            return;
        }
        // Message was already removed, try next
    }
}

void OfflineCacheQueue::dropLowestPriorityMessage() {
    if (queue_.empty()) return;

    // Find lowest priority message
    auto lowest_priority = Priority::CRITICAL;
    std::string lowest_priority_id;

    for (const auto& pair : message_map_) {
        if (pair.second.priority < lowest_priority) {
            lowest_priority = pair.second.priority;
            lowest_priority_id = pair.second.msg_id;
        }
    }

    if (!lowest_priority_id.empty()) {
        message_map_.erase(lowest_priority_id);
        // Rebuild queue (simplified approach)
        std::vector<OutboundMessage> messages;
        while (!queue_.empty()) {
            auto msg = queue_.top();
            queue_.pop();
            // Only keep messages that are still in the map
            if (message_map_.find(msg.msg_id) != message_map_.end()) {
                messages.push_back(msg);
            }
        }
        for (const auto& msg : messages) {
            queue_.push(msg);
        }
    }
}

} // namespace mqtt
} // namespace tbox
