#pragma once

#include "mqtt_types.h"
#include <queue>
#include <map>
#include <mutex>
#include <chrono>

namespace tbox {
namespace mqtt {

// Priority comparator for message queue
struct MessageComparator {
    bool operator()(const OutboundMessage& a, const OutboundMessage& b) const {
        if (a.priority != b.priority) {
            return a.priority < b.priority;  // Higher priority first
        }
        return a.created_at < b.created_at;  // Newer messages first within same priority
    }
};

// Offline cache queue (Design §2, §3)
class OfflineCacheQueue {
public:
    explicit OfflineCacheQueue(size_t max_size);
    ~OfflineCacheQueue();

    // Enqueue message for offline delivery
    bool enqueue(const OutboundMessage& message);

    // Dequeue highest priority message
    OutboundMessage dequeue();

    // Get queue size
    size_t size() const;

    // Check if queue is empty
    bool empty() const;

    // Get message by ID
    OutboundMessage getMessage(const std::string& msg_id) const;

    // Update message state
    void updateMessageState(const std::string& msg_id, MessageState new_state);

    // Clear all messages
    void clear();

    // Get messages by state
    std::vector<OutboundMessage> getMessagesByState(MessageState state) const;

    // Get queue statistics
    struct QueueStats {
        size_t total_count;
        size_t pending_count;
        size_t inflight_count;
        size_t acked_count;
        size_t dropped_count;
    };
    QueueStats getStats() const;

private:
    void dropOldestMessage();
    void dropLowestPriorityMessage();

    size_t max_size_;
    std::priority_queue<OutboundMessage,
                       std::vector<OutboundMessage>,
                       MessageComparator> queue_;
    std::map<std::string, OutboundMessage> message_map_;
    mutable std::mutex mutex_;
};

} // namespace mqtt
} // namespace tbox
