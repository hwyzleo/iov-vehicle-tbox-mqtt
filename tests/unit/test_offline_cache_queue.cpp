#include <gtest/gtest.h>
#include "offline_cache_queue.h"

using namespace tbox::mqtt;

TEST(OfflineCacheQueueTest, QueueInitialization) {
    OfflineCacheQueue queue(100);  // max 100 messages
    EXPECT_EQ(queue.size(), 0);
    EXPECT_TRUE(queue.empty());
}

TEST(OfflineCacheQueueTest, EnqueueMessage) {
    OfflineCacheQueue queue(100);

    OutboundMessage msg;
    msg.msg_id = "test_001";
    msg.topic = "test/topic";
    msg.qos = QoS::AT_LEAST_ONCE;
    msg.priority = Priority::NORMAL;

    EXPECT_TRUE(queue.enqueue(msg));
    EXPECT_EQ(queue.size(), 1);
}

TEST(OfflineCacheQueueTest, DequeueByPriority) {
    OfflineCacheQueue queue(100);

    OutboundMessage low_msg;
    low_msg.msg_id = "low_001";
    low_msg.priority = Priority::LOW;

    OutboundMessage high_msg;
    high_msg.msg_id = "high_001";
    high_msg.priority = Priority::HIGH;

    queue.enqueue(low_msg);
    queue.enqueue(high_msg);

    auto msg = queue.dequeue();
    ASSERT_TRUE(msg.has_value());
    EXPECT_EQ(msg->msg_id, "high_001");
}

TEST(OfflineCacheQueueTest, DropOldestWhenFull) {
    OfflineCacheQueue queue(2);  // max 2 messages

    OutboundMessage msg1;
    msg1.msg_id = "msg_001";
    msg1.created_at = std::chrono::steady_clock::now() - std::chrono::seconds(10);

    OutboundMessage msg2;
    msg2.msg_id = "msg_002";
    msg2.created_at = std::chrono::steady_clock::now() - std::chrono::seconds(5);

    OutboundMessage msg3;
    msg3.msg_id = "msg_003";
    msg3.created_at = std::chrono::steady_clock::now();

    queue.enqueue(msg1);
    queue.enqueue(msg2);
    EXPECT_TRUE(queue.enqueue(msg3));

    // Oldest should be dropped
    EXPECT_EQ(queue.size(), 2);
    
    // Verify msg1 (oldest) is no longer retrievable
    auto dropped_msg = queue.getMessage("msg_001");
    EXPECT_EQ(dropped_msg.msg_id, "");  // Should be default-constructed (not found)
    
    // Verify msg2 and msg3 are still in queue
    auto msg2_retrieved = queue.getMessage("msg_002");
    EXPECT_EQ(msg2_retrieved.msg_id, "msg_002");
    
    auto msg3_retrieved = queue.getMessage("msg_003");
    EXPECT_EQ(msg3_retrieved.msg_id, "msg_003");
    
    // Dequeue should return highest priority (both have same priority, so oldest first)
    auto dequeued = queue.dequeue();
    ASSERT_TRUE(dequeued.has_value());
    EXPECT_EQ(dequeued->msg_id, "msg_002");  // msg2 is older than msg3
}

TEST(OfflineCacheQueueTest, MessageStateTracking) {
    OfflineCacheQueue queue(100);

    OutboundMessage msg;
    msg.msg_id = "test_001";
    msg.state = MessageState::PENDING;

    queue.enqueue(msg);

    queue.updateMessageState("test_001", MessageState::INFLIGHT);
    auto retrieved = queue.getMessage("test_001");
    EXPECT_EQ(retrieved.state, MessageState::INFLIGHT);
}

TEST(OfflineCacheQueueTest, ClearQueue) {
    OfflineCacheQueue queue(100);

    OutboundMessage msg;
    msg.msg_id = "test_001";
    queue.enqueue(msg);

    queue.clear();
    EXPECT_EQ(queue.size(), 0);
}

TEST(OfflineCacheQueueTest, DequeueEmptyQueue) {
    OfflineCacheQueue queue(100);
    
    auto msg = queue.dequeue();
    EXPECT_FALSE(msg.has_value());
}
