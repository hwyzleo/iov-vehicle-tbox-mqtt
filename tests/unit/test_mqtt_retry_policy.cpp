// CR-004 §10: Retry policy 单元测试
#include <gtest/gtest.h>
#include "mqtt_retry_policy.h"

using namespace tbox::mqtt;

TEST(MqttRetryPolicyTest, ReadOnlyMethodsAllowRetry) {
    EXPECT_TRUE(MqttRetryPolicy::should_retry(
        static_cast<uint32_t>(ipc::MethodId::GET_CONNECTION_STATE)));
    EXPECT_TRUE(MqttRetryPolicy::should_retry(
        static_cast<uint32_t>(ipc::MethodId::REGISTER_ROUTE)));
}

TEST(MqttRetryPolicyTest, PublishAllowsRetryForDedup) {
    // publish 允许一次重试（Server 端按 msg_id 去重, CR-004 §5.2）
    EXPECT_TRUE(MqttRetryPolicy::should_retry(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH)));
}

TEST(MqttRetryPolicyTest, OneShotMethodsForbidRetry) {
    // subscribe/unsubscribe 一次性，禁止自动重放
    EXPECT_FALSE(MqttRetryPolicy::should_retry(
        static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE)));
    EXPECT_FALSE(MqttRetryPolicy::should_retry(
        static_cast<uint32_t>(ipc::MethodId::UNSUBSCRIBE)));
}

TEST(MqttRetryPolicyTest, UnknownMethodForbidsRetry) {
    EXPECT_FALSE(MqttRetryPolicy::should_retry(9999));
}

TEST(MqttRetryPolicyTest, Categorize) {
    EXPECT_EQ(MqttRetryPolicy::categorize(
        static_cast<uint32_t>(ipc::MethodId::GET_CONNECTION_STATE)),
        MqttRetryPolicy::Category::kReadOnlyIdempotent);
    EXPECT_EQ(MqttRetryPolicy::categorize(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH)),
        MqttRetryPolicy::Category::kPublishDedupable);
    EXPECT_EQ(MqttRetryPolicy::categorize(
        static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE)),
        MqttRetryPolicy::Category::kOneShot);
}
