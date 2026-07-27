#include <gtest/gtest.h>
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "log_types.h"

using namespace tbox::mqtt;
using namespace tbox::fw::log;

class MqttLogContextTest : public ::testing::Test {
protected:
    void SetUp() override {
        LogConfig config;
        config.level = LogLevel::kDebug;
        config.console_config.enabled = true;
        MqttLogAdapter::init("mqtt_test", config);
    }
};

// ============================================================
// ContextScope 创建 (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, ContextScopeCreation) {
    LogContext context;
    context.trace_id = "trace-123";
    context.request_id = "req-456";
    context.session_id = "sess-789";

    {
        ContextScope scope(context);

        const LogContext* current = ContextScope::current();
        ASSERT_NE(current, nullptr);
        EXPECT_EQ(current->trace_id, "trace-123");
        EXPECT_EQ(current->request_id, "req-456");
        EXPECT_EQ(current->session_id, "sess-789");
    }
}

// ============================================================
// ContextScope 嵌套 (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, ContextScopeNested) {
    LogContext outer_context;
    outer_context.trace_id = "outer-trace";
    outer_context.request_id = "outer-req";

    LogContext inner_context;
    inner_context.trace_id = "inner-trace";
    inner_context.request_id = "inner-req";

    {
        ContextScope outer_scope(outer_context);

        const LogContext* current = ContextScope::current();
        EXPECT_EQ(current->trace_id, "outer-trace");

        {
            ContextScope inner_scope(inner_context);

            current = ContextScope::current();
            EXPECT_EQ(current->trace_id, "inner-trace");
        }

        // 退出内部作用域后恢复外部上下文
        current = ContextScope::current();
        EXPECT_EQ(current->trace_id, "outer-trace");
    }
}

// ============================================================
// ContextScope 空值 (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, ContextScopeEmpty) {
    LogContext context;
    // 不设置任何 ID

    {
        ContextScope scope(context);

        const LogContext* current = ContextScope::current();
        ASSERT_NE(current, nullptr);
        EXPECT_TRUE(current->trace_id.empty());
        EXPECT_TRUE(current->request_id.empty());
        EXPECT_TRUE(current->session_id.empty());
    }
}

// ============================================================
// ContextScope 无作用域时 current 返回 null (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, ContextScopeNullWhenNone) {
    const LogContext* current = ContextScope::current();
    EXPECT_EQ(current, nullptr);
}

// ============================================================
// 带上下文的日志输出 (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, LogWithContext) {
    LogContext context;
    context.trace_id = "trace-123";
    context.request_id = "req-456";
    context.session_id = "sess-789";

    {
        ContextScope scope(context);

        EXPECT_NO_THROW({
            MqttLogAdapter::publish().info(
                "mqtt.test.with_context",
                "带上下文的日志",
                {{"test_field", FieldValue::makeString("value")}}
            );
        });
    }
}

// ============================================================
// makeContextScope 便捷函数 (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, MakeContextScopeHelper) {
    std::string trace_id = "trace-abc";
    std::string request_id = "req-def";

    {
        auto scope = makeContextScope(trace_id, request_id);

        const LogContext* current = ContextScope::current();
        ASSERT_NE(current, nullptr);
        EXPECT_EQ(current->trace_id, "trace-abc");
        EXPECT_EQ(current->request_id, "req-def");
    }

    // scope 离开作用域后上下文已清理
    const LogContext* after = ContextScope::current();
    EXPECT_EQ(after, nullptr);
}

// ============================================================
// generateRequestId 生成唯一 ID (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, GenerateRequestId) {
    std::string id1 = generateRequestId();
    std::string id2 = generateRequestId();

    // 两次生成的 ID 应不同
    EXPECT_NE(id1, id2);

    // ID 应以 "mqtt-" 开头
    EXPECT_EQ(id1.substr(0, 5), "mqtt-");
    EXPECT_EQ(id2.substr(0, 5), "mqtt-");
}

// ============================================================
// 上下文贯穿发布链路 (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, ContextPropagationDuringPublish) {
    std::string trace_id = "publish-trace-123";
    std::string request_id = "publish-req-456";
    std::string msg_id = "msg-789";

    {
        auto scope = makeContextScope(trace_id, request_id);

        // 模拟发布链路中的日志
        EXPECT_NO_THROW({
            MqttLogAdapter::publish().info(
                "mqtt.publish.accepted",
                "消息受理",
                {{"msg_id_hash", FieldValue::makeString(msgIdHash(msg_id))}}
            );
        });

        // 验证上下文在操作期间被设置
        const LogContext* current = ContextScope::current();
        ASSERT_NE(current, nullptr);
        EXPECT_EQ(current->trace_id, trace_id);
        EXPECT_EQ(current->request_id, request_id);
    }

    // 上下文已清理
    const LogContext* after = ContextScope::current();
    EXPECT_EQ(after, nullptr);
}

// ============================================================
// 下行消息生成 request_id (CR-003 §3)
// ============================================================
TEST_F(MqttLogContextTest, DownlinkRequestIdGeneration) {
    // 下行消息进入时生成 request_id
    std::string request_id = generateRequestId();

    {
        auto scope = makeContextScope("", request_id);

        EXPECT_NO_THROW({
            MqttLogAdapter::router().info(
                "mqtt.downlink.received",
                "下行消息接收",
                {{"topic_template", FieldValue::makeString("vehicle/{device_sn}/down/fota")}}
            );
        });

        const LogContext* current = ContextScope::current();
        ASSERT_NE(current, nullptr);
        EXPECT_EQ(current->request_id, request_id);
        // trace_id 为空（下行消息无上游 trace_id）
        EXPECT_TRUE(current->trace_id.empty());
    }
}
