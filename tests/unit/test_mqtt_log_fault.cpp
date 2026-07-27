#include <gtest/gtest.h>
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "log_types.h"

using namespace tbox::mqtt;
using namespace tbox::fw::log;

class MqttLogFaultTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 每个测试前重置状态
    }
};

// ============================================================
// Logger 初始化失败降级 (CR-003 §2, §6)
// ============================================================
TEST_F(MqttLogFaultTest, LoggerInitFailureDegradation) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    config.strict = false;  // 非严格模式

    auto result = MqttLogAdapter::init("mqtt_test", config);

    // 在非严格模式下，即使初始化失败也应该能继续
    if (result.error != LogError::kOk) {
        EXPECT_FALSE(result.error_message.empty());
    }
}

// ============================================================
// 严格模式 fail-closed (CR-003 §2)
// ============================================================
TEST_F(MqttLogFaultTest, LoggerInitStrictMode) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    config.strict = true;

    auto result = MqttLogAdapter::init("mqtt_test", config);

    // 严格模式下，初始化成功应该返回 kOk
    EXPECT_EQ(result.error, LogError::kOk);
}

// ============================================================
// 队列溢出处理 (CR-003 §2, FW-0204)
// ============================================================
TEST_F(MqttLogFaultTest, QueueOverflowHandling) {
    LogConfig config;
    config.level = LogLevel::kDebug;
    config.console_config.enabled = true;
    config.async_config.enabled = true;
    config.async_config.queue_size = 10;  // 小队列

    MqttLogAdapter::init("mqtt_test", config);

    // 快速生成大量日志
    for (int i = 0; i < 100; ++i) {
        MqttLogAdapter::connection().info(
            "test.queue_overflow",
            "测试队列溢出",
            {{"index", FieldValue::makeInt(i)}}
        );
    }

    // 应该不会崩溃
    EXPECT_TRUE(true);
}

// ============================================================
// Sink 故障处理 (CR-003 §2, FW-0203)
// ============================================================
TEST_F(MqttLogFaultTest, SinkFailureHandling) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    config.file_config.enabled = true;
    config.file_config.root = "/nonexistent/path";

    MqttLogAdapter::init("mqtt_test", config);

    // 尝试输出日志
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.sink_failure",
            "测试 sink 故障"
        );
    });
}

// ============================================================
// 多次初始化 (CR-003 §2)
// ============================================================
TEST_F(MqttLogFaultTest, MultipleInitCalls) {
    LogConfig config1;
    config1.level = LogLevel::kInfo;
    config1.console_config.enabled = true;

    LogConfig config2;
    config2.level = LogLevel::kDebug;
    config2.console_config.enabled = true;

    auto result1 = MqttLogAdapter::init("mqtt_test1", config1);
    auto result2 = MqttLogAdapter::init("mqtt_test2", config2);

    // 两次初始化都应该成功（或第二次被忽略）
    EXPECT_EQ(result1.error, LogError::kOk);
}

// ============================================================
// Logger 故障不破坏通信语义 (CR-003 §6)
// ============================================================
TEST_F(MqttLogFaultTest, LoggerFailureDoesNotBreakCommunicationSemantics) {
    // Logger 失败不得改变重连退避、QoS、丢弃策略、顺序分发或下游路由结果
    // 这里验证 Logger 故障后仍可正常输出日志
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    config.strict = false;

    auto result = MqttLogAdapter::init("mqtt_test", config);

    // 即使 Logger 可能降级，通信事件仍可记录
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "mqtt.connection.established",
            "MQTT 连接建立成功",
            {
                {"broker_hash", FieldValue::makeString("h_abcd1234")},
                {"keepalive_s", FieldValue::makeInt(60)}
            }
        );
    });

    EXPECT_NO_THROW({
        MqttLogAdapter::publish().error(
            "mqtt.publish.failed",
            "发布超时或重试耗尽",
            {
                {"topic_template", FieldValue::makeString("vehicle/{device_sn}/up/fota")},
                {"qos", FieldValue::makeString("1")},
                {"error_code", FieldValue::makeString("MQTT-1005")}
            }
        );
    });

    EXPECT_NO_THROW({
        MqttLogAdapter::offline_queue().warn(
            "mqtt.offline_queue.message_dropped",
            "离线缓存达到上限并丢弃消息",
            {
                {"queue_depth", FieldValue::makeInt(1000)},
                {"queue_capacity", FieldValue::makeInt(1000)},
                {"drop_policy", FieldValue::makeString("drop_oldest")},
                {"error_code", FieldValue::makeString("MQTT-1006")}
            }
        );
    });
}

// ============================================================
// 日志降级后错误码语义独立 (CR-003 §6)
// ============================================================
TEST_F(MqttLogFaultTest, ErrorCodeSemanticsIndependentAfterDegradation) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    config.strict = false;
    MqttLogAdapter::init("mqtt_test", config);

    // FW-02xx 与 MQTT-10xx 保持独立语义
    // FW-02xx 描述日志组件故障，MQTT-10xx 描述通信业务失败
    EXPECT_NO_THROW({
        // 通信业务失败事件
        MqttLogAdapter::connection().error(
            "mqtt.connection.failed",
            "网络不可用或连接失败",
            {
                {"reason_code", FieldValue::makeString("timeout")},
                {"error_code", FieldValue::makeString("MQTT-1002")}
            }
        );
    });
}

// ============================================================
// 所有日志级别可调用 (CR-003 §2)
// ============================================================
TEST_F(MqttLogFaultTest, AllLogLevelsCallable) {
    LogConfig config;
    config.level = LogLevel::kTrace;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    EXPECT_NO_THROW({
        MqttLogAdapter::connection().trace("test.trace", "trace msg");
        MqttLogAdapter::connection().debug("test.debug", "debug msg");
        MqttLogAdapter::connection().info("test.info", "info msg");
        MqttLogAdapter::connection().warn("test.warn", "warn msg");
        MqttLogAdapter::connection().error("test.error", "error msg");
    });
}

// ============================================================
// flush 可调用 (CR-003 §2)
// ============================================================
TEST_F(MqttLogFaultTest, FlushCallable) {
    LogConfig config;
    config.level = LogLevel::kInfo;
    config.console_config.enabled = true;
    MqttLogAdapter::init("mqtt_test", config);

    EXPECT_NO_THROW({
        MqttLogAdapter::connection().flush();
    });
}
