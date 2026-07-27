#include <gtest/gtest.h>
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "mqtt_types.h"
#include "log_types.h"
#include <regex>

using namespace tbox::mqtt;
using namespace tbox::fw::log;

class MqttLogEventsTest : public ::testing::Test {
protected:
    void SetUp() override {
        LogConfig config;
        config.level = LogLevel::kDebug;
        config.console_config.enabled = true;
        MqttLogAdapter::init("mqtt_test", config);
    }
};

// ============================================================
// 事件名格式校验 (CR-003 §4: 事件名是稳定检索契约)
// ============================================================
TEST_F(MqttLogEventsTest, EventNameFormat) {
    // 事件名格式: mqtt.<module>.<action> 或 mqtt.<module>.<submodule>.<action>
    std::regex event_pattern("^mqtt\\.[a-z_]+\\.[a-z_.]+$");

    // CR-003 §4 业务事件清单中的所有事件
    EXPECT_TRUE(std::regex_match("mqtt.certificate.waiting", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.tls.handshake_failed", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.connection.established", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.connection.failed", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.connection.disconnected", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.publish.rejected", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.publish.acknowledged", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.publish.failed", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.offline_queue.message_dropped", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.downlink.route_failed", event_pattern));
    EXPECT_TRUE(std::regex_match("mqtt.subscription.failed", event_pattern));
}

// ============================================================
// CR-003 §4 业务事件清单 - 全部 11 个事件
// ============================================================

// mqtt.certificate.waiting (INFO, MQTT-1001)
TEST_F(MqttLogEventsTest, CertificateWaiting) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "mqtt.certificate.waiting",
            "SEC 证书尚未就绪，连接保持等待",
            {
                {"state", FieldValue::makeString("CERT_WAIT")},
                {"retry_count", FieldValue::makeInt(0)},
                {"next_retry_ms", FieldValue::makeInt(5000)},
                {"error_code", FieldValue::makeString("MQTT-1001")}
            }
        );
    });
}

// mqtt.tls.handshake_failed (ERROR, MQTT-1001)
TEST_F(MqttLogEventsTest, TlsHandshakeFailed) {
    EXPECT_NO_THROW({
        MqttLogAdapter::tls().error(
            "mqtt.tls.handshake_failed",
            "证书/密钥不可用或 mTLS 握手失败",
            {
                {"broker_hash", FieldValue::makeString("h_abcd1234")},
                {"tls_version", FieldValue::makeString("TLSv1.3")},
                {"reason_code", FieldValue::makeString("handshake_failure")},
                {"duration_ms", FieldValue::makeInt(150)},
                {"error_code", FieldValue::makeString("MQTT-1001")}
            }
        );
    });
}

// mqtt.connection.established (INFO)
TEST_F(MqttLogEventsTest, ConnectionEstablished) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "mqtt.connection.established",
            "MQTT CONNECT/CONNACK 成功",
            {
                {"broker_hash", FieldValue::makeString("h_abcd1234")},
                {"session_present", FieldValue::makeBool(false)},
                {"keepalive_s", FieldValue::makeInt(60)},
                {"duration_ms", FieldValue::makeInt(200)}
            }
        );
    });
}

// mqtt.connection.failed (ERROR, MQTT-1002)
TEST_F(MqttLogEventsTest, ConnectionFailed) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().error(
            "mqtt.connection.failed",
            "网络不可用或连接失败",
            {
                {"broker_hash", FieldValue::makeString("h_abcd1234")},
                {"reason_code", FieldValue::makeString("connection_refused")},
                {"retry_count", FieldValue::makeInt(3)},
                {"duration_ms", FieldValue::makeInt(5000)},
                {"error_code", FieldValue::makeString("MQTT-1002")}
            }
        );
    });
}

// mqtt.connection.disconnected (WARN, MQTT-1003)
TEST_F(MqttLogEventsTest, ConnectionDisconnected) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().warn(
            "mqtt.connection.disconnected",
            "连接异常断开",
            {
                {"reason_code", FieldValue::makeString("unexpected_disconnect")},
                {"retry_count", FieldValue::makeInt(1)},
                {"next_retry_ms", FieldValue::makeInt(2000)},
                {"error_code", FieldValue::makeString("MQTT-1003")}
            }
        );
    });
}

// mqtt.publish.rejected (WARN, MQTT-1004)
TEST_F(MqttLogEventsTest, PublishRejected) {
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().warn(
            "mqtt.publish.rejected",
            "topic 或消息格式非法",
            {
                {"topic_template", FieldValue::makeString("vehicle/{device_sn}/up/fota")},
                {"qos", FieldValue::makeString("1")},
                {"payload_size", FieldValue::makeInt(256)},
                {"validation_reason", FieldValue::makeString("empty_topic")},
                {"error_code", FieldValue::makeString("MQTT-1004")}
            }
        );
    });
}

// mqtt.publish.acknowledged (DEBUG)
TEST_F(MqttLogEventsTest, PublishAcknowledged) {
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().debug(
            "mqtt.publish.acknowledged",
            "QoS1 PUBACK 成功",
            {
                {"msg_id_hash", FieldValue::makeString("m_abcd1234")},
                {"topic_template", FieldValue::makeString("vehicle/{device_sn}/up/fota")},
                {"qos", FieldValue::makeString("1")},
                {"retry_count", FieldValue::makeInt(0)},
                {"duration_ms", FieldValue::makeInt(50)}
            }
        );
    });
}

// mqtt.publish.failed (ERROR, MQTT-1005)
TEST_F(MqttLogEventsTest, PublishFailed) {
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().error(
            "mqtt.publish.failed",
            "发布超时或重试耗尽",
            {
                {"msg_id_hash", FieldValue::makeString("m_abcd1234")},
                {"topic_template", FieldValue::makeString("vehicle/{device_sn}/up/fota")},
                {"qos", FieldValue::makeString("1")},
                {"retry_count", FieldValue::makeInt(3)},
                {"duration_ms", FieldValue::makeInt(30000)},
                {"error_code", FieldValue::makeString("MQTT-1005")}
            }
        );
    });
}

// mqtt.offline_queue.message_dropped (WARN, MQTT-1006)
TEST_F(MqttLogEventsTest, OfflineQueueMessageDropped) {
    EXPECT_NO_THROW({
        MqttLogAdapter::offline_queue().warn(
            "mqtt.offline_queue.message_dropped",
            "离线缓存达到上限并丢弃消息",
            {
                {"msg_id_hash", FieldValue::makeString("m_abcd1234")},
                {"priority", FieldValue::makeString("NORMAL")},
                {"queue_depth", FieldValue::makeInt(1000)},
                {"queue_capacity", FieldValue::makeInt(1000)},
                {"drop_policy", FieldValue::makeString("drop_oldest")},
                {"error_code", FieldValue::makeString("MQTT-1006")}
            }
        );
    });
}

// mqtt.downlink.route_failed (WARN, MQTT-1007)
TEST_F(MqttLogEventsTest, DownlinkRouteFailed) {
    EXPECT_NO_THROW({
        MqttLogAdapter::router().warn(
            "mqtt.downlink.route_failed",
            "下行消息无法路由或无订阅者",
            {
                {"topic_template", FieldValue::makeString("vehicle/{device_sn}/down/fota")},
                {"payload_size", FieldValue::makeInt(128)},
                {"reason", FieldValue::makeString("no_route")},
                {"error_code", FieldValue::makeString("MQTT-1007")}
            }
        );
    });
}

// mqtt.subscription.failed (ERROR, MQTT-1008)
TEST_F(MqttLogEventsTest, SubscriptionFailed) {
    EXPECT_NO_THROW({
        MqttLogAdapter::subscribe().error(
            "mqtt.subscription.failed",
            "订阅请求失败",
            {
                {"topic_template", FieldValue::makeString("vehicle/{device_sn}/down/fota")},
                {"qos", FieldValue::makeString("1")},
                {"reason_code", FieldValue::makeString("suback_failure")},
                {"duration_ms", FieldValue::makeInt(100)},
                {"error_code", FieldValue::makeString("MQTT-1008")}
            }
        );
    });
}

// ============================================================
// 字段类型验证
// ============================================================
TEST_F(MqttLogEventsTest, FieldTypesWork) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.field_types",
            "Test field types",
            {
                {"string_field", FieldValue::makeString("value")},
                {"int_field", FieldValue::makeInt(42)},
                {"bool_field", FieldValue::makeBool(true)},
                {"double_field", FieldValue::makeDouble(3.14)}
            }
        );
    });
}

// ============================================================
// 错误码边界验证 (CR-003 §6)
// ============================================================
TEST_F(MqttLogEventsTest, ErrorCodeBoundary) {
    // MQTT 业务错误码在 1001-1008 范围内
    EXPECT_GE(static_cast<int>(ErrorCode::MQTT_1001), 1001);
    EXPECT_LE(static_cast<int>(ErrorCode::MQTT_1001), 1008);
    EXPECT_GE(static_cast<int>(ErrorCode::MQTT_1008), 1001);
    EXPECT_LE(static_cast<int>(ErrorCode::MQTT_1008), 1008);

    // 验证 errorCodeToString 输出
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1001), "MQTT-1001");
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1002), "MQTT-1002");
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1003), "MQTT-1003");
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1004), "MQTT-1004");
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1005), "MQTT-1005");
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1006), "MQTT-1006");
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1007), "MQTT-1007");
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1008), "MQTT-1008");
}

// ============================================================
// FW-02xx 与 MQTT-10xx 语义独立 (CR-003 §6)
// ============================================================
TEST_F(MqttLogEventsTest, FwErrorCodesIndependentFromMqtt) {
    // FW-02xx 描述日志组件自身故障
    EXPECT_EQ(static_cast<uint32_t>(LogError::kConfigInvalid), 201);
    EXPECT_EQ(static_cast<uint32_t>(LogError::kInitFailed), 202);
    EXPECT_EQ(static_cast<uint32_t>(LogError::kSinkFailed), 203);
    EXPECT_EQ(static_cast<uint32_t>(LogError::kQueueOverflow), 204);
    EXPECT_EQ(static_cast<uint32_t>(LogError::kSensitiveViolation), 205);

    // MQTT-10xx 描述 MQTT 通信业务失败 (1001-1008)
    // 两者不得互相替代 (CR-003 §6)
    for (int i = 1001; i <= 1008; ++i) {
        EXPECT_NE(i, static_cast<int>(LogError::kConfigInvalid));
        EXPECT_NE(i, static_cast<int>(LogError::kInitFailed));
        EXPECT_NE(i, static_cast<int>(LogError::kSinkFailed));
        EXPECT_NE(i, static_cast<int>(LogError::kQueueOverflow));
        EXPECT_NE(i, static_cast<int>(LogError::kSensitiveViolation));
    }
}
