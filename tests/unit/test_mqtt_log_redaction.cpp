#include <gtest/gtest.h>
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "log_types.h"

using namespace tbox::mqtt;
using namespace tbox::fw::log;

class MqttLogRedactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        LogConfig config;
        config.level = LogLevel::kDebug;
        config.console_config.enabled = true;
        config.redact_config.identifiers = "mask";
        MqttLogAdapter::init("mqtt_test", config);
    }
};

// ============================================================
// Identifier 字段掩码 (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, IdentifierMasking) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.identifier",
            "测试标识符掩码",
            {
                {"client_id", FieldValue::makeString("HWYZTEST900000001-00000001AA00000001"), Sensitivity::Identifier},
                {"vin", FieldValue::makeString("LMWXXX1234567890"), Sensitivity::Identifier},
                {"device_sn", FieldValue::makeString("SN12345678"), Sensitivity::Identifier}
            }
        );
    });
}

// ============================================================
// Secret 字段拒绝输出 (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, SecretRejection) {
    EXPECT_NO_THROW({
        MqttLogAdapter::tls().info(
            "test.secret",
            "测试密钥拒绝",
            {
                {"private_key", FieldValue::makeString("MIIEvgIBADANBg..."), Sensitivity::Secret},
                {"password", FieldValue::makeString("s3cr3t"), Sensitivity::Secret},
                {"token", FieldValue::makeString("eyJhbGciOi..."), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// Payload 字段限长 (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, PayloadLimiting) {
    std::string long_payload(1000, 'A');
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().info(
            "test.payload",
            "测试报文限长",
            {
                {"payload_summary", FieldValue::makeString(long_payload), Sensitivity::Payload}
            }
        );
    });
}

// ============================================================
// Normal 字段不脱敏 (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, NormalFieldNoRedaction) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.normal",
            "测试普通字段",
            {
                {"qos", FieldValue::makeString("1"), Sensitivity::Normal},
                {"retry_count", FieldValue::makeInt(3), Sensitivity::Normal},
                {"duration_ms", FieldValue::makeInt(150), Sensitivity::Normal}
            }
        );
    });
}

// ============================================================
// 混合敏感度字段 (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, MixedSensitivityFields) {
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().info(
            "test.mixed",
            "测试混合敏感度字段",
            {
                {"qos", FieldValue::makeString("1"), Sensitivity::Normal},
                {"client_id_hash", FieldValue::makeString("c_abcd1234"), Sensitivity::Normal},
                {"client_id", FieldValue::makeString("VIN123SN456"), Sensitivity::Identifier},
                {"private_key", FieldValue::makeString("SECRET_KEY"), Sensitivity::Secret},
                {"payload_summary", FieldValue::makeString("payload_data"), Sensitivity::Payload}
            }
        );
    });
}

// ============================================================
// 使用脱敏辅助函数输出 hash (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, HashHelperFunctions) {
    // brokerHash
    std::string bhash = brokerHash("vagw.openiov.top");
    EXPECT_FALSE(bhash.empty());
    EXPECT_EQ(bhash.substr(0, 2), "h_");

    // clientIdHash
    std::string chash = clientIdHash("HWYZTEST900000001-00000001AA00000001");
    EXPECT_FALSE(chash.empty());
    EXPECT_EQ(chash.substr(0, 2), "c_");

    // msgIdHash
    std::string mhash = msgIdHash("msg_123456789");
    EXPECT_FALSE(mhash.empty());
    EXPECT_EQ(mhash.substr(0, 2), "m_");

    // 相同输入产生相同 hash
    EXPECT_EQ(brokerHash("vagw.openiov.top"), bhash);
    EXPECT_EQ(clientIdHash("HWYZTEST900000001-00000001AA00000001"), chash);

    // 不同输入产生不同 hash
    EXPECT_NE(brokerHash("host1"), brokerHash("host2"));
    EXPECT_NE(clientIdHash("id1"), clientIdHash("id2"));

    // 空输入
    EXPECT_EQ(brokerHash(""), "h_empty");
    EXPECT_EQ(clientIdHash(""), "c_empty");
    EXPECT_EQ(msgIdHash(""), "m_empty");
}

// ============================================================
// topicTemplate 模板化 (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, TopicTemplateFunction) {
    // 包含设备标识的 topic 应被模板化
    std::string templated = topicTemplate("vehicle/HWYZTEST900000001-00000001AA00000001/up/fota");
    EXPECT_NE(templated.find("{device_sn}"), std::string::npos);
    EXPECT_EQ(templated.find("HWYZTEST900000001"), std::string::npos);

    // 不含设备标识的 topic 保持原样
    std::string clean = topicTemplate("vehicle/diag/status");
    EXPECT_EQ(clean, "vehicle/diag/status");

    // 空 topic
    EXPECT_EQ(topicTemplate(""), "t_empty");

    // 多段设备标识
    std::string multi = topicTemplate("vehicle/VIN123SN456/down/VIN123SN456/config");
    EXPECT_NE(multi.find("{device_sn}"), std::string::npos);
    EXPECT_EQ(multi.find("VIN123SN456"), std::string::npos);
}

// ============================================================
// 使用 hash 字段而非原始标识符 (CR-003 §5)
// ============================================================
TEST_F(MqttLogRedactionTest, UseHashFieldsNotRawIdentifiers) {
    // 正确用法：使用 hash 字段
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "mqtt.connection.established",
            "MQTT 连接建立成功",
            {
                {"broker_hash", FieldValue::makeString(brokerHash("vagw.openiov.top"))},
                {"client_id_hash", FieldValue::makeString(clientIdHash("VIN123SN456"))},
                {"keepalive_s", FieldValue::makeInt(60)}
            }
        );
    });

    // 正确用法：使用 topic_template
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().debug(
            "mqtt.publish.acknowledged",
            "QoS1 PUBACK 成功",
            {
                {"msg_id_hash", FieldValue::makeString(msgIdHash("msg_123"))},
                {"topic_template", FieldValue::makeString(topicTemplate("vehicle/VIN123SN456/up/fota"))},
                {"qos", FieldValue::makeString("1")}
            }
        );
    });
}
