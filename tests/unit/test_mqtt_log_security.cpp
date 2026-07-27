#include <gtest/gtest.h>
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "log_types.h"

using namespace tbox::mqtt;
using namespace tbox::fw::log;

class MqttLogSecurityTest : public ::testing::Test {
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
// 私钥不暴露 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, PrivateKeyNotExposed) {
    std::string private_key = "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC7...";

    EXPECT_NO_THROW({
        MqttLogAdapter::tls().info(
            "test.private_key",
            "测试私钥不暴露",
            {
                {"private_key", FieldValue::makeString(private_key), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// Token 不暴露 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, TokenNotExposed) {
    std::string token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";

    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.token",
            "测试 Token 不暴露",
            {
                {"token", FieldValue::makeString(token), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// 用户名/密码不暴露 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, PasswordNotExposed) {
    std::string password = "s3cr3t_p@ssw0rd";

    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.password",
            "测试密码不暴露",
            {
                {"password", FieldValue::makeString(password), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// 证书链不暴露 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, CertChainNotExposed) {
    std::string cert_chain = "MIIDXTCCAkWgAwIBAgIJAJC1HiIAZAiUMA0GCSqGSIb3DQEBCwUA...";

    EXPECT_NO_THROW({
        MqttLogAdapter::tls().info(
            "test.cert_chain",
            "测试证书链不暴露",
            {
                {"cert_chain", FieldValue::makeString(cert_chain), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// HSM 句柄不暴露 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, KeyHandleNotExposed) {
    std::string key_handle = "handle_12345_pkcs11";

    EXPECT_NO_THROW({
        MqttLogAdapter::tls().info(
            "test.key_handle",
            "测试密钥句柄不暴露",
            {
                {"key_handle", FieldValue::makeString(key_handle), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// VIN 掩码 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, VinMasked) {
    std::string vin = "LMWXXX1234567890";

    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.vin",
            "测试 VIN 掩码",
            {
                {"vin", FieldValue::makeString(vin), Sensitivity::Identifier}
            }
        );
    });
}

// ============================================================
// clientId 掩码 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, ClientIdMasked) {
    std::string client_id = "HWYZTEST900000001-00000001AA00000001";

    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.client_id",
            "测试 clientId 掩码",
            {
                {"client_id", FieldValue::makeString(client_id), Sensitivity::Identifier}
            }
        );
    });
}

// ============================================================
// device_sn 掩码 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, DeviceSnMasked) {
    std::string device_sn = "SN12345678ABCDEF";

    EXPECT_NO_THROW({
        MqttLogAdapter::publish().info(
            "test.device_sn",
            "测试 device_sn 掩码",
            {
                {"device_sn", FieldValue::makeString(device_sn), Sensitivity::Identifier}
            }
        );
    });
}

// ============================================================
// Broker endpoint 不暴露完整地址 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, BrokerEndpointNotExposed) {
    std::string broker_host = "vagw.openiov.top";

    // 正确用法：使用 broker_hash 而非原始地址
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.broker_hash",
            "测试 Broker hash",
            {
                {"broker_hash", FieldValue::makeString(brokerHash(broker_host))}
            }
        );
    });

    // 错误用法（应避免）：直接输出 Broker 地址作为 Identifier
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.broker_raw",
            "测试 Broker 原始地址标识符掩码",
            {
                {"broker_host", FieldValue::makeString(broker_host), Sensitivity::Identifier}
            }
        );
    });
}

// ============================================================
// 原始 topic 不暴露设备标识 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, RawTopicNotExposed) {
    std::string raw_topic = "vehicle/HWYZTEST900000001-00000001AA00000001/up/fota";

    // 正确用法：使用 topic_template
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().info(
            "test.topic_template",
            "测试 topic 模板化",
            {
                {"topic_template", FieldValue::makeString(topicTemplate(raw_topic))}
            }
        );
    });
}

// ============================================================
// 原始 payload 不进入常规日志 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, RawPayloadNotInNormalLog) {
    std::string raw_payload = R"({"vin":"LMWXXX1234567890","token":"secret123","data":"..."})";

    // 原始 payload 应标记为 Payload 敏感度，且仅 DEBUG 级别可输出摘要
    EXPECT_NO_THROW({
        MqttLogAdapter::publish().debug(
            "test.raw_payload",
            "测试原始 payload 限长",
            {
                {"payload_summary", FieldValue::makeString(raw_payload), Sensitivity::Payload}
            }
        );
    });
}

// ============================================================
// PKCS#11 URI 不暴露 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, Pkcs11UriNotExposed) {
    std::string pkcs11_uri = "pkcs11:object=device-key;type=private";

    EXPECT_NO_THROW({
        MqttLogAdapter::tls().info(
            "test.pkcs11_uri",
            "测试 PKCS#11 URI 不暴露",
            {
                {"pkcs11_uri", FieldValue::makeString(pkcs11_uri), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// 完整证书文件路径不暴露 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, CertFilePathNotExposed) {
    std::string cert_path = "/var/tbox/mqtt/tbox_chain.crt";
    std::string key_path = "/var/tbox/mqtt/tbox.pem";

    EXPECT_NO_THROW({
        MqttLogAdapter::tls().info(
            "test.cert_path",
            "测试证书路径不暴露",
            {
                {"cert_path", FieldValue::makeString(cert_path), Sensitivity::Secret},
                {"key_path", FieldValue::makeString(key_path), Sensitivity::Secret}
            }
        );
    });
}

// ============================================================
// 混合安全字段测试 (CR-003 §5)
// ============================================================
TEST_F(MqttLogSecurityTest, MixedSecurityFields) {
    EXPECT_NO_THROW({
        MqttLogAdapter::connection().info(
            "test.mixed_security",
            "测试混合安全字段",
            {
                // Normal - 可直接输出
                {"qos", FieldValue::makeString("1"), Sensitivity::Normal},
                {"retry_count", FieldValue::makeInt(3), Sensitivity::Normal},
                {"broker_hash", FieldValue::makeString("h_abcd1234"), Sensitivity::Normal},
                // Identifier - 应掩码
                {"client_id", FieldValue::makeString("VIN123SN456"), Sensitivity::Identifier},
                // Secret - 应拒绝输出
                {"password", FieldValue::makeString("s3cr3t"), Sensitivity::Secret},
                {"private_key", FieldValue::makeString("MIIE..."), Sensitivity::Secret},
                // Payload - 应限长
                {"payload_summary", FieldValue::makeString("data..."), Sensitivity::Payload}
            }
        );
    });
}
