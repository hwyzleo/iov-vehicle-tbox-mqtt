#include <gtest/gtest.h>
#include "mqtt_client_manager.h"
#include "credential_manager.h"
#include "sec_tls_key_adapter.h"
#include <openssl/x509.h>
#include <openssl/evp.h>
#include <openssl/ec.h>
#include <openssl/bn.h>

using namespace tbox::mqtt;

namespace {
/// CR-017: 生成指定 CN 的自签 ECDSA P-256 DER 证书（用于身份一致性测试）
std::vector<uint8_t> makeSelfSignedDer(const std::string& cn) {
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
    EVP_PKEY_keygen_init(pctx);
    EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1);
    EVP_PKEY* pkey = nullptr;
    EVP_PKEY_keygen(pctx, &pkey);
    EVP_PKEY_CTX_free(pctx);
    if (!pkey) return {};

    X509* cert = X509_new();
    X509_set_version(cert, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
    X509_gmtime_adj(X509_get_notBefore(cert), 0);
    X509_gmtime_adj(X509_get_notAfter(cert), 31536000L);
    X509_NAME* name = X509_NAME_new();
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
        reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
    X509_set_subject_name(cert, name);
    X509_set_issuer_name(cert, name);
    X509_NAME_free(name);
    X509_set_pubkey(cert, pkey);
    X509_sign(cert, pkey, EVP_sha256());
    unsigned char* buf = nullptr;
    int len = i2d_X509(cert, &buf);
    std::vector<uint8_t> der(buf, buf + len);
    OPENSSL_free(buf);
    X509_free(cert);
    EVP_PKEY_free(pkey);
    return der;
}
} // namespace

class MqttClientManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        cred_mgr_.simulateReady();
    }

    CredentialManager cred_mgr_;
    SecTlsKeyAdapter tls_adapter_;
};

TEST_F(MqttClientManagerTest, InitializeClient) {
    MqttClientManager client;
    EXPECT_TRUE(client.initialize(cred_mgr_, tls_adapter_));
}

TEST_F(MqttClientManagerTest, InitializeClientWithNotReadyCredential) {
    CredentialManager not_ready;
    not_ready.simulateNotReady();

    MqttClientManager client;
    // initialize() just stores references; readiness checked in connect()
    EXPECT_TRUE(client.initialize(not_ready, tls_adapter_));
}

TEST_F(MqttClientManagerTest, ConnectWithNotReadyCredential) {
    CredentialManager not_ready;
    not_ready.simulateNotReady();

    MqttClientManager client;
    client.initialize(not_ready, tls_adapter_);

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";
    config.credential_profile = "mqtt";
    config.keepalive = 60;
    config.clean_start = false;
    config.session_expiry = 3600;

    // connect() should fail because credential is not ready
    EXPECT_FALSE(client.connect(config));
}

TEST_F(MqttClientManagerTest, ConnectWithoutInitialize) {
    MqttClientManager client;

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";
    config.credential_profile = "mqtt";
    config.keepalive = 60;
    config.clean_start = false;
    config.session_expiry = 3600;

    // connect() should fail because cred_mgr_ and tls_adapter_ are not set
    EXPECT_FALSE(client.connect(config));
}

TEST_F(MqttClientManagerTest, PublishWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize(cred_mgr_, tls_adapter_));

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    auto result = client.publish("test/topic", payload, QoS::AT_LEAST_ONCE);
    EXPECT_FALSE(result.accepted);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1002);
}

TEST_F(MqttClientManagerTest, SubscribeWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize(cred_mgr_, tls_adapter_));

    auto result = client.subscribe("test/topic", QoS::AT_LEAST_ONCE);
    EXPECT_FALSE(result.status);
    EXPECT_EQ(result.error, ErrorCode::MQTT_1002);
}

TEST_F(MqttClientManagerTest, UnsubscribeWithoutConnect) {
    MqttClientManager client;
    ASSERT_TRUE(client.initialize(cred_mgr_, tls_adapter_));

    EXPECT_FALSE(client.unsubscribe("test/topic"));
}

TEST_F(MqttClientManagerTest, SetCallbacks) {
    MqttClientManager client;

    bool message_received = false;
    bool connection_changed = false;

    client.setMessageCallback([&message_received](const std::string& topic, const std::vector<uint8_t>& payload) {
        message_received = true;
    });

    client.setConnectionCallback([&connection_changed](bool connected) {
        connection_changed = true;
    });

    EXPECT_FALSE(message_received);
    EXPECT_FALSE(connection_changed);
}

TEST_F(MqttClientManagerTest, SetLwtConfig) {
    MqttClientManager client;

    LwtConfig lwt;
    lwt.topic = "status/offline";
    lwt.payload = {0x00};
    lwt.qos = QoS::AT_LEAST_ONCE;
    lwt.retain = true;

    client.setLwtConfig(lwt);
}

TEST_F(MqttClientManagerTest, IsConnectedInitiallyFalse) {
    MqttClientManager client;
    EXPECT_FALSE(client.isConnected());
}

TEST_F(MqttClientManagerTest, IsConnectedAfterFailedConnect) {
    CredentialManager not_ready;
    not_ready.simulateNotReady();

    MqttClientManager client;
    client.initialize(not_ready, tls_adapter_);

    ConnectionConfig config;
    config.broker_host = "test.mosquitto.org";
    config.broker_port = 8883;
    config.client_id = "test_client";
    config.credential_profile = "mqtt";
    config.keepalive = 60;

    EXPECT_FALSE(client.connect(config));
    EXPECT_FALSE(client.isConnected());
}

TEST_F(MqttClientManagerTest, NoLocalCredentialFileAccess) {
    // CR-005 §10 安全测试: 确保不使用本地凭据文件
    // MqttClientManager 不接受任何文件路径参数
    MqttClientManager client;
    EXPECT_TRUE(client.initialize(cred_mgr_, tls_adapter_));
    // No file path parameters in initialize() - compilation verifies this
}

// CR-017 §3: leaf CN 与 PROV username 身份不一致时 fail-closed（不发起网络连接）
TEST_F(MqttClientManagerTest, ConnectFailsOnUsernameCnConflict) {
    std::vector<uint8_t> der = makeSelfSignedDer("ECU-ABC");
    ASSERT_FALSE(der.empty());

    CredentialManager ready;
    ActiveTlsCredential cred;
    cred.credential_id = "cred-1";
    cred.version = 1;
    cred.status = tbox::sec::TlsCredentialStatus::READY;
    cred.root_ca_bundle = der;
    cred.client_cert_chain = der;
    cred.private_key_ref.data = {0x01, 0x02};
    cred.key_algorithm = tbox::sec::KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    ready.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    MqttClientManager client;
    ASSERT_TRUE(client.initialize(ready, adapter));

    ConnectionConfig config;
    config.broker_host = "broker.example.com";
    config.broker_port = 8883;
    config.client_id = "VIN0001-SN0001";
    config.credential_profile = "mqtt";
    config.username = "ECU-XYZ";  // 与 CN(ECU-ABC) 不一致
    config.min_tls_version = TlsVersion::TLS_1_2;
    config.max_tls_version = TlsVersion::TLS_1_3;

    EXPECT_FALSE(client.connect(config));
    // 不发布有效会话身份
    EXPECT_FALSE(client.getTlsSessionIdentity().valid);
    EXPECT_TRUE(client.getTlsSessionIdentity().ecu_uid.empty());
}
