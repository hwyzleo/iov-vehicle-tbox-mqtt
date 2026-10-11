// CR-014 §8: Cloud Ready 契约集成测试
// daemon 组件（CloudReadinessGate + SecTlsKeyAdapter 指纹 + IPC serializer）
// 经 framework-ipc 到 SDK Client 的端到端契约验证。
#include <gtest/gtest.h>
#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "cloud_readiness_gate.h"
#include "sec_tls_key_adapter.h"
#include "credential_manager.h"
#include "ipc.h"
#include <openssl/x509.h>
#include <openssl/evp.h>
#include <openssl/ec.h>
#include <openssl/sha.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace tbox::mqtt;
using namespace tbox::sec;

namespace {

// ---- 测试用 ECDSA P-256 自签证书 ----
struct TestCert {
    std::vector<uint8_t> der;
    EVP_PKEY* pkey = nullptr;
    ~TestCert() { if (pkey) EVP_PKEY_free(pkey); }
};

TestCert createTestCert(const std::string& cn) {
    TestCert tc;
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
    EVP_PKEY_keygen_init(pctx);
    EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1);
    EVP_PKEY* pkey = nullptr;
    EVP_PKEY_keygen(pctx, &pkey);
    EVP_PKEY_CTX_free(pctx);
    tc.pkey = pkey;

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
    tc.der.assign(buf, buf + len);
    OPENSSL_free(buf);
    X509_free(cert);
    return tc;
}

// 用真实 SecTlsKeyAdapter 从测试证书得到 AC-02 定义的 leaf 指纹
std::string fingerprintForCert(TestCert& tc) {
    CredentialManager mgr;
    ActiveTlsCredential cred;
    cred.credential_id = "cred-x";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;
    cred.client_cert_chain = tc.der;
    cred.private_key_ref.data = {0x00, 0x01};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    if (!ctx) return "";
    std::string fp = adapter.getLeafFingerprint();
    adapter.destroyContext();
    return fp;
}

class CloudReadinessIntegHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    ::tbox::fw::ipc::Server* server = nullptr;
    CloudReadinessGate gate;
    std::atomic<int> publish_count{0};

    std::pair<int32_t, bool> onPublish(const PublishEnvelope&) override {
        publish_count.fetch_add(1);
        return {0, true};
    }
    std::pair<int32_t, bool> onSubscribe(const Route&, int) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribeEventChannel(int) override { return {0, true}; }
    std::pair<int32_t, bool> onUnsubscribe(int) override { return {0, true}; }
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry&) override { return {0, true}; }
    std::string onGetConnectionState() override { return R"({"state":"CLOUD_READY"})"; }
    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot&) override { return ReplaceSnapshotResult{}; }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override { return SnapshotStatusResult{}; }
    CloudReadinessSnapshot onGetSubscriptionReadiness() override {
        auto r = gate.getReadiness();
        r.owner_generations["tsp"] = 1;
        return r;
    }
    PublishResult onPublishRoute(const RoutePublishRequest&) override { return PublishResult{}; }
    std::pair<int32_t, bool> onSubscribeRoutedDownlinkEventChannel(
        const std::string&, int) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribeCloudReadinessEventChannel(int client_fd) override {
        if (server) {
            server->add_subscription(
                client_fd, static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED));
        }
        return {0, true};
    }
};

std::string make_socket() {
    return std::string("/tmp/tbox_mqtt_cloud_ready_") +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
}

TlsSessionIdentity makeIdentity(uint64_t cert_gen, const std::string& fp, uint64_t conn_gen) {
    TlsSessionIdentity id;
    id.cert_session_generation = cert_gen;
    id.connection_generation = conn_gen;
    id.credential_id = "cred-x";
    id.credential_version = cert_gen;
    id.certificate_ref = fp;
    id.device_identity_digest = fp;
    id.valid = true;
    return id;
}

} // namespace

class MqttCloudReadinessIntegTest : public ::testing::Test {
protected:
    std::string socket_path;
    CloudReadinessIntegHandler handler;
    std::unique_ptr<MqttIpcDispatcher> dispatcher;
    std::unique_ptr<::tbox::fw::ipc::Server> server;
    std::unique_ptr<Client> client;

    void SetUp() override {
        socket_path = make_socket();
        ::unlink(socket_path.c_str());
        dispatcher = std::make_unique<MqttIpcDispatcher>(&handler);
        server = std::make_unique<::tbox::fw::ipc::Server>(socket_path);
        handler.server = server.get();
        auto* dp = dispatcher.get();
        ASSERT_TRUE(server->start(
            [dp](uint32_t m, std::string_view p, int fd) { return dp->dispatch(m, p, fd); },
            [dp](int fd) { dp->onClientDisconnected(fd); }));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        client = std::make_unique<Client>(socket_path);

        // daemon 侧事件发布：使用与查询响应相同的 serializer
        handler.gate.setCallback([this](const CloudReadinessSnapshot& s) {
            handler.server->push_event(
                static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED),
                cloudReadinessToJson(s));
        });
    }

    void TearDown() override {
        client.reset();
        server->stop();
        server.reset();
        ::unlink(socket_path.c_str());
    }
};

// 查询快照携带实际装载 leaf 证书的身份
TEST_F(MqttCloudReadinessIntegTest, QuerySnapshotCarriesLeafIdentity) {
    TestCert tc = createTestCert("integ-leaf");
    std::string fp = fingerprintForCert(tc);
    ASSERT_FALSE(fp.empty());

    handler.gate.onConnectionState(ConnectionState::CONNECTED, 5);
    handler.gate.setTlsSessionIdentity(makeIdentity(3, fp, 5));
    handler.gate.onSubscriptionsReady(true, 1, 1);

    auto r = client->getSubscriptionReadiness();
    EXPECT_TRUE(r.contract_supported);
    EXPECT_TRUE(r.cloud_ready);
    EXPECT_EQ(r.connection_generation, 5u);
    EXPECT_EQ(r.cert_session_generation, 3u);
    EXPECT_EQ(r.certificate_ref, fp);
    EXPECT_EQ(r.device_identity_digest, fp);
    EXPECT_GT(r.event_sequence, 0u);
}

// 订阅进入/退出事件与查询一致
TEST_F(MqttCloudReadinessIntegTest, SubscribeEnterAndExitConsistent) {
    TestCert tc = createTestCert("integ-enter-exit");
    std::string fp = fingerprintForCert(tc);

    std::mutex m;
    std::vector<CloudReadinessSnapshot> events;
    auto sub = client->subscribeCloudReadinessChanged([&](const CloudReadinessSnapshot& s) {
        std::lock_guard<std::mutex> l(m);
        events.push_back(s);
    });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    handler.gate.onConnectionState(ConnectionState::CONNECTED, 5);
    handler.gate.setTlsSessionIdentity(makeIdentity(3, fp, 5));
    handler.gate.onSubscriptionsReady(true, 1, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    handler.gate.revokeSession("credential_changed");
    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::lock_guard<std::mutex> l(m);
        if (events.size() >= 2) break;
    }

    std::lock_guard<std::mutex> l(m);
    ASSERT_GE(events.size(), 2u);
    // 最后一个事件为退出，且仍携带被撤销会话的旧身份 (CR-014 §4.2)
    const auto& last = events.back();
    EXPECT_FALSE(last.cloud_ready);
    EXPECT_EQ(last.cert_session_generation, 3u);
    EXPECT_EQ(last.certificate_ref, fp);
    // 至少有一个进入事件
    bool saw_ready = false;
    for (const auto& e : events) if (e.cloud_ready) saw_ready = true;
    EXPECT_TRUE(saw_ready);

    // 查询与最后事件读同一状态对象
    auto q = client->getSubscriptionReadiness();
    EXPECT_FALSE(q.cloud_ready);
    EXPECT_EQ(q.event_sequence, events.back().event_sequence);
}

// 凭据轮换：旧会话先退出 Cloud Ready，新会话使用新 generation 与指纹
TEST_F(MqttCloudReadinessIntegTest, RotationUsesNewSessionGenerationAndFingerprint) {
    TestCert tc1 = createTestCert("integ-cred-1");
    TestCert tc2 = createTestCert("integ-cred-2");
    std::string fp1 = fingerprintForCert(tc1);
    std::string fp2 = fingerprintForCert(tc2);
    ASSERT_FALSE(fp1.empty());
    ASSERT_FALSE(fp2.empty());
    ASSERT_NE(fp1, fp2);

    handler.gate.onConnectionState(ConnectionState::CONNECTED, 5);
    handler.gate.setTlsSessionIdentity(makeIdentity(1, fp1, 5));
    handler.gate.onSubscriptionsReady(true, 1, 1);
    ASSERT_TRUE(handler.gate.isCloudReady());
    EXPECT_EQ(client->getSubscriptionReadiness().certificate_ref, fp1);

    // 旧会话撤销（先发布 cloud_ready=false）
    handler.gate.revokeSession("credential_changed");
    auto revoked = client->getSubscriptionReadiness();
    EXPECT_FALSE(revoked.cloud_ready);
    EXPECT_EQ(revoked.certificate_ref, fp1);
    EXPECT_EQ(revoked.cert_session_generation, 1u);

    // 新会话：新 generation + 新指纹，重新收敛后才 ready
    handler.gate.setTlsSessionIdentity(makeIdentity(2, fp2, 5));
    EXPECT_FALSE(client->getSubscriptionReadiness().cloud_ready);
    handler.gate.onSubscriptionsReady(true, 1, 1);
    auto fresh = client->getSubscriptionReadiness();
    EXPECT_TRUE(fresh.cloud_ready);
    EXPECT_EQ(fresh.cert_session_generation, 2u);
    EXPECT_EQ(fresh.certificate_ref, fp2);
}

// 业务 payload 无法覆盖 readiness 身份摘要
TEST_F(MqttCloudReadinessIntegTest, PublishCannotOverrideIdentity) {
    TestCert tc = createTestCert("integ-override");
    std::string fp = fingerprintForCert(tc);

    handler.gate.onConnectionState(ConnectionState::CONNECTED, 5);
    handler.gate.setTlsSessionIdentity(makeIdentity(3, fp, 5));
    handler.gate.onSubscriptionsReady(true, 1, 1);

    PublishEnvelope env;
    env.msg_id = "m-1";
    env.topic = "vehicle/{ecu_uid}/up/fota";
    env.qos = QoS::AT_LEAST_ONCE;
    env.payload = {0x01, 0x02};
    auto pr = client->publish(env);
    EXPECT_TRUE(pr.accepted);
    EXPECT_GE(handler.publish_count.load(), 1);

    auto r = client->getSubscriptionReadiness();
    EXPECT_EQ(r.certificate_ref, fp);
    EXPECT_EQ(r.device_identity_digest, fp);
}
