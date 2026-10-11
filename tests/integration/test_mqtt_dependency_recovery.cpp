//
// TBOX-MQTT-DSN-CR-016 §11: SEC/PROV IPC client 自动恢复集成测试
//
// 使用最小 stub framework-ipc Server（真实 AF_UNIX socket + framework-ipc framing）
// 覆盖：SEC/PROV 晚启动、首次连接失败、运行期对端重启、generation 防旧回调。
// 目标：MQTT client 无需重启 daemon 即可恢复。
//
// stub 只实现测试所需最小协议（READ_BINDING / GET_TLS_CREDENTIAL[_STATE]），
// method id 集中在 tests/support/dependency_stub_protocol.h。
//

#include <gtest/gtest.h>

#include "credential_manager.h"
#include "provision_identity_manager.h"
#include "dependency_recovery.h"
#include "ipc.h"

#include "support/dependency_stub_protocol.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

using namespace tbox::mqtt;

namespace {

// 进程级忽略 SIGPIPE：MQTT daemon 由 hwyz::Application 设置 SIG_IGN；
// 本测试在真实 socket 上模拟对端关闭，必须与 daemon 语义一致。
struct IgnoreSigpipe {
    IgnoreSigpipe() { ::signal(SIGPIPE, SIG_IGN); }
};
const IgnoreSigpipe g_ignore_sigpipe{};

std::string uniqueSock(const char* tag) {
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::string("/tmp/tbox_mqtt_cr016_") + tag + "_" +
           std::to_string(now) + ".sock";
}

/// 最小 stub framework-ipc Server。
/// responder 返回 response JSON（method_id 对应 method id）。
class StubServer {
public:
    explicit StubServer(std::string sock) : sock_(std::move(sock)) {}

    void start(std::function<std::string(uint32_t, const std::string&)> responder) {
        ::unlink(sock_.c_str());
        server_ = std::make_unique<::tbox::fw::ipc::Server>(sock_);
        auto r = std::move(responder);
        server_->start(
            [r](uint32_t method_id, std::string_view params, int /*fd*/) {
                return r(method_id, std::string(params));
            },
            {});
        // 等待 listen 就绪
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    void stop() {
        if (server_) {
            server_->stop();
            server_.reset();
        }
        ::unlink(sock_.c_str());
    }

    ~StubServer() { stop(); }

    const std::string& socket() const { return sock_; }

private:
    std::string sock_;
    std::unique_ptr<::tbox::fw::ipc::Server> server_;
};

/// PROV stub：READ_BINDING -> BOUND 绑定
std::string provResponder(uint32_t method_id, const std::string&) {
    if (method_id == test::kProvReadBinding) {
        return R"({"status":0,"vin":"HWYZTEST900000001","ecu_uid":"00000001AA00000001","sn":"TBOX00000001","state":2,"locked":true})";
    }
    return R"({"status":0})";
}

/// SEC stub：GET_TLS_CREDENTIAL -> READY 凭据
std::string secResponder(uint32_t method_id, const std::string&) {
    if (method_id == test::kSecGetTlsCredential) {
        return R"({"status":0,"credential_id":"test-cred","version":1,"root_ca_bundle":"","client_cert_chain":"","private_key_ref":"","key_algorithm":"","not_before":1000000000,"not_after":2000000000,"credential_status":"READY"})";
    }
    if (method_id == test::kSecGetTlsCredentialState) {
        return R"({"status":0,"credential_id":"test-cred","version":1,"credential_status":"READY","reason_code":0})";
    }
    return R"({"status":0})";
}

CredentialPollConfig shortSecPoll() {
    CredentialPollConfig c;
    c.initial_backoff = Duration(50);
    c.max_backoff = Duration(100);
    return c;
}

ProvisionRetryConfig shortProvRetry() {
    ProvisionRetryConfig c;
    c.initial_backoff = Duration(50);
    c.max_backoff = Duration(100);
    c.multiplier = 2.0;
    return c;
}

} // namespace

// ============================================================
// SEC：晚启动 / 首连失败 -> 自动恢复
// ============================================================

TEST(MqttDependencyRecoveryIntegTest, SecClientRecoversWhenServerStartsLate) {
    std::string sock = uniqueSock("sec_late");
    CredentialManager mgr(sock, "mqtt");
    mgr.setCredentialPollConfig(shortSecPoll());

    // stub 尚未启动：首次连接失败，retry_count 递增且不再永久自锁
    EXPECT_FALSE(mgr.ensureConnected());
    EXPECT_EQ(mgr.getIpcState(), IpcState::DISCONNECTED);
    auto rs1 = mgr.getRetryState();
    EXPECT_EQ(rs1.retry_count, 1u);
    EXPECT_EQ(rs1.total_attempts, 1u);

    EXPECT_FALSE(mgr.ensureConnected());
    auto rs2 = mgr.getRetryState();
    EXPECT_EQ(rs2.retry_count, 2u);
    EXPECT_EQ(rs2.total_attempts, 2u);

    // SEC 稍后启动
    StubServer server(sock);
    server.start(secResponder);

    // 无需重建 manager，下一次尝试自动恢复
    EXPECT_TRUE(mgr.ensureConnected());
    EXPECT_EQ(mgr.getIpcState(), IpcState::CONNECTED);

    ActiveTlsCredential cred;
    EXPECT_TRUE(mgr.getTlsCredential(cred));
    EXPECT_TRUE(cred.ready);

    // 成功离态后连续失败计数重置，total_attempts 保持累计
    mgr.markDependencyReady();
    auto rs3 = mgr.getRetryState();
    EXPECT_EQ(rs3.retry_count, 0u);
    EXPECT_GE(rs3.total_attempts, 3u);

    server.stop();
}

// ============================================================
// PROV：晚启动 / 首连失败 -> 自动恢复
// ============================================================

TEST(MqttDependencyRecoveryIntegTest, ProvClientRecoversWhenServerStartsLate) {
    std::string sock = uniqueSock("prov_late");
    ProvisionIdentityManager mgr(sock);
    mgr.setProvisionRetryConfig(shortProvRetry());

    EXPECT_FALSE(mgr.ensureConnected());
    EXPECT_FALSE(mgr.readBinding());
    EXPECT_EQ(mgr.getIpcState(), IpcState::DISCONNECTED);
    auto rs1 = mgr.getRetryState();
    EXPECT_EQ(rs1.retry_count, 1u);
    EXPECT_FALSE(mgr.getLastError().empty());

    StubServer server(sock);
    server.start(provResponder);

    EXPECT_TRUE(mgr.ensureConnected());
    EXPECT_TRUE(mgr.readBinding());
    EXPECT_TRUE(mgr.isBound());
    EXPECT_EQ(mgr.getSnapshot().vin, "HWYZTEST900000001");

    mgr.markDependencyReady();
    EXPECT_EQ(mgr.getRetryState().retry_count, 0u);

    server.stop();
}

// ============================================================
// 运行期对端重启：识别断链并重建 client（无需重启 MQTT）
// ============================================================

TEST(MqttDependencyRecoveryIntegTest, SecClientRecoversAfterPeerRestart) {
    std::string sock = uniqueSock("sec_restart");
    CredentialManager mgr(sock, "mqtt");
    mgr.setCredentialPollConfig(shortSecPoll());

    StubServer server(sock);
    server.start(secResponder);

    EXPECT_TRUE(mgr.ensureConnected());
    ActiveTlsCredential cred;
    EXPECT_TRUE(mgr.getTlsCredential(cred));
    uint64_t gen_before = mgr.getClientGeneration();

    // 对端重启：旧连接失效
    server.stop();

    // 首次查询识别传输失败并失效旧 transport
    ActiveTlsCredential cred2;
    EXPECT_FALSE(mgr.getTlsCredential(cred2));
    EXPECT_EQ(mgr.getIpcState(), IpcState::DISCONNECTED);

    // 对端恢复：重建 client 后恢复（generation 递增）
    server.start(secResponder);
    EXPECT_TRUE(mgr.ensureConnected());
    EXPECT_GT(mgr.getClientGeneration(), gen_before);
    EXPECT_TRUE(mgr.getTlsCredential(cred2));
    EXPECT_TRUE(cred2.ready);

    server.stop();
}

TEST(MqttDependencyRecoveryIntegTest, ProvClientRecoversAfterPeerRestart) {
    std::string sock = uniqueSock("prov_restart");
    ProvisionIdentityManager mgr(sock);
    mgr.setProvisionRetryConfig(shortProvRetry());

    StubServer server(sock);
    server.start(provResponder);

    EXPECT_TRUE(mgr.ensureConnected());
    EXPECT_TRUE(mgr.readBinding());
    uint64_t gen_before = mgr.getClientGeneration();

    server.stop();

    // 旧连接上查询失败后 transport 失效
    EXPECT_FALSE(mgr.readBinding());
    EXPECT_EQ(mgr.getIpcState(), IpcState::DISCONNECTED);

    server.start(provResponder);
    EXPECT_TRUE(mgr.ensureConnected());
    EXPECT_GT(mgr.getClientGeneration(), gen_before);
    EXPECT_TRUE(mgr.readBinding());

    server.stop();
}

// ============================================================
// generation guard：旧 client 迟到返回 READY/BOUND 时丢弃
// ============================================================

TEST(MqttDependencyRecoveryIntegTest, SecLateResultDroppedByGenerationGuard) {
    std::string sock = uniqueSock("sec_stale");
    CredentialManager mgr(sock, "mqtt");
    mgr.setCredentialPollConfig(shortSecPoll());

    StubServer server(sock);
    server.start(secResponder);
    ASSERT_TRUE(mgr.ensureConnected());

    // 在查询返回与提交之间模拟一次 transport 失效（迟到回调）
    mgr.setPreCommitHookForTest([&mgr]() {
        mgr.invalidateTransportForTest();
    });

    ActiveTlsCredential cred;
    EXPECT_FALSE(mgr.getTlsCredential(cred));

    mgr.setPreCommitHookForTest(nullptr);
    server.stop();
}

TEST(MqttDependencyRecoveryIntegTest, ProvLateResultDroppedByGenerationGuard) {
    std::string sock = uniqueSock("prov_stale");
    ProvisionIdentityManager mgr(sock);
    mgr.setProvisionRetryConfig(shortProvRetry());

    StubServer server(sock);
    server.start(provResponder);
    ASSERT_TRUE(mgr.ensureConnected());

    mgr.setPreCommitHookForTest([&mgr]() {
        mgr.invalidateTransportForTest();
    });

    EXPECT_FALSE(mgr.readBinding());

    mgr.setPreCommitHookForTest(nullptr);
    server.stop();
}
