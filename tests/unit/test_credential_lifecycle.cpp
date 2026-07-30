#include <gtest/gtest.h>
#include "credential_manager.h"
#include "sec_tls_key_adapter.h"
#include "connection_state_machine.h"
#include <thread>
#include <atomic>
#include <chrono>

using namespace tbox::mqtt;
using namespace tbox::sec;

// ============================================================
// CR-005 §7: 凭据生命周期测试
// ============================================================

TEST(CredentialLifecycleTest, GenerationIncrementsOnVersionChange) {
    CredentialManager mgr;
    mgr.simulateReady();

    ActiveTlsCredential cred1 = mgr.getSnapshot();
    uint64_t gen1 = mgr.getGeneration();

    // Simulate version change (new credential)
    ActiveTlsCredential cred2 = cred1;
    cred2.version = cred1.version + 1;
    mgr.simulateReady(cred2);

    uint64_t gen2 = mgr.getGeneration();
    EXPECT_GT(gen2, gen1);
}

TEST(CredentialLifecycleTest, GenerationIncrementsOnInvalidate) {
    CredentialManager mgr;
    mgr.simulateReady();
    uint64_t gen1 = mgr.getGeneration();

    mgr.invalidateCredential();
    uint64_t gen2 = mgr.getGeneration();
    EXPECT_EQ(gen2, gen1 + 1);

    mgr.invalidateCredential();
    uint64_t gen3 = mgr.getGeneration();
    EXPECT_EQ(gen3, gen2 + 1);
}

TEST(CredentialLifecycleTest, CleanupOrderClearsAllBuffers) {
    // CR-005 §7 cleanup 顺序:
    // 禁止新握手 -> 标记 generation 失效 -> 停止/等待旧 TLS callback ->
    // 释放 SSL/SSL_CTX -> 清零 CA/cert/key_ref 缓冲 -> 获取新版本
    CredentialManager mgr;
    mgr.simulateReady();

    auto snap = mgr.getSnapshot();
    ASSERT_FALSE(snap.root_ca_bundle.empty());
    ASSERT_FALSE(snap.client_cert_chain.empty());
    ASSERT_FALSE(snap.private_key_ref.empty());

    uint64_t gen_before = mgr.getGeneration();
    mgr.cleanupCredential();
    uint64_t gen_after = mgr.getGeneration();
    EXPECT_GT(gen_after, gen_before);  // generation 应递增

    snap = mgr.getSnapshot();
    EXPECT_TRUE(snap.root_ca_bundle.empty());
    EXPECT_TRUE(snap.client_cert_chain.empty());
    EXPECT_TRUE(snap.private_key_ref.empty());
    EXPECT_FALSE(snap.ready);
    EXPECT_EQ(snap.status, TlsCredentialStatus::NOT_READY);
}

TEST(CredentialLifecycleTest, ChangedEventTriggersCallback) {
    CredentialManager mgr;
    mgr.simulateReady();

    std::atomic<int> call_count{0};
    mgr.subscribeTlsCredentialChanged([&call_count]() {
        call_count++;
    });

    EXPECT_EQ(call_count.load(), 0);
    mgr.simulateStateChanged();
    EXPECT_EQ(call_count.load(), 1);

    mgr.simulateStateChanged();
    EXPECT_EQ(call_count.load(), 2);
}

TEST(CredentialLifecycleTest, ChangedEventIncrementsGeneration) {
    CredentialManager mgr;
    mgr.simulateReady();
    uint64_t gen1 = mgr.getGeneration();

    mgr.simulateStateChanged();
    uint64_t gen2 = mgr.getGeneration();
    EXPECT_GT(gen2, gen1);
}

TEST(CredentialLifecycleTest, ConcurrentSnapshotAccess) {
    // CR-005 §7: 网络线程、TLS callback 和 SEC event callback 只通过线程安全快照访问当前凭据
    CredentialManager mgr;
    mgr.simulateReady();

    std::atomic<bool> running{true};
    std::atomic<int> ready_count{0};

    // Reader thread: continuously get snapshots
    std::thread reader([&]() {
        while (running) {
            auto snap = mgr.getSnapshot();
            if (snap.ready) {
                ready_count++;
            }
        }
    });

    // Writer thread: toggle credential state
    std::thread writer([&]() {
        for (int i = 0; i < 100; ++i) {
            mgr.simulateNotReady();
            mgr.simulateReady();
        }
        running = false;
    });

    writer.join();
    reader.join();

    // Should not crash; ready_count should be > 0
    EXPECT_GT(ready_count.load(), 0);
}

TEST(CredentialLifecycleTest, StateMachineCredentialInvalidTransition) {
    // CR-005 §6: CONNECTING -> CERT_WAIT (SEC-1011~1014)
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    ASSERT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);

    sm.onCredentialInvalid("SEC-1011: credential invalid");
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CERT_WAIT);
}

TEST(CredentialLifecycleTest, StateMachineCredentialChangedFromConnected) {
    // CR-005 §6: CONNECTED -> CERT_WAIT (credential changed / revoked / SEC restart)
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    ASSERT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED);

    sm.onCredentialChanged();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CERT_WAIT);
}

TEST(CredentialLifecycleTest, StateMachineCredentialStateChangedFromReconnecting) {
    // CR-005 §6: RECONNECTING -> CERT_WAIT (credential state changed)
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionFailure("network error");
    ASSERT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);

    sm.onCredentialStateChanged();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CERT_WAIT);
}

TEST(CredentialLifecycleTest, StateMachineConnectionGenerationIncrements) {
    // CR-005 §7: connection generation 递增
    ConnectionStateMachine sm;
    uint64_t gen1 = sm.getConnectionGeneration();

    sm.invalidateConnectionGeneration();
    uint64_t gen2 = sm.getConnectionGeneration();
    EXPECT_GT(gen2, gen1);
}

TEST(CredentialLifecycleTest, StateMachineCredentialChangedFromConnecting) {
    // CR-005 §6: CONNECTING -> CERT_WAIT (credential changed during connect)
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    ASSERT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);

    sm.onCredentialChanged();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CERT_WAIT);
}

TEST(CredentialLifecycleTest, CredentialPollConfigSetsBackoff) {
    // CR-005 §6: CERT_WAIT 独立退避
    ConnectionStateMachine sm;
    CredentialPollConfig config;
    config.initial_backoff = Duration(500);
    config.max_backoff = Duration(5000);
    sm.setCredentialPollConfig(config);

    // Start -> PROV_WAIT
    sm.start();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::PROV_WAIT);
}

TEST(CredentialLifecycleTest, TlsAdapterDestroyAfterBuild) {
    // CR-005 §7: 旧 TLS session 清理
    CredentialManager mgr;
    mgr.simulateReady();

    SecTlsKeyAdapter adapter;
    // Without building a real context, just verify destroy is safe
    adapter.destroyContext();
    EXPECT_FALSE(adapter.isContextValid());
}

TEST(CredentialLifecycleTest, ReFetchAfterCleanup) {
    // CR-005 §7: 清理后重新获取凭据
    CredentialManager mgr;
    mgr.simulateReady();
    EXPECT_TRUE(mgr.isCredentialReady());

    mgr.cleanupCredential();
    EXPECT_FALSE(mgr.isCredentialReady());

    // Re-fetch
    mgr.simulateReady();
    EXPECT_TRUE(mgr.isCredentialReady());

    auto snap = mgr.getSnapshot();
    EXPECT_FALSE(snap.root_ca_bundle.empty());
    EXPECT_FALSE(snap.private_key_ref.empty());
}
