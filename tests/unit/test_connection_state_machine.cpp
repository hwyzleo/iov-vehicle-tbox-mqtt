#include <gtest/gtest.h>
#include "connection_state_machine.h"
#include "store.h"
#include <filesystem>
#include <limits>
#include <thread>
#include <chrono>

using namespace tbox::mqtt;

namespace {
std::string createTempStoreDir() {
    auto dir = std::filesystem::temp_directory_path() / "tbox_mqtt_test_sm_store";
    std::filesystem::create_directories(dir);
    return dir.string();
}

void cleanupTempStoreDir(const std::string& path) {
    std::filesystem::remove_all(path);
}
}

TEST(ConnectionStateMachineTest, InitialStateIsInit) {
    ConnectionStateMachine sm;
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::INIT);
}

TEST(ConnectionStateMachineTest, TransitionToCertWait) {
    ConnectionStateMachine sm;
    sm.start();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::PROV_WAIT);
}

// CR-007 §7: CERT_WAIT -> SUBSCRIPTION_WAIT（门闩），onGateSatisfied -> CONNECTING
TEST(ConnectionStateMachineTest, TransitionToSubscriptionWaitWhenCertReady) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::SUBSCRIPTION_WAIT);

    sm.onGateSatisfied();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);
}

// CR-007 §6: 门闩超时降级连接
TEST(ConnectionStateMachineTest, TransitionToConnectingOnGateTimeout) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::SUBSCRIPTION_WAIT);

    sm.onGateTimeout();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);
    EXPECT_TRUE(sm.isDegraded());
}

TEST(ConnectionStateMachineTest, TransitionToConnectedOnSuccess) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED);
}

TEST(ConnectionStateMachineTest, TransitionToReconnectingOnFailure) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionFailure("mTLS handshake failed");
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);
}

TEST(ConnectionStateMachineTest, ExponentialBackoffReconnection) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionFailure("timeout");

    auto state_info = sm.getStateInfo();
    EXPECT_EQ(state_info.retry_count, 1);
    EXPECT_GT(state_info.next_retry_at, std::chrono::steady_clock::now());
}

TEST(ConnectionStateMachineTest, TransitionToOfflineAfterMaxRetries) {
    ConnectionStateMachine sm;
    sm.setMaxRetries(3);
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();

    for (int i = 0; i < 3; ++i) {
        sm.onConnectionFailure("timeout");
        sm.onRetryTimeout();
    }

    EXPECT_EQ(sm.getCurrentState(), ConnectionState::OFFLINE);
}

TEST(ConnectionStateMachineTest, ResetToProvWaitFromOffline) {
    ConnectionStateMachine sm;
    sm.setMaxRetries(3);
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();

    for (int i = 0; i < 3; ++i) {
        sm.onConnectionFailure("timeout");
        sm.onRetryTimeout();
    }

    sm.onNetworkRestored();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::PROV_WAIT);
}

TEST(ConnectionStateMachineTest, TransitionToReconnectingOnDisconnect) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED);

    sm.onDisconnect();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);

    auto state_info = sm.getStateInfo();
    EXPECT_EQ(state_info.retry_count, 1);
    EXPECT_EQ(state_info.last_error, "Connection disconnected");
    EXPECT_GT(state_info.next_retry_at, std::chrono::steady_clock::now());
}

TEST(ConnectionStateMachineTest, CallbackInvokedOnStateChange) {
    ConnectionStateMachine sm;
    std::vector<ConnectionState> transitions;

    sm.setStateChangeCallback([&transitions](ConnectionState state) {
        transitions.push_back(state);
    });

    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();

    // PROV_WAIT, CERT_WAIT, SUBSCRIPTION_WAIT, CONNECTING, CONNECTED
    ASSERT_EQ(transitions.size(), 5u);
    EXPECT_EQ(transitions[0], ConnectionState::PROV_WAIT);
    EXPECT_EQ(transitions[1], ConnectionState::CERT_WAIT);
    EXPECT_EQ(transitions[2], ConnectionState::SUBSCRIPTION_WAIT);
    EXPECT_EQ(transitions[3], ConnectionState::CONNECTING);
    EXPECT_EQ(transitions[4], ConnectionState::CONNECTED);
}

TEST(ConnectionStateMachineTest, CallbackInvokedOnDisconnect) {
    ConnectionStateMachine sm;
    std::vector<ConnectionState> transitions;

    sm.setStateChangeCallback([&transitions](ConnectionState state) {
        transitions.push_back(state);
    });

    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    sm.onDisconnect();

    // PROV_WAIT, CERT_WAIT, SUBSCRIPTION_WAIT, CONNECTING, CONNECTED, RECONNECTING
    ASSERT_EQ(transitions.size(), 6u);
    EXPECT_EQ(transitions[0], ConnectionState::PROV_WAIT);
    EXPECT_EQ(transitions[1], ConnectionState::CERT_WAIT);
    EXPECT_EQ(transitions[2], ConnectionState::SUBSCRIPTION_WAIT);
    EXPECT_EQ(transitions[3], ConnectionState::CONNECTING);
    EXPECT_EQ(transitions[4], ConnectionState::CONNECTED);
    EXPECT_EQ(transitions[5], ConnectionState::RECONNECTING);
}

TEST(ConnectionStateMachineTest, StatePersistedToStore) {
    std::string store_dir = createTempStoreDir();
    auto store = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_dir));

    ConnectionStateMachine sm;
    sm.setStore(store);
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionFailure("test error");

    int state_int = store->loadOr<int>(StoreKeys::kConnState, 0);
    EXPECT_EQ(static_cast<ConnectionState>(state_int), ConnectionState::RECONNECTING);

    std::string last_error = store->loadOr<std::string>(StoreKeys::kConnLastError, "");
    EXPECT_EQ(last_error, "test error");

    int retry_count = store->loadOr<int>(StoreKeys::kConnRetryCount, 0);
    EXPECT_EQ(retry_count, 1);

    cleanupTempStoreDir(store_dir);
}

TEST(ConnectionStateMachineTest, StateRestoredFromStore) {
    std::string store_dir = createTempStoreDir();

    {
        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ConnectionStateMachine sm;
        sm.setStore(store);
        sm.start();
        sm.onProvisionReady();
        sm.onCertificateReady();
        sm.onGateSatisfied();
        sm.onConnectionFailure("persistent error");
        sm.stop();
    }

    {
        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ConnectionStateMachine sm;
        sm.setStore(store);
        sm.start();

        EXPECT_EQ(sm.getCurrentState(), ConnectionState::PROV_WAIT);

        auto state_info = sm.getStateInfo();
        EXPECT_EQ(state_info.retry_count, 0u);
        sm.stop();
    }

    cleanupTempStoreDir(store_dir);
}

TEST(ConnectionStateMachineTest, ConnectedDowngradedToProvWaitOnRestore) {
    std::string store_dir = createTempStoreDir();

    {
        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ConnectionStateMachine sm;
        sm.setStore(store);
        sm.start();
        sm.onProvisionReady();
        sm.onCertificateReady();
        sm.onGateSatisfied();
        sm.onConnectionSuccess();
        sm.stop();
    }

    {
        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ConnectionStateMachine sm;
        sm.setStore(store);
        sm.start();

        EXPECT_EQ(sm.getCurrentState(), ConnectionState::PROV_WAIT);
        sm.stop();
    }

    cleanupTempStoreDir(store_dir);
}

TEST(ConnectionStateMachineTest, RetryTimerAutoTransitionsToConnecting) {
    ConnectionStateMachine sm;
    sm.setMaxRetries(5);

    std::vector<ConnectionState> transitions;
    sm.setStateChangeCallback([&transitions](ConnectionState state) {
        transitions.push_back(state);
    });

    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();

    sm.onConnectionFailure("timeout");

    std::this_thread::sleep_for(std::chrono::seconds(4));

    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);

    sm.stop();
}

TEST(ConnectionStateMachineTest, RetryTimerStopsOnStop) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionFailure("timeout");

    sm.stop();

    EXPECT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);
}

// ============================================================
// CR-007 §7: 订阅门闩与 Cloud Ready 状态转换
// ============================================================

TEST(ConnectionStateMachineTest, SubscribingToCloudReady) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED);

    sm.onSubscribing();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::SUBSCRIBING);

    sm.onSubscriptionsReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CLOUD_READY);
}

TEST(ConnectionStateMachineTest, CloudReadyLostToSubscribing) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    sm.onSubscribing();
    sm.onSubscriptionsReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CLOUD_READY);

    sm.onCloudReadyLost();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::SUBSCRIBING);
}

TEST(ConnectionStateMachineTest, MandatoryFailedToDegraded) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    sm.onSubscribing();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::SUBSCRIBING);

    sm.onMandatoryFailed("suback_timeout");
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED_DEGRADED);
}

TEST(ConnectionStateMachineTest, DegradedDoesNotReachCloudReady) {
    // 门闩超时降级后，即使 mandatory active 也不得 Cloud Ready
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateTimeout();
    EXPECT_TRUE(sm.isDegraded());

    sm.onConnectionSuccess();
    sm.onSubscribing();
    sm.onSubscriptionsReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED_DEGRADED);

    // 清除降级后可达 Cloud Ready
    sm.setDegraded(false);
    sm.onSubscriptionsReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CLOUD_READY);
}

// ============================================================
// CR-017 §3 / §19: connection generation 生命周期
// ============================================================

TEST(ConnectionStateMachineTest, ConnectionGenerationStartsAtZero) {
    // 0 = 尚无有效连接上下文
    ConnectionStateMachine sm;
    EXPECT_EQ(sm.getConnectionGeneration(), 0u);
}

TEST(ConnectionStateMachineTest, CommitActiveConnectionIsOneBasedAndMonotonic) {
    ConnectionStateMachine sm;
    EXPECT_EQ(sm.commitActiveConnection(), 1u);
    EXPECT_EQ(sm.getConnectionGeneration(), 1u);
    EXPECT_EQ(sm.commitActiveConnection(), 2u);
    EXPECT_EQ(sm.getConnectionGeneration(), 2u);
}

TEST(ConnectionStateMachineTest, NextNonzeroGenerationSkipsZeroOnWrap) {
    EXPECT_EQ(next_nonzero_connection_generation(0u), 1u);
    EXPECT_EQ(next_nonzero_connection_generation(1u), 2u);
    EXPECT_EQ(next_nonzero_connection_generation(41u), 42u);
    // uint64 回绕不得回到 0
    EXPECT_EQ(next_nonzero_connection_generation(
                  std::numeric_limits<uint64_t>::max()), 1u);
}

TEST(ConnectionStateMachineTest, ColdStartCommitsFirstGenerationOnConnect) {
    // 冷启动：0 -> 首次有效会话 1
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    ASSERT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);
    ASSERT_EQ(sm.getConnectionGeneration(), 0u);  // 尚未提交有效连接上下文

    sm.onConnectionSuccess();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED);
    EXPECT_EQ(sm.getConnectionGeneration(), 1u);
}

TEST(ConnectionStateMachineTest, ReconnectYieldsNewGeneration) {
    // 普通断线重连：1 -> 2（不依赖 PROV/凭据变化）
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    ASSERT_EQ(sm.getConnectionGeneration(), 1u);

    sm.onDisconnect();
    ASSERT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);
    sm.onRetryTimeout();
    ASSERT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);
    sm.onConnectionSuccess();
    EXPECT_EQ(sm.getConnectionGeneration(), 2u);
}

TEST(ConnectionStateMachineTest, InvalidationNeverProducesZero) {
    // 显式 invalidate 也必须产生非零 generation
    ConnectionStateMachine sm;
    sm.invalidateConnectionGeneration();
    EXPECT_NE(sm.getConnectionGeneration(), 0u);
    sm.invalidateConnectionGeneration();
    EXPECT_NE(sm.getConnectionGeneration(), 0u);
}

TEST(ConnectionStateMachineTest, CredentialChangeInvalidatesGeneration) {
    // 换证：旧连接上下文失效，generation 必须前进且非零
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    const uint64_t gen1 = sm.getConnectionGeneration();
    ASSERT_NE(gen1, 0u);

    sm.onCredentialChanged();  // CONNECTED -> CERT_WAIT
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CERT_WAIT);
    EXPECT_GT(sm.getConnectionGeneration(), gen1);
    EXPECT_NE(sm.getConnectionGeneration(), 0u);
}

TEST(ConnectionStateMachineTest, ProvisionChangeInvalidatesGeneration) {
    // PROV 绑定变化：旧连接上下文失效，generation 必须前进且非零
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onGateSatisfied();
    sm.onConnectionSuccess();
    const uint64_t gen1 = sm.getConnectionGeneration();
    ASSERT_NE(gen1, 0u);

    sm.onProvisionChanged();  // CONNECTED -> PROV_WAIT
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::PROV_WAIT);
    EXPECT_GT(sm.getConnectionGeneration(), gen1);
    EXPECT_NE(sm.getConnectionGeneration(), 0u);
}
