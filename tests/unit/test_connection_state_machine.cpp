#include <gtest/gtest.h>
#include "connection_state_machine.h"
#include "store.h"
#include <filesystem>
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

TEST(ConnectionStateMachineTest, TransitionToConnectingWhenCertReady) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);
}

TEST(ConnectionStateMachineTest, TransitionToConnectedOnSuccess) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onConnectionSuccess();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED);
}

TEST(ConnectionStateMachineTest, TransitionToReconnectingOnFailure) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onConnectionFailure("mTLS handshake failed");
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);
}

TEST(ConnectionStateMachineTest, ExponentialBackoffReconnection) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
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
    sm.onConnectionSuccess();

    ASSERT_EQ(transitions.size(), 4u);
    EXPECT_EQ(transitions[0], ConnectionState::PROV_WAIT);
    EXPECT_EQ(transitions[1], ConnectionState::CERT_WAIT);
    EXPECT_EQ(transitions[2], ConnectionState::CONNECTING);
    EXPECT_EQ(transitions[3], ConnectionState::CONNECTED);
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
    sm.onConnectionSuccess();
    sm.onDisconnect();

    ASSERT_EQ(transitions.size(), 5u);
    EXPECT_EQ(transitions[0], ConnectionState::PROV_WAIT);
    EXPECT_EQ(transitions[1], ConnectionState::CERT_WAIT);
    EXPECT_EQ(transitions[2], ConnectionState::CONNECTING);
    EXPECT_EQ(transitions[3], ConnectionState::CONNECTED);
    EXPECT_EQ(transitions[4], ConnectionState::RECONNECTING);
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
    sm.onConnectionFailure("test error");

    // Verify state was persisted to store
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

    // First session: transition to RECONNECTING and persist
    {
        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ConnectionStateMachine sm;
        sm.setStore(store);
        sm.start();
        sm.onProvisionReady();
        sm.onCertificateReady();
        sm.onConnectionFailure("persistent error");
        // State is now RECONNECTING, persisted to store
        sm.stop();
    }

    // Second session: restore from store, should downgrade to PROV_WAIT
    // (PROV binding snapshot is in-memory only, must re-read on restart)
    {
        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ConnectionStateMachine sm;
        sm.setStore(store);
        sm.start();

        // RECONNECTING should be downgraded to PROV_WAIT on restart
        EXPECT_EQ(sm.getCurrentState(), ConnectionState::PROV_WAIT);

        auto state_info = sm.getStateInfo();
        EXPECT_EQ(state_info.retry_count, 0u);
        sm.stop();
    }

    cleanupTempStoreDir(store_dir);
}

TEST(ConnectionStateMachineTest, ConnectedDowngradedToProvWaitOnRestore) {
    std::string store_dir = createTempStoreDir();

    // First session: reach CONNECTED state (persisted to store)
    {
        auto store = std::make_shared<hwyz::store::Store>(
            hwyz::store::Store::open("mqtt", store_dir));
        ConnectionStateMachine sm;
        sm.setStore(store);
        sm.start();
        sm.onProvisionReady();
        sm.onCertificateReady();
        sm.onConnectionSuccess();
        // State is now CONNECTED, persisted to store
        sm.stop();
    }

    // Second session: CONNECTED should be downgraded to PROV_WAIT
    // (PROV binding snapshot is in-memory only, must re-read on restart)
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
    // Now in CONNECTING

    // Simulate connection failure -> RECONNECTING with short backoff
    sm.onConnectionFailure("timeout");
    // Now in RECONNECTING, timer thread should auto-fire after backoff delay

    // Wait for the timer to fire (base delay is 1s * 2^1 = 2s + jitter)
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // Timer should have auto-transitioned to CONNECTING
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);

    sm.stop();
}

TEST(ConnectionStateMachineTest, RetryTimerStopsOnStop) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onProvisionReady();
    sm.onCertificateReady();
    sm.onConnectionFailure("timeout");
    // Now in RECONNECTING

    // Stop should cleanly join the timer thread
    sm.stop();

    // State machine should still be usable after stop
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);
}
