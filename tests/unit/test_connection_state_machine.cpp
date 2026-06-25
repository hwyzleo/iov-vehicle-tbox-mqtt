#include <gtest/gtest.h>
#include "connection_state_machine.h"

using namespace tbox::mqtt;

TEST(ConnectionStateMachineTest, InitialStateIsInit) {
    ConnectionStateMachine sm;
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::INIT);
}

TEST(ConnectionStateMachineTest, TransitionToCertWait) {
    ConnectionStateMachine sm;
    sm.start();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CERT_WAIT);
}

TEST(ConnectionStateMachineTest, TransitionToConnectingWhenCertReady) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onCertificateReady();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTING);
}

TEST(ConnectionStateMachineTest, TransitionToConnectedOnSuccess) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onCertificateReady();
    sm.onConnectionSuccess();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CONNECTED);
}

TEST(ConnectionStateMachineTest, TransitionToReconnectingOnFailure) {
    ConnectionStateMachine sm;
    sm.start();
    sm.onCertificateReady();
    sm.onConnectionFailure("mTLS handshake failed");
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::RECONNECTING);
}

TEST(ConnectionStateMachineTest, ExponentialBackoffReconnection) {
    ConnectionStateMachine sm;
    sm.start();
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
    sm.onCertificateReady();

    for (int i = 0; i < 3; ++i) {
        sm.onConnectionFailure("timeout");
        sm.onRetryTimeout();
    }

    EXPECT_EQ(sm.getCurrentState(), ConnectionState::OFFLINE);
}

TEST(ConnectionStateMachineTest, ResetToCertWaitFromOffline) {
    ConnectionStateMachine sm;
    sm.setMaxRetries(3);
    sm.start();
    sm.onCertificateReady();

    for (int i = 0; i < 3; ++i) {
        sm.onConnectionFailure("timeout");
        sm.onRetryTimeout();
    }

    sm.onNetworkRestored();
    EXPECT_EQ(sm.getCurrentState(), ConnectionState::CERT_WAIT);
}

TEST(ConnectionStateMachineTest, TransitionToReconnectingOnDisconnect) {
    ConnectionStateMachine sm;
    sm.start();
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
    sm.onCertificateReady();
    sm.onConnectionSuccess();

    ASSERT_EQ(transitions.size(), 3u);
    EXPECT_EQ(transitions[0], ConnectionState::CERT_WAIT);
    EXPECT_EQ(transitions[1], ConnectionState::CONNECTING);
    EXPECT_EQ(transitions[2], ConnectionState::CONNECTED);
}

TEST(ConnectionStateMachineTest, CallbackInvokedOnDisconnect) {
    ConnectionStateMachine sm;
    std::vector<ConnectionState> transitions;

    sm.setStateChangeCallback([&transitions](ConnectionState state) {
        transitions.push_back(state);
    });

    sm.start();
    sm.onCertificateReady();
    sm.onConnectionSuccess();
    sm.onDisconnect();

    ASSERT_EQ(transitions.size(), 4u);
    EXPECT_EQ(transitions[0], ConnectionState::CERT_WAIT);
    EXPECT_EQ(transitions[1], ConnectionState::CONNECTING);
    EXPECT_EQ(transitions[2], ConnectionState::CONNECTED);
    EXPECT_EQ(transitions[3], ConnectionState::RECONNECTING);
}
