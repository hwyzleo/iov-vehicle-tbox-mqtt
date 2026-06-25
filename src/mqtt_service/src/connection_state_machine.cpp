#include "connection_state_machine.h"
#include <algorithm>
#include <cstdlib>

namespace tbox {
namespace mqtt {

ConnectionStateMachine::ConnectionStateMachine() {
    state_info_.state = ConnectionState::INIT;
}

void ConnectionStateMachine::setStateChangeCallback(StateChangeCallback callback) {
    callback_ = callback;
}

void ConnectionStateMachine::start() {
    transitionTo(ConnectionState::CERT_WAIT);
}

ConnectionState ConnectionStateMachine::getCurrentState() const {
    return state_info_.state;
}

ConnectionStateInfo ConnectionStateMachine::getStateInfo() const {
    return state_info_;
}

void ConnectionStateMachine::setMaxRetries(uint32_t max_retries) {
    max_retries_ = max_retries;
}

void ConnectionStateMachine::onCertificateReady() {
    if (state_info_.state == ConnectionState::CERT_WAIT) {
        transitionTo(ConnectionState::CONNECTING);
    }
}

void ConnectionStateMachine::onConnectionSuccess() {
    if (state_info_.state == ConnectionState::CONNECTING) {
        state_info_.retry_count = 0;
        transitionTo(ConnectionState::CONNECTED);
    }
}

void ConnectionStateMachine::onConnectionFailure(const std::string& error) {
    if (state_info_.state == ConnectionState::CONNECTING) {
        state_info_.last_error = error;
        state_info_.retry_count++;

        if (state_info_.retry_count >= max_retries_) {
            transitionTo(ConnectionState::OFFLINE);
        } else {
            transitionTo(ConnectionState::RECONNECTING);
            calculateNextRetry();
        }
    }
}

void ConnectionStateMachine::onRetryTimeout() {
    if (state_info_.state == ConnectionState::RECONNECTING) {
        if (state_info_.retry_count >= max_retries_) {
            transitionTo(ConnectionState::OFFLINE);
        } else {
            transitionTo(ConnectionState::CONNECTING);
        }
    }
}

void ConnectionStateMachine::onNetworkRestored() {
    if (state_info_.state == ConnectionState::OFFLINE) {
        state_info_.retry_count = 0;
        transitionTo(ConnectionState::CERT_WAIT);
    }
}

void ConnectionStateMachine::onDisconnect() {
    if (state_info_.state == ConnectionState::CONNECTED) {
        state_info_.last_error = "Connection disconnected";
        state_info_.retry_count++;
        transitionTo(ConnectionState::RECONNECTING);
        calculateNextRetry();
    }
}

void ConnectionStateMachine::transitionTo(ConnectionState new_state) {
    state_info_.state = new_state;
    if (callback_) {
        callback_(new_state);
    }
}

void ConnectionStateMachine::calculateNextRetry() {
    auto delay = base_retry_delay_ * (1 << std::min(state_info_.retry_count, 10u));
    delay = std::min(delay, max_retry_delay_);

    auto jitter_ms = delay.count() / 10;
    auto jitter = (std::rand() % (2 * jitter_ms)) - jitter_ms;
    delay += std::chrono::milliseconds(jitter);

    state_info_.next_retry_at = std::chrono::steady_clock::now() + delay;
}

} // namespace mqtt
} // namespace tbox
