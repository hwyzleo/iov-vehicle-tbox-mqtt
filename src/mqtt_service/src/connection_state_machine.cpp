#include "connection_state_machine.h"
#include <algorithm>
#include <iostream>

namespace tbox {
namespace mqtt {

ConnectionStateMachine::ConnectionStateMachine() {
    state_info_.state = ConnectionState::INIT;
}

void ConnectionStateMachine::setStateChangeCallback(StateChangeCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = callback;
}

void ConnectionStateMachine::setStore(std::shared_ptr<hwyz::store::Store> store) {
    std::lock_guard<std::mutex> lock(mutex_);
    store_ = store;
}

void ConnectionStateMachine::start() {
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // Restore previous state from framework-store before starting
        restoreState();

        // If restored state was CONNECTED, downgrade to RECONNECTING
        // (cannot resume a live TCP/TLS connection across restart)
        if (state_info_.state == ConnectionState::CONNECTED) {
            state_info_.state = ConnectionState::RECONNECTING;
            state_info_.last_error = "Restored from store, downgrading CONNECTED to RECONNECTING";
            persistState();
        }

        // Transition to CERT_WAIT to begin connection flow
        if (state_info_.state == ConnectionState::INIT ||
            state_info_.state == ConnectionState::OFFLINE) {
            cb = transitionTo(ConnectionState::CERT_WAIT);
        } else {
            // Already in a non-initial state from restore; notify callback
            cb = callback_;
        }
    }
    if (cb) cb(state_info_.state);
}

ConnectionState ConnectionStateMachine::getCurrentState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_info_.state;
}

ConnectionStateInfo ConnectionStateMachine::getStateInfo() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_info_;
}

void ConnectionStateMachine::setMaxRetries(uint32_t max_retries) {
    std::lock_guard<std::mutex> lock(mutex_);
    max_retries_ = max_retries;
}

void ConnectionStateMachine::onCertificateReady() {
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CERT_WAIT) {
            cb = transitionTo(ConnectionState::CONNECTING);
        }
    }
    if (cb) cb(ConnectionState::CONNECTING);
}

void ConnectionStateMachine::onConnectionSuccess() {
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CONNECTING) {
            state_info_.retry_count = 0;
            cb = transitionTo(ConnectionState::CONNECTED);
        }
    }
    if (cb) cb(ConnectionState::CONNECTED);
}

void ConnectionStateMachine::onConnectionFailure(const std::string& error) {
    StateChangeCallback cb;
    ConnectionState new_state;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CONNECTING) {
            state_info_.last_error = error;
            state_info_.retry_count++;

            if (state_info_.retry_count >= max_retries_) {
                new_state = ConnectionState::OFFLINE;
                cb = transitionTo(ConnectionState::OFFLINE);
            } else {
                new_state = ConnectionState::RECONNECTING;
                cb = transitionTo(ConnectionState::RECONNECTING);
                calculateNextRetry();
            }
        }
    }
    if (cb) cb(new_state);
}

void ConnectionStateMachine::onRetryTimeout() {
    StateChangeCallback cb;
    ConnectionState new_state;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::RECONNECTING) {
            if (state_info_.retry_count >= max_retries_) {
                new_state = ConnectionState::OFFLINE;
                cb = transitionTo(ConnectionState::OFFLINE);
            } else {
                new_state = ConnectionState::CONNECTING;
                cb = transitionTo(ConnectionState::CONNECTING);
            }
        }
    }
    if (cb) cb(new_state);
}

void ConnectionStateMachine::onNetworkRestored() {
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::OFFLINE) {
            state_info_.retry_count = 0;
            cb = transitionTo(ConnectionState::CERT_WAIT);
        }
    }
    if (cb) cb(ConnectionState::CERT_WAIT);
}

void ConnectionStateMachine::onDisconnect() {
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CONNECTED) {
            state_info_.last_error = "Connection disconnected";
            state_info_.retry_count++;
            cb = transitionTo(ConnectionState::RECONNECTING);
            calculateNextRetry();
        }
    }
    if (cb) cb(ConnectionState::RECONNECTING);
}

ConnectionStateMachine::StateChangeCallback ConnectionStateMachine::transitionTo(ConnectionState new_state) {
    state_info_.state = new_state;
    persistState();
    return callback_;
}

void ConnectionStateMachine::calculateNextRetry() {
    auto delay = base_retry_delay_ * (1 << std::min(state_info_.retry_count, 10u));
    delay = std::min(delay, max_retry_delay_);

    auto jitter_ms = delay.count() / 10;
    std::uniform_int_distribution<int64_t> dist(-jitter_ms, jitter_ms);
    auto jitter = dist(rng_);
    delay += std::chrono::milliseconds(jitter);

    state_info_.next_retry_at = std::chrono::steady_clock::now() + delay;
}

void ConnectionStateMachine::persistState() {
    if (!store_ || !store_->isReady()) {
        return;
    }

    try {
        store_->save<int>(StoreKeys::kConnState, static_cast<int>(state_info_.state));
        store_->save<std::string>(StoreKeys::kConnLastError, state_info_.last_error);
        store_->save<int>(StoreKeys::kConnRetryCount, static_cast<int>(state_info_.retry_count));
    } catch (const hwyz::store::StoreException& e) {
        std::cerr << "[MQTT] State persist failed: " << e.what() << std::endl;
    }
}

void ConnectionStateMachine::restoreState() {
    if (!store_ || !store_->isReady()) {
        return;
    }

    try {
        int state_int = store_->loadOr<int>(StoreKeys::kConnState,
                                             static_cast<int>(ConnectionState::INIT));
        state_info_.state = static_cast<ConnectionState>(state_int);
        state_info_.last_error = store_->loadOr<std::string>(StoreKeys::kConnLastError, "");
        state_info_.retry_count = static_cast<uint32_t>(
            store_->loadOr<int>(StoreKeys::kConnRetryCount, 0));

        if (state_info_.state != ConnectionState::INIT) {
            std::cout << "[MQTT] Restored state from store: "
                      << static_cast<int>(state_info_.state)
                      << ", retry_count=" << state_info_.retry_count << std::endl;
        }
    } catch (const hwyz::store::StoreException& e) {
        std::cerr << "[MQTT] State restore failed: " << e.what() << std::endl;
    }
}

} // namespace mqtt
} // namespace tbox
