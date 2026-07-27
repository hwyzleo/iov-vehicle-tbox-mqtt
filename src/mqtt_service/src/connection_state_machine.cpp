#include "connection_state_machine.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <algorithm>

namespace tbox {
namespace mqtt {

ConnectionStateMachine::ConnectionStateMachine() {
    state_info_.state = ConnectionState::INIT;
}

ConnectionStateMachine::~ConnectionStateMachine() {
    stop();
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
    // Stop any previously running timer thread before starting fresh
    stop();

    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // Restore previous state from framework-store before starting
        restoreState();

        // On restart, any active connection state is invalid - a live
        // TCP/TLS session cannot be resumed across process boundaries.
        // Downgrade CONNECTED / CONNECTING / RECONNECTING to CERT_WAIT so
        // the normal flow (CERT_WAIT -> onCertificateReady -> CONNECTING)
        // re-establishes the connection from scratch.
        if (state_info_.state == ConnectionState::CONNECTED ||
            state_info_.state == ConnectionState::CONNECTING ||
            state_info_.state == ConnectionState::RECONNECTING) {
            state_info_.state = ConnectionState::CERT_WAIT;
            state_info_.last_error = "Restarted, re-entering CERT_WAIT";
            state_info_.retry_count = 0;
            persistState();
        }

        if (state_info_.state == ConnectionState::INIT ||
            state_info_.state == ConnectionState::OFFLINE) {
            cb = transitionTo(ConnectionState::CERT_WAIT);
        } else {
            cb = callback_;
        }

        // Start the retry timer thread
        timer_running_ = true;
        timer_thread_ = std::thread(&ConnectionStateMachine::timerLoop, this);
    }
    if (cb) cb(state_info_.state);
}

void ConnectionStateMachine::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        timer_running_ = false;
        timer_cv_.notify_all();
    }
    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }
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
    timer_cv_.notify_all();
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
    timer_cv_.notify_all();
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
        MqttLogAdapter::connection().warn(
            "mqtt.store.persist_failed",
            "连接状态持久化失败",
            {
                {"state", tbox::fw::log::FieldValue::makeString(
                    connectionStateToString(state_info_.state))},
                {"reason", tbox::fw::log::FieldValue::makeString(e.what())}
            }
        );
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
            MqttLogAdapter::connection().info(
                "mqtt.store.state_restored",
                "从 framework-store 恢复连接状态",
                {
                    {"state", tbox::fw::log::FieldValue::makeString(
                        connectionStateToString(state_info_.state))},
                    {"retry_count", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(state_info_.retry_count))}
                }
            );
        }
    } catch (const hwyz::store::StoreException& e) {
        MqttLogAdapter::connection().warn(
            "mqtt.store.restore_failed",
            "连接状态恢复失败",
            {{"reason", tbox::fw::log::FieldValue::makeString(e.what())}}
        );
    }
}

void ConnectionStateMachine::timerLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (timer_running_) {
        if (state_info_.state == ConnectionState::RECONNECTING) {
            auto wake_time = state_info_.next_retry_at;
            bool predicate_changed = timer_cv_.wait_until(lock, wake_time, [this] {
                return !timer_running_ ||
                       state_info_.state != ConnectionState::RECONNECTING;
            });

            if (!timer_running_) break;
            if (predicate_changed) continue; // State changed externally, re-evaluate

            // Timeout expired and state is still RECONNECTING -> trigger retry.
            // Unlock before calling onRetryTimeout() (it acquires the same mutex).
            lock.unlock();
            onRetryTimeout();
            lock.lock();
        } else {
            // Not in RECONNECTING; sleep until state changes or we stop
            timer_cv_.wait(lock, [this] {
                return !timer_running_ ||
                       state_info_.state == ConnectionState::RECONNECTING;
            });
        }
    }
}

} // namespace mqtt
} // namespace tbox
