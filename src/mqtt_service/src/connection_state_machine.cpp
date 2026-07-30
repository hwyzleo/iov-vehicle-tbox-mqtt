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

        // CR-006 §4: On restart, PROV binding snapshot is in-memory only,
        // so we must always re-read binding from PROV_WAIT. A live TCP/TLS
        // session also cannot be resumed across process boundaries.
        // Downgrade CONNECTED / CONNECTING / RECONNECTING / CERT_WAIT to
        // PROV_WAIT so the normal flow (PROV_WAIT -> CERT_WAIT -> CONNECTING)
        // re-establishes the connection from scratch.
        if (state_info_.state == ConnectionState::CONNECTED ||
            state_info_.state == ConnectionState::CONNECTING ||
            state_info_.state == ConnectionState::RECONNECTING ||
            state_info_.state == ConnectionState::CERT_WAIT) {
            state_info_.state = ConnectionState::PROV_WAIT;
            state_info_.last_error = "Restarted, re-entering PROV_WAIT";
            state_info_.retry_count = 0;
            persistState();
        }

        if (state_info_.state == ConnectionState::INIT ||
            state_info_.state == ConnectionState::OFFLINE) {
            provision_poll_count_ = 0;
            calculateNextProvisionPoll();
            cb = transitionTo(ConnectionState::PROV_WAIT);
        } else {
            // PROV_WAIT (from downgrade or restored)
            provision_poll_count_ = 0;
            calculateNextProvisionPoll();
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
            credential_poll_count_ = 0;  // CR-005 §6: 重置退避计数
            cb = transitionTo(ConnectionState::CONNECTING);
        }
    }
    if (cb) cb(ConnectionState::CONNECTING);
}

void ConnectionStateMachine::onConnectionSuccess() {
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // CR-005 §7: 检查 connection generation，阻止旧 callback 重入 CONNECTED
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
            provision_poll_count_ = 0;
            calculateNextProvisionPoll();
            cb = transitionTo(ConnectionState::PROV_WAIT);
        }
    }
    if (cb) cb(ConnectionState::PROV_WAIT);
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

// ============================================================
// CR-005 §6: 凭据事件
// ============================================================

void ConnectionStateMachine::setCredentialPollConfig(const CredentialPollConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    credential_poll_config_ = config;
}

// ============================================================
// CR-006 §4: PROV 绑定事件
// ============================================================

void ConnectionStateMachine::setProvisionRetryConfig(const ProvisionRetryConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    provision_retry_config_ = config;
}

void ConnectionStateMachine::onProvisionReady() {
    // CR-006 §4: PROV_WAIT -> CERT_WAIT (binding valid)
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::PROV_WAIT) {
            provision_poll_count_ = 0;  // Reset PROV poll count
            credential_poll_count_ = 0;
            calculateNextCredentialPoll();
            cb = transitionTo(ConnectionState::CERT_WAIT);
        }
    }
    if (cb) cb(ConnectionState::CERT_WAIT);
}

void ConnectionStateMachine::onProvisionInvalid(const std::string& reason) {
    // CR-006 §4: binding invalid/changed -> PROV_WAIT
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CONNECTING ||
            state_info_.state == ConnectionState::CONNECTED ||
            state_info_.state == ConnectionState::RECONNECTING) {
            state_info_.last_error = reason;
            state_info_.retry_count = 0;
            provision_poll_count_ = 0;
            calculateNextProvisionPoll();
            cb = transitionTo(ConnectionState::PROV_WAIT);
        } else if (state_info_.state == ConnectionState::PROV_WAIT) {
            // Already in PROV_WAIT, increment poll count for next retry
            provision_poll_count_++;
            calculateNextProvisionPoll();
            cb = callback_;
        }
    }
    timer_cv_.notify_all();
    if (cb) cb(ConnectionState::PROV_WAIT);
}

void ConnectionStateMachine::onProvisionChanged() {
    // CR-006 §7: binding changed during active connection -> PROV_WAIT
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CONNECTED ||
            state_info_.state == ConnectionState::CONNECTING ||
            state_info_.state == ConnectionState::RECONNECTING) {
            connection_generation_++;  // Invalidate connection generation
            state_info_.last_error = "Provision binding changed";
            state_info_.retry_count = 0;
            provision_poll_count_ = 0;
            calculateNextProvisionPoll();
            cb = transitionTo(ConnectionState::PROV_WAIT);
        }
    }
    timer_cv_.notify_all();
    if (cb) cb(ConnectionState::PROV_WAIT);
}

void ConnectionStateMachine::onProvisionPollTimeout() {
    // CR-006 §4: PROV_WAIT 退避超时，递增计数并重新计算退避
    // 实际的 PROV readBinding 由 MqttService 经 state change callback 触发
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::PROV_WAIT) {
            provision_poll_count_++;
            calculateNextProvisionPoll();
            cb = callback_;
        }
    }
    if (cb) cb(ConnectionState::PROV_WAIT);
}

void ConnectionStateMachine::onCredentialInvalid(const std::string& reason) {
    // CR-005 §6: CONNECTING -> CERT_WAIT (SEC-1011~1014 / credential invalid)
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CONNECTING) {
            state_info_.last_error = reason;
            credential_poll_count_ = 0;
            calculateNextCredentialPoll();
            cb = transitionTo(ConnectionState::CERT_WAIT);
        }
    }
    timer_cv_.notify_all();
    if (cb) cb(ConnectionState::CERT_WAIT);
}

void ConnectionStateMachine::onCredentialChanged() {
    // CR-005 §6: CONNECTED -> CERT_WAIT (credential changed / revoked / SEC restart)
    // CR-005 §7: 先标记 connection generation 失效，再断开
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CONNECTED ||
            state_info_.state == ConnectionState::CONNECTING) {
            connection_generation_++;  // 标记失效，阻止旧 callback 重入 CONNECTED
            state_info_.last_error = "Credential changed/restarted";
            credential_poll_count_ = 0;
            calculateNextCredentialPoll();
            cb = transitionTo(ConnectionState::CERT_WAIT);
        }
    }
    timer_cv_.notify_all();
    if (cb) cb(ConnectionState::CERT_WAIT);
}

void ConnectionStateMachine::onCredentialStateChanged() {
    // CR-005 §6: RECONNECTING -> CERT_WAIT (credential state changed)
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::RECONNECTING) {
            credential_poll_count_ = 0;
            calculateNextCredentialPoll();
            cb = transitionTo(ConnectionState::CERT_WAIT);
        }
    }
    timer_cv_.notify_all();
    if (cb) cb(ConnectionState::CERT_WAIT);
}

void ConnectionStateMachine::onCredentialPollTimeout() {
    // CR-005 §6: CERT_WAIT 退避超时，递增计数并重新计算退避
    // 实际的 SEC 查询由 MqttService 经 state change callback 触发
    StateChangeCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_info_.state == ConnectionState::CERT_WAIT) {
            credential_poll_count_++;
            calculateNextCredentialPoll();
            // 通知上层重新检查凭据状态（同状态回调）
            cb = callback_;
        }
    }
    if (cb) cb(ConnectionState::CERT_WAIT);
}

uint64_t ConnectionStateMachine::getConnectionGeneration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connection_generation_;
}

void ConnectionStateMachine::invalidateConnectionGeneration() {
    std::lock_guard<std::mutex> lock(mutex_);
    connection_generation_++;
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

// CR-005 §6: CERT_WAIT 独立退避（不与 Broker 网络重连计数混用）
void ConnectionStateMachine::calculateNextCredentialPoll() {
    // 指数退避：initial_backoff * 2^min(count, 6) capped at max_backoff
    auto delay = credential_poll_config_.initial_backoff *
                 (1 << std::min(credential_poll_count_, 6u));
    delay = std::min(delay, credential_poll_config_.max_backoff);

    auto jitter_ms = delay.count() / 10;
    if (jitter_ms > 0) {
        std::uniform_int_distribution<int64_t> dist(-jitter_ms, jitter_ms);
        auto jitter = dist(rng_);
        delay += std::chrono::milliseconds(jitter);
    }

    next_credential_poll_at_ = std::chrono::steady_clock::now() + delay;
}

// CR-006 §4: PROV_WAIT 独立退避（不与 CERT_WAIT / 网络重连混用）
void ConnectionStateMachine::calculateNextProvisionPoll() {
    // 指数退避 with configurable multiplier (CR-006 §9)
    double multiplier = provision_retry_config_.multiplier;
    if (multiplier < 1.0) multiplier = 1.0;

    double delay_ms = static_cast<double>(provision_retry_config_.initial_backoff.count());
    for (uint32_t i = 0; i < std::min(provision_poll_count_, 6u); ++i) {
        delay_ms *= multiplier;
    }
    auto delay = Duration(static_cast<int64_t>(delay_ms));
    delay = std::min(delay, provision_retry_config_.max_backoff);

    auto jitter_ms = delay.count() / 10;
    if (jitter_ms > 0) {
        std::uniform_int_distribution<int64_t> dist(-jitter_ms, jitter_ms);
        auto jitter = dist(rng_);
        delay += std::chrono::milliseconds(jitter);
    }

    next_provision_poll_at_ = std::chrono::steady_clock::now() + delay;
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
        } else if (state_info_.state == ConnectionState::CERT_WAIT) {
            // CR-005 §6: CERT_WAIT 独立退避定时器
            auto wake_time = next_credential_poll_at_;
            bool predicate_changed = timer_cv_.wait_until(lock, wake_time, [this] {
                return !timer_running_ ||
                       state_info_.state != ConnectionState::CERT_WAIT;
            });

            if (!timer_running_) break;
            if (predicate_changed) continue; // State changed externally, re-evaluate

            // Timeout expired and state is still CERT_WAIT -> trigger credential poll.
            lock.unlock();
            onCredentialPollTimeout();
            lock.lock();
        } else if (state_info_.state == ConnectionState::PROV_WAIT) {
            // CR-006 §4: PROV_WAIT 独立退避定时器
            auto wake_time = next_provision_poll_at_;
            bool predicate_changed = timer_cv_.wait_until(lock, wake_time, [this] {
                return !timer_running_ ||
                       state_info_.state != ConnectionState::PROV_WAIT;
            });

            if (!timer_running_) break;
            if (predicate_changed) continue; // State changed externally, re-evaluate

            // Timeout expired and state is still PROV_WAIT -> trigger provision poll.
            lock.unlock();
            onProvisionPollTimeout();
            lock.lock();
        } else {
            // Not in RECONNECTING or CERT_WAIT; sleep until state changes or we stop
            timer_cv_.wait(lock, [this] {
                return !timer_running_ ||
                       state_info_.state == ConnectionState::RECONNECTING ||
                       state_info_.state == ConnectionState::CERT_WAIT ||
                       state_info_.state == ConnectionState::PROV_WAIT;
            });
        }
    }
}

} // namespace mqtt
} // namespace tbox
