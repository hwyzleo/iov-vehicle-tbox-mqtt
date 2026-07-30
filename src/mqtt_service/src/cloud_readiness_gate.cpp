// CloudReadinessGate (CR-007 §2, §10)
#include "cloud_readiness_gate.h"

namespace tbox {
namespace mqtt {

void CloudReadinessGate::setCallback(ChangeCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(cb);
}

void CloudReadinessGate::onConnectionState(ConnectionState state, uint64_t connection_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    connection_state_ = state;
    connection_generation_ = connection_generation;
    // 连接断开或离开 connected 态时退出 Cloud Ready (CR-007 §10)
    if (!isConnectedState(state)) {
        subscriptions_ready_ = false;
        evaluate("connection_lost");
    } else {
        evaluate(state == ConnectionState::CLOUD_READY ? "cloud_ready" : "state_changed");
    }
}

void CloudReadinessGate::setOwnerGenerations(const std::map<std::string, uint64_t>& gens) {
    std::lock_guard<std::mutex> lock(mutex_);
    owner_generations_ = gens;
    // required owner generation 变化时若已在 Cloud Ready 需重评估 (CR-007 §10)
    evaluate("owner_generation_changed");
}

void CloudReadinessGate::onSubscriptionsReady(bool ready, uint32_t mandatory_active,
                                              uint32_t mandatory_total) {
    std::lock_guard<std::mutex> lock(mutex_);
    subscriptions_ready_ = ready;
    mandatory_active_ = mandatory_active;
    mandatory_total_ = mandatory_total;
    evaluate(ready ? "subscriptions_ready" : "subscriptions_not_ready");
}

void CloudReadinessGate::onMandatoryFailed(const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    subscriptions_ready_ = false;
    evaluate(reason);
}

void CloudReadinessGate::setDegraded(bool degraded, const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    degraded_ = degraded;
    evaluate(reason);
}

void CloudReadinessGate::evaluate(const std::string& reason) {
    bool new_ready = isConnectedState(connection_state_) &&
                     subscriptions_ready_ &&
                     !degraded_ &&
                     connection_state_ != ConnectionState::CONNECTED_DEGRADED;
    last_reason_ = reason;
    if (new_ready != cloud_ready_) {
        cloud_ready_ = new_ready;
        if (callback_) {
            callback_(cloud_ready_, mandatory_active_, mandatory_total_,
                      new_ready ? "cloud_ready" : reason);
        }
    }
}

bool CloudReadinessGate::isCloudReady() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cloud_ready_;
}

SubscriptionReadiness CloudReadinessGate::getReadiness() const {
    std::lock_guard<std::mutex> lock(mutex_);
    SubscriptionReadiness r;
    r.cloud_ready = cloud_ready_;
    r.subscriptions_ready = subscriptions_ready_;
    r.connected = isConnectedState(connection_state_);
    r.connection_generation = connection_generation_;
    r.owner_generations = owner_generations_;
    r.mandatory_count = mandatory_total_;
    r.mandatory_active = mandatory_active_;
    r.reason_code = cloud_ready_ ? "" : last_reason_;
    return r;
}

} // namespace mqtt
} // namespace tbox
