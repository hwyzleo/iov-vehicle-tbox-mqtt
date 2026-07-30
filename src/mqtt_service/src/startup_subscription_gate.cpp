// StartupSubscriptionGate (CR-007 §2, §6)
#include "startup_subscription_gate.h"
#include <algorithm>

namespace tbox {
namespace mqtt {

void StartupSubscriptionGate::configure(const std::vector<std::string>& required_owners,
                                        Duration timeout) {
    std::lock_guard<std::mutex> lock(mutex_);
    required_owners_ = required_owners;
    timeout_ = timeout;
}

void StartupSubscriptionGate::startWait() {
    std::lock_guard<std::mutex> lock(mutex_);
    start_time_ = Clock::now();
    waiting_ = true;
}

void StartupSubscriptionGate::onSnapshotAccepted(const std::string& owner,
                                                 bool registration_complete) {
    std::lock_guard<std::mutex> lock(mutex_);
    bool is_required = std::find(required_owners_.begin(), required_owners_.end(),
                                 owner) != required_owners_.end();
    if (!is_required) return;
    if (registration_complete) {
        completed_owners_.insert(owner);
    } else {
        completed_owners_.erase(owner);
    }
}

bool StartupSubscriptionGate::isSatisfied() const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& owner : required_owners_) {
        if (completed_owners_.find(owner) == completed_owners_.end()) {
            return false;
        }
    }
    return true;
}

bool StartupSubscriptionGate::isTimedOut() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!waiting_) return false;
    // 检查是否已满足
    bool satisfied = true;
    for (const auto& owner : required_owners_) {
        if (completed_owners_.find(owner) == completed_owners_.end()) {
            satisfied = false;
            break;
        }
    }
    if (satisfied) return false;
    return (Clock::now() - start_time_) >= timeout_;
}

std::vector<std::string> StartupSubscriptionGate::missingOwners() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> missing;
    for (const auto& owner : required_owners_) {
        if (completed_owners_.find(owner) == completed_owners_.end()) {
            missing.push_back(owner);
        }
    }
    return missing;
}

Duration StartupSubscriptionGate::elapsed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!waiting_) return Duration(0);
    return std::chrono::duration_cast<Duration>(Clock::now() - start_time_);
}

void StartupSubscriptionGate::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    completed_owners_.clear();
    waiting_ = false;
}

} // namespace mqtt
} // namespace tbox
