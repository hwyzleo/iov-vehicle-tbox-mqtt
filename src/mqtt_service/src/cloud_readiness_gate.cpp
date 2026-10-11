// CloudReadinessGate (CR-007 §2, §10; CR-014 §3, §4.2, §12.5)
#include "cloud_readiness_gate.h"

namespace tbox {
namespace mqtt {

void CloudReadinessGate::setCallback(ChangeCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(cb);
}

void CloudReadinessGate::onConnectionState(ConnectionState state,
                                           uint64_t connection_generation) {
    ChangeCallback cb;
    CloudReadinessSnapshot snap;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connection_state_ = state;
        connection_generation_ = connection_generation;
        // 连接断开或离开 connected 态时退出 Cloud Ready (CR-007 §10)
        if (!isConnectedState(state)) {
            subscriptions_ready_ = false;
        }
        const std::string reason =
            isConnectedState(state)
                ? (state == ConnectionState::CLOUD_READY ? "cloud_ready" : "state_changed")
                : "connection_lost";
        changed = evaluateLocked(reason, snap);
        if (changed) cb = callback_;
    }
    if (cb) cb(snap);
}

void CloudReadinessGate::setOwnerGenerations(
        const std::map<std::string, uint64_t>& gens) {
    ChangeCallback cb;
    CloudReadinessSnapshot snap;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        owner_generations_ = gens;
        // required owner generation 变化时若已在 Cloud Ready 需重评估 (CR-007 §10)
        changed = evaluateLocked("owner_generation_changed", snap);
        if (changed) cb = callback_;
    }
    if (cb) cb(snap);
}

void CloudReadinessGate::onSubscriptionsReady(bool ready, uint32_t mandatory_active,
                                              uint32_t mandatory_total) {
    ChangeCallback cb;
    CloudReadinessSnapshot snap;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        subscriptions_ready_ = ready;
        mandatory_active_ = mandatory_active;
        mandatory_total_ = mandatory_total;
        changed = evaluateLocked(ready ? "subscriptions_ready" : "subscriptions_not_ready",
                                 snap);
        if (changed) cb = callback_;
    }
    if (cb) cb(snap);
}

void CloudReadinessGate::onMandatoryFailed(const std::string& reason) {
    ChangeCallback cb;
    CloudReadinessSnapshot snap;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        subscriptions_ready_ = false;
        changed = evaluateLocked(reason, snap);
        if (changed) cb = callback_;
    }
    if (cb) cb(snap);
}

void CloudReadinessGate::setDegraded(bool degraded, const std::string& reason) {
    ChangeCallback cb;
    CloudReadinessSnapshot snap;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        degraded_ = degraded;
        changed = evaluateLocked(reason, snap);
        if (changed) cb = callback_;
    }
    if (cb) cb(snap);
}

void CloudReadinessGate::setTlsSessionIdentity(const TlsSessionIdentity& identity) {
    ChangeCallback cb;
    CloudReadinessSnapshot snap;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        identity_ = identity;
        identity_.valid = true;
        // 新 TLS 会话：强制重新收敛，旧 SUBACK 不得激活新 generation (CR-014 §4.1)
        subscriptions_ready_ = false;
        degraded_ = false;
        changed = evaluateLocked("tls_session_established", snap);
        if (changed) cb = callback_;
    }
    if (cb) cb(snap);
}

void CloudReadinessGate::revokeSession(const std::string& reason) {
    ChangeCallback cb;
    CloudReadinessSnapshot snap;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 先提交并发布旧 generation 的 cloud_ready=false，快照仍携带旧身份 (CR-014 §4.2)
        subscriptions_ready_ = false;
        degraded_ = false;
        changed = evaluateLocked(reason, snap);
        if (changed) cb = callback_;
    }
    if (cb) cb(snap);
}

bool CloudReadinessGate::evaluateLocked(const std::string& reason,
                                        CloudReadinessSnapshot& out) {
    bool new_ready = isConnectedState(connection_state_) &&
                     subscriptions_ready_ &&
                     !degraded_ &&
                     connection_state_ != ConnectionState::CONNECTED_DEGRADED;
    last_reason_ = reason;
    cloud_ready_ = new_ready;

    CloudReadinessSnapshot next = buildSnapshotLocked(new_ready, reason);
    if (!has_committed_ || snapshotChanged(committed_, next)) {
        next.event_sequence = ++event_sequence_;
        committed_ = next;
        has_committed_ = true;
        out = next;
        return true;
    }
    out = committed_;
    return false;
}

CloudReadinessSnapshot CloudReadinessGate::buildSnapshotLocked(
        bool ready, const std::string& reason) const {
    CloudReadinessSnapshot s;
    s.cloud_ready = ready;
    s.connection_generation = connection_generation_;
    s.cert_session_generation = identity_.cert_session_generation;
    s.credential_id = identity_.credential_id;
    s.credential_version = identity_.credential_version;
    s.device_identity_digest = identity_.device_identity_digest;
    s.certificate_ref = identity_.certificate_ref;
    s.reason_code = ready ? "cloud_ready" : reason;
    s.event_sequence = event_sequence_;
    s.subscriptions_ready = subscriptions_ready_;
    s.connected = isConnectedState(connection_state_);
    s.owner_generations = owner_generations_;
    s.mandatory_count = mandatory_total_;
    s.mandatory_active = mandatory_active_;
    s.contract_version = kCloudReadinessContractVersion;
    s.contract_supported = true;
    return s;
}

bool CloudReadinessGate::snapshotChanged(const CloudReadinessSnapshot& a,
                                         const CloudReadinessSnapshot& b) {
    return a.cloud_ready != b.cloud_ready ||
           a.connection_generation != b.connection_generation ||
           a.cert_session_generation != b.cert_session_generation ||
           a.credential_id != b.credential_id ||
           a.credential_version != b.credential_version ||
           a.device_identity_digest != b.device_identity_digest ||
           a.certificate_ref != b.certificate_ref ||
           a.reason_code != b.reason_code ||
           a.subscriptions_ready != b.subscriptions_ready ||
           a.connected != b.connected ||
           a.owner_generations != b.owner_generations ||
           a.mandatory_count != b.mandatory_count ||
           a.mandatory_active != b.mandatory_active;
}

bool CloudReadinessGate::isCloudReady() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cloud_ready_;
}

CloudReadinessSnapshot CloudReadinessGate::getReadiness() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (has_committed_) return committed_;
    return buildSnapshotLocked(cloud_ready_, last_reason_);
}

} // namespace mqtt
} // namespace tbox
