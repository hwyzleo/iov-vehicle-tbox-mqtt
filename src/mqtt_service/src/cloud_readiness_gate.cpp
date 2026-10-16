// CloudReadinessGate (CR-007 §2, §10; CR-014 §3, §4.2, §12.5; CR-015 §18.3; CR-017 §3)
#include "cloud_readiness_gate.h"
#include "daemon_epoch.h"

namespace tbox {
namespace mqtt {

CloudReadinessGate::CloudReadinessGate()
    : daemon_epoch_(daemonEpoch()),
      daemon_epoch_valid_(!daemon_epoch_.empty()) {
}

void CloudReadinessGate::setCallback(ChangeCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(cb);
}

void CloudReadinessGate::setCapabilityCallback(CapabilityChangeCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    capability_callback_ = std::move(cb);
}

void CloudReadinessGate::onConnectionState(ConnectionState state,
                                           uint64_t connection_generation) {
    ChangeCallback cb;
    CapabilityChangeCallback ccb;
    CloudReadinessSnapshot snap;
    std::vector<CapabilityReadiness> cap_events;
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
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (cb) cb(snap);
    if (ccb) for (const auto& e : cap_events) ccb(e);
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
    CapabilityChangeCallback ccb;
    CloudReadinessSnapshot snap;
    std::vector<CapabilityReadiness> cap_events;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        subscriptions_ready_ = ready;
        mandatory_active_ = mandatory_active;
        mandatory_total_ = mandatory_total;
        changed = evaluateLocked(ready ? "subscriptions_ready" : "subscriptions_not_ready",
                                 snap);
        if (changed) cb = callback_;
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (cb) cb(snap);
    if (ccb) for (const auto& e : cap_events) ccb(e);
}

void CloudReadinessGate::onMandatoryFailed(const std::string& reason) {
    ChangeCallback cb;
    CapabilityChangeCallback ccb;
    CloudReadinessSnapshot snap;
    std::vector<CapabilityReadiness> cap_events;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        subscriptions_ready_ = false;
        changed = evaluateLocked(reason, snap);
        if (changed) cb = callback_;
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (cb) cb(snap);
    if (ccb) for (const auto& e : cap_events) ccb(e);
}

void CloudReadinessGate::setDegraded(bool degraded, const std::string& reason) {
    ChangeCallback cb;
    CapabilityChangeCallback ccb;
    CloudReadinessSnapshot snap;
    std::vector<CapabilityReadiness> cap_events;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        degraded_ = degraded;
        changed = evaluateLocked(reason, snap);
        if (changed) cb = callback_;
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (cb) cb(snap);
    if (ccb) for (const auto& e : cap_events) ccb(e);
}

void CloudReadinessGate::setTlsSessionIdentity(const TlsSessionIdentity& identity) {
    ChangeCallback cb;
    CapabilityChangeCallback ccb;
    CloudReadinessSnapshot snap;
    std::vector<CapabilityReadiness> cap_events;
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
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (cb) cb(snap);
    if (ccb) for (const auto& e : cap_events) ccb(e);
}

void CloudReadinessGate::revokeSession(const std::string& reason) {
    ChangeCallback cb;
    CapabilityChangeCallback ccb;
    CloudReadinessSnapshot snap;
    std::vector<CapabilityReadiness> cap_events;
    bool changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 先提交并发布旧 generation 的 cloud_ready=false，快照仍携带旧身份 (CR-014 §4.2)
        subscriptions_ready_ = false;
        degraded_ = false;
        changed = evaluateLocked(reason, snap);
        if (changed) cb = callback_;
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (cb) cb(snap);
    if (ccb) for (const auto& e : cap_events) ccb(e);
}

// ============================================================
// CR-015 §18.3: capability-scoped readiness
// ============================================================

void CloudReadinessGate::setCapabilityState(const std::string& capability,
                                            bool enabled,
                                            bool contract_supported,
                                            bool snapshot_accepted,
                                            bool uplink_ready,
                                            uint64_t owner_generation) {
    CapabilityChangeCallback ccb;
    std::vector<CapabilityReadiness> cap_events;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& c = capabilities_[capability];
        if (c.capability.empty()) c.capability = capability;
        c.enabled = enabled;
        c.contract_supported = contract_supported;
        c.snapshot_accepted = snapshot_accepted;
        c.uplink_ready = uplink_ready;
        c.owner_generation = owner_generation;
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (ccb) for (const auto& e : cap_events) ccb(e);
}

void CloudReadinessGate::setCapabilityDownlinkReady(const std::string& capability,
                                                    bool ready,
                                                    bool rejected) {
    CapabilityChangeCallback ccb;
    std::vector<CapabilityReadiness> cap_events;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& c = capabilities_[capability];
        if (c.capability.empty()) c.capability = capability;
        c.downlink_suback_ready = ready;
        capability_downlink_rejected_[capability] = (rejected && !ready);
        reevaluateCapabilitiesLocked(cap_events);
        if (!cap_events.empty()) ccb = capability_callback_;
    }
    if (ccb) for (const auto& e : cap_events) ccb(e);
}

void CloudReadinessGate::reevaluateCapabilitiesLocked(
        std::vector<CapabilityReadiness>& events) {
    for (auto& [cap, current] : capabilities_) {
        CapabilityReadiness next = current;
        next.connection_generation = connection_generation_;
        next.cert_session_generation = identity_.cert_session_generation;
        // ready = cloud_ready && enabled && contract_supported && snapshot_accepted
        //         && uplink_ready && downlink_suback_ready (DSN-CR-015 §18.3)
        next.ready = cloud_ready_ && current.enabled && current.contract_supported &&
                     current.snapshot_accepted && current.uplink_ready &&
                     current.downlink_suback_ready;
        bool rejected = false;
        auto rit = capability_downlink_rejected_.find(cap);
        if (rit != capability_downlink_rejected_.end()) rejected = rit->second;
        next.reason_code = capabilityReasonFor(cap, current.enabled,
                                               current.contract_supported,
                                               current.snapshot_accepted,
                                               current.uplink_ready,
                                               current.downlink_suback_ready,
                                               rejected);
        if (capabilityChanged(current, next)) {
            next.event_sequence = ++capability_sequences_[cap];
            current = next;
            events.push_back(next);
        }
    }
}

bool CloudReadinessGate::capabilityChanged(const CapabilityReadiness& a,
                                           const CapabilityReadiness& b) {
    return a.ready != b.ready ||
           a.enabled != b.enabled ||
           a.contract_supported != b.contract_supported ||
           a.snapshot_accepted != b.snapshot_accepted ||
           a.uplink_ready != b.uplink_ready ||
           a.downlink_suback_ready != b.downlink_suback_ready ||
           a.owner_generation != b.owner_generation ||
           a.connection_generation != b.connection_generation ||
           a.cert_session_generation != b.cert_session_generation ||
           a.reason_code != b.reason_code;
}

std::string CloudReadinessGate::capabilityReasonFor(
        const std::string& cap, bool enabled, bool contract_supported,
        bool snapshot_accepted, bool uplink_ready, bool downlink_suback_ready,
        bool downlink_rejected) {
    const bool keyprov = (cap == kCapabilitySecurityKeyprov);
    if (!enabled) {
        return keyprov ? capability_reason::kKeyprovDisabled : "CAPABILITY_DISABLED";
    }
    if (!contract_supported) {
        return keyprov ? capability_reason::kKeyprovContractUnsupported
                       : "CAPABILITY_CONTRACT_UNSUPPORTED";
    }
    if (!snapshot_accepted) {
        return keyprov ? capability_reason::kKeyprovSnapshotRejected
                       : "CAPABILITY_SNAPSHOT_REJECTED";
    }
    if (!uplink_ready) {
        return keyprov ? capability_reason::kKeyprovSubscribePending
                       : "CAPABILITY_UPLINK_PENDING";
    }
    if (!downlink_suback_ready) {
        if (downlink_rejected) {
            return keyprov ? capability_reason::kKeyprovSubackRejected
                           : "CAPABILITY_SUBACK_REJECTED";
        }
        return keyprov ? capability_reason::kKeyprovSubscribePending
                       : "CAPABILITY_SUBSCRIBE_PENDING";
    }
    return "READY";
}

CapabilityReadiness CloudReadinessGate::getCapabilityReadiness(
        const std::string& capability) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = capabilities_.find(capability);
    if (it == capabilities_.end()) {
        CapabilityReadiness r;
        r.capability = capability;
        return r;
    }
    return it->second;
}

std::map<std::string, CapabilityReadiness>
CloudReadinessGate::getCapabilities() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return capabilities_;
}

// ============================================================
// 内部评估
// ============================================================

bool CloudReadinessGate::evaluateLocked(const std::string& reason,
                                        CloudReadinessSnapshot& out) {
    // CR-017 §3: daemon epoch 随机源失败时不得宣告身份绑定能力就绪。
    // CR-017 §3/§19: 有效身份上下文 = daemon epoch 可用且 connection generation 非零。
    // generation==0 表示尚未提交任何可发布连接上下文，此时不得宣告身份就绪。
    const bool identity_context_valid =
        daemon_epoch_valid_ && connection_generation_ != 0;
    bool new_ready = isConnectedState(connection_state_) &&
                     subscriptions_ready_ &&
                     !degraded_ &&
                     identity_context_valid &&
                     connection_state_ != ConnectionState::CONNECTED_DEGRADED;
    // CR-017 §3: epoch/上下文不可用时给出明确、可区分的 identity reason
    const std::string effective_reason = identity_context_valid
        ? reason
        : session_reason::kSessionIdentityInvalid;
    last_reason_ = effective_reason;
    cloud_ready_ = new_ready;

    CloudReadinessSnapshot next = buildSnapshotLocked(new_ready, effective_reason);
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
    s.publish_contract_version = kPublishSessionContractVersion;
    s.contract_supported = true;
    // CR-017 §4: 会话身份投影与可校验上下文（与指纹/generation 同一不可变对象产生）
    s.ecu_uid = identity_.ecu_uid;
    s.session_context.daemon_epoch = daemon_epoch_;
    s.session_context.connection_generation = connection_generation_;
    s.session_context.cert_session_generation = identity_.cert_session_generation;
    s.capabilities = capabilities_;
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
           a.ecu_uid != b.ecu_uid ||
           a.session_context.daemon_epoch != b.session_context.daemon_epoch ||
           a.session_context.connection_generation != b.session_context.connection_generation ||
           a.session_context.cert_session_generation != b.session_context.cert_session_generation ||
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
    CloudReadinessSnapshot s = has_committed_
        ? committed_
        : buildSnapshotLocked(cloud_ready_, last_reason_);
    // CR-015: capability map 与独立查询读取同一原子状态
    s.capabilities = capabilities_;
    return s;
}

std::string CloudReadinessGate::getDaemonEpoch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return daemon_epoch_;
}

bool CloudReadinessGate::validateSession(const SessionContext& expected) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!daemon_epoch_valid_ || !identity_.valid) return false;
    return expected.daemon_epoch == daemon_epoch_ &&
           expected.connection_generation == connection_generation_ &&
           expected.cert_session_generation == identity_.cert_session_generation;
}

} // namespace mqtt
} // namespace tbox
