// BrokerSubscriptionReconciler (CR-007 §2, §8)
#include "broker_subscription_reconciler.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"

namespace tbox {
namespace mqtt {

BrokerSubscriptionReconciler::BrokerSubscriptionReconciler() = default;
BrokerSubscriptionReconciler::~BrokerSubscriptionReconciler() = default;

void BrokerSubscriptionReconciler::setBrokerOperations(BrokerOperations* ops) {
    broker_ops_ = ops;
}

void BrokerSubscriptionReconciler::setManifestStore(AppliedSubscriptionManifestStore* manifest) {
    manifest_ = manifest;
}

void BrokerSubscriptionReconciler::setReconcileTimeout(Duration timeout) {
    reconcile_timeout_ = timeout;
}

void BrokerSubscriptionReconciler::setSubackRetryMax(uint32_t max) {
    suback_retry_max_ = max;
}

void BrokerSubscriptionReconciler::sendSubscribe(const std::string& topic, uint8_t qos,
                                                 uint64_t gen) {
    if (!broker_ops_) return;
    bool ok = broker_ops_->brokerSubscribe(topic, qos);
    auto& ts = topics_[topic];
    ts.qos = qos;
    ts.state = ok ? BrokerSubscriptionState::SUBSCRIBING : BrokerSubscriptionState::FAILED;
    ts.reconcile_generation = gen;
    ts.last_attempt = std::chrono::steady_clock::now();
}

void BrokerSubscriptionReconciler::sendUnsubscribe(const std::string& topic, uint64_t gen) {
    if (!broker_ops_) return;
    bool ok = broker_ops_->brokerUnsubscribe(topic);
    auto& ts = topics_[topic];
    ts.state = ok ? BrokerSubscriptionState::UNSUBSCRIBING : BrokerSubscriptionState::FAILED;
    ts.reconcile_generation = gen;
    ts.last_attempt = std::chrono::steady_clock::now();
}

ReconcileResult BrokerSubscriptionReconciler::reconcile(
        std::shared_ptr<const std::map<std::string, EffectiveSubscription>> desired,
        bool session_present,
        const std::string& client_id_hash,
        uint64_t identity_generation) {
    std::lock_guard<std::mutex> lock(mutex_);
    ReconcileResult result;
    result.session_present = session_present;

    ++reconcile_generation_;
    uint64_t gen = reconcile_generation_;

    // 更新 mandatory 集合（CR-015: eff.mandatory 仅表示 GLOBAL_MANDATORY）
    mandatory_topics_.clear();
    capability_scopes_.clear();
    for (const auto& [topic, eff] : *desired) {
        if (eff.mandatory) mandatory_topics_.insert(topic);
        if (!eff.readiness_scope.empty()) capability_scopes_.insert(eff.readiness_scope);
    }

    // 判定 manifest 是否可用于 session 恢复
    bool manifest_valid = false;
    std::map<std::string, uint8_t> applied;
    if (session_present && manifest_) {
        manifest_valid = manifest_->isIdentityMatch(client_id_hash, identity_generation);
        if (manifest_valid) {
            applied = manifest_->load().active_topics;
        }
    }
    result.manifest_valid = manifest_valid;

    if (!session_present || !manifest_valid) {
        // CR-007 §8.1/§8.2: 全量订阅（session 不存在或 manifest 不可信）
        // 对不在 desired 的既有 active topic 发 UNSUBSCRIBE（session_present=true 但 manifest 无效时清理）
        if (session_present && !manifest_valid) {
            for (auto& [topic, ts] : topics_) {
                if (desired->find(topic) == desired->end() &&
                    ts.state != BrokerSubscriptionState::UNSUBSCRIBING) {
                    sendUnsubscribe(topic, gen);
                    ++result.unsubscribe_count;
                }
            }
        } else {
            // session_present=false: Broker 已清空，本地状态重置
            topics_.clear();
        }
        for (const auto& [topic, eff] : *desired) {
            sendSubscribe(topic, eff.effective_qos, gen);
            ++result.subscribe_count;
        }
    } else {
        // CR-007 §8.2: session_present=true 且 manifest identity 匹配，计算 diff
        for (const auto& [topic, eff] : *desired) {
            auto it = applied.find(topic);
            if (it == applied.end()) {
                // 新增
                sendSubscribe(topic, eff.effective_qos, gen);
                ++result.subscribe_count;
            } else if (eff.effective_qos != it->second) {
                // QoS 变化（升或降）：重订阅
                sendSubscribe(topic, eff.effective_qos, gen);
                ++result.subscribe_count;
            } else {
                // 无变化：视为 Broker Session 保留，置 ACTIVE（identity 已核对）
                auto& ts = topics_[topic];
                ts.qos = eff.effective_qos;
                ts.mandatory = eff.mandatory;
                ts.state = BrokerSubscriptionState::ACTIVE;
                ts.reconcile_generation = gen;
            }
        }
        // 删除：applied 中有但 desired 无
        for (const auto& [topic, qos] : applied) {
            if (desired->find(topic) == desired->end()) {
                sendUnsubscribe(topic, gen);
                ++result.unsubscribe_count;
            }
        }
    }

    // 同步 mandatory 标记与 capability scope 到 topics_ (CR-015)
    for (const auto& [topic, eff] : *desired) {
        topics_[topic].mandatory = eff.mandatory;
        topics_[topic].readiness_scope = eff.readiness_scope;
    }

    return result;
}

void BrokerSubscriptionReconciler::onSubAck(const std::string& topic, bool success) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) return;

    // 旧 generation 迟到 ACK：只记录，不激活新 generation (CR-007 §9)
    if (it->second.reconcile_generation != reconcile_generation_ &&
        it->second.state != BrokerSubscriptionState::SUBSCRIBING) {
        MqttLogAdapter::subscribe().debug(
            "mqtt.subscription.stale_ack",
            "收到旧 generation 迟到 SUBACK，忽略",
            {{"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(topic))}}
        );
        return;
    }

    if (success) {
        it->second.state = BrokerSubscriptionState::ACTIVE;
        // 提交 manifest
        if (manifest_) {
            AppliedSubscriptionManifest m = manifest_->load();
            m.active_topics[topic] = it->second.qos;
            m.broker_manifest_version++;
            m.updated_at = std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
            manifest_->save(m);
        }
    } else {
        it->second.state = BrokerSubscriptionState::FAILED;
        it->second.retries++;
        if (it->second.retries <= suback_retry_max_) {
            sendSubscribe(topic, it->second.qos, reconcile_generation_);
        }
    }
}

void BrokerSubscriptionReconciler::onUnsubAck(const std::string& topic, bool success) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) return;
    if (success) {
        topics_.erase(it);
        if (manifest_) {
            AppliedSubscriptionManifest m = manifest_->load();
            m.active_topics.erase(topic);
            m.broker_manifest_version++;
            manifest_->save(m);
        }
    } else {
        it->second.state = BrokerSubscriptionState::FAILED;
        it->second.retries++;
        if (it->second.retries <= suback_retry_max_) {
            sendUnsubscribe(topic, reconcile_generation_);
        }
    }
}

void BrokerSubscriptionReconciler::tick() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    for (auto& [topic, ts] : topics_) {
        if (ts.state == BrokerSubscriptionState::SUBSCRIBING ||
            ts.state == BrokerSubscriptionState::UNSUBSCRIBING) {
            if (now - ts.last_attempt >= reconcile_timeout_) {
                ts.retries++;
                if (ts.retries <= suback_retry_max_) {
                    if (ts.state == BrokerSubscriptionState::SUBSCRIBING) {
                        sendSubscribe(topic, ts.qos, reconcile_generation_);
                    } else {
                        sendUnsubscribe(topic, reconcile_generation_);
                    }
                } else {
                    ts.state = BrokerSubscriptionState::FAILED;
                }
            }
        }
    }
}

bool BrokerSubscriptionReconciler::isConverged() const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& topic : mandatory_topics_) {
        auto it = topics_.find(topic);
        if (it == topics_.end() || it->second.state != BrokerSubscriptionState::ACTIVE) {
            return false;
        }
    }
    return true;
}

uint32_t BrokerSubscriptionReconciler::mandatoryActive() const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint32_t count = 0;
    for (const auto& topic : mandatory_topics_) {
        auto it = topics_.find(topic);
        if (it != topics_.end() && it->second.state == BrokerSubscriptionState::ACTIVE) {
            ++count;
        }
    }
    return count;
}

uint32_t BrokerSubscriptionReconciler::mandatoryTotal() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<uint32_t>(mandatory_topics_.size());
}

bool BrokerSubscriptionReconciler::capabilityConverged(const std::string& readiness_scope) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (readiness_scope.empty()) return false;
    bool any = false;
    for (const auto& [topic, ts] : topics_) {
        if (ts.readiness_scope != readiness_scope) continue;
        any = true;
        if (ts.state != BrokerSubscriptionState::ACTIVE) return false;
    }
    return any;
}

bool BrokerSubscriptionReconciler::hasCapability(const std::string& readiness_scope) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return capability_scopes_.count(readiness_scope) > 0;
}

bool BrokerSubscriptionReconciler::capabilityRejected(const std::string& readiness_scope) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (readiness_scope.empty()) return false;
    for (const auto& [topic, ts] : topics_) {
        if (ts.readiness_scope != readiness_scope) continue;
        if (ts.state == BrokerSubscriptionState::FAILED) return true;
    }
    return false;
}

std::map<std::string, uint8_t> BrokerSubscriptionReconciler::activeTopics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<std::string, uint8_t> out;
    for (const auto& [topic, ts] : topics_) {
        if (ts.state == BrokerSubscriptionState::ACTIVE) {
            out[topic] = ts.qos;
        }
    }
    return out;
}

void BrokerSubscriptionReconciler::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    topics_.clear();
    mandatory_topics_.clear();
    capability_scopes_.clear();
}

} // namespace mqtt
} // namespace tbox
