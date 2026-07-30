// EffectiveSubscriptionTable (CR-007 §2, §3, §5)
#include "effective_subscription_table.h"
#include <algorithm>

namespace tbox {
namespace mqtt {

EffectiveSubscriptionTable::EffectiveSubscriptionTable()
    : downstream_(std::make_shared<const std::map<std::string, EffectiveSubscription>>()),
      upstream_(std::make_shared<const std::vector<UpstreamRoute>>()) {
}

EffectiveSubscriptionTable::~EffectiveSubscriptionTable() = default;

void EffectiveSubscriptionTable::setEcuUid(const std::string& ecu_uid) {
    std::lock_guard<std::mutex> lock(mutex_);
    ecu_uid_ = ecu_uid;
}

std::string EffectiveSubscriptionTable::expandTopic(const std::string& topic_template) const {
    std::string out = topic_template;
    const std::string placeholder = "{ecu_uid}";
    size_t pos = 0;
    while ((pos = out.find(placeholder, pos)) != std::string::npos) {
        out.replace(pos, placeholder.size(), ecu_uid_);
        pos += ecu_uid_.size();
    }
    return out;
}

std::shared_ptr<const std::map<std::string, EffectiveSubscription>>
EffectiveSubscriptionTable::recompute(const OwnerSnapshotRegistry& registry) {
    auto all = registry.getAllLatestSnapshots();

    auto down = std::make_shared<std::map<std::string, EffectiveSubscription>>();
    auto up = std::make_shared<std::vector<UpstreamRoute>>();

    std::string ecu_uid;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ecu_uid = ecu_uid_;
    }

    for (const auto& [owner, snap] : all) {
        for (const auto& item : snap.items) {
            // 展开模板（ecu_uid 为空时保留占位，reconciler 会因无法订阅而失败）
            std::string topic = item.topic_template;
            const std::string placeholder = "{ecu_uid}";
            size_t pos = 0;
            while ((pos = topic.find(placeholder, pos)) != std::string::npos) {
                topic.replace(pos, placeholder.size(), ecu_uid);
                pos += ecu_uid.size();
            }
            if (topic.empty()) continue;

            // 下行订阅：DOWN / BIDIRECTIONAL
            if (item.direction == SnapshotDirection::DOWN ||
                item.direction == SnapshotDirection::BIDIRECTIONAL) {
                auto& eff = (*down)[topic];
                if (eff.expanded_topic.empty()) {
                    eff.expanded_topic = topic;
                    eff.topic_template = item.topic_template;
                    eff.effective_qos = item.qos;
                } else {
                    eff.effective_qos = std::max(eff.effective_qos, item.qos);
                }
                eff.mandatory = eff.mandatory || item.mandatory;
                eff.owners.insert(owner);
                eff.targets.push_back({owner, item.target, item.route_id});
            }

            // 上行路由：UP / BIDIRECTIONAL
            if (item.direction == SnapshotDirection::UP ||
                item.direction == SnapshotDirection::BIDIRECTIONAL) {
                UpstreamRoute r;
                r.internal_addr = item.route_id.empty() ? item.target : item.route_id;
                r.mqtt_topic = topic;
                r.qos = item.qos;
                up->push_back(std::move(r));
            }
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    downstream_ = down;
    upstream_ = up;
    return down;
}

std::shared_ptr<const std::map<std::string, EffectiveSubscription>>
EffectiveSubscriptionTable::downstreamSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return downstream_;
}

std::vector<UpstreamRoute> EffectiveSubscriptionTable::upstreamRoutes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return *upstream_;
}

uint32_t EffectiveSubscriptionTable::mandatoryCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint32_t count = 0;
    for (const auto& [topic, eff] : *downstream_) {
        if (eff.mandatory) ++count;
    }
    return count;
}

void EffectiveSubscriptionTable::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    downstream_ = std::make_shared<const std::map<std::string, EffectiveSubscription>>();
    upstream_ = std::make_shared<const std::vector<UpstreamRoute>>();
}

} // namespace mqtt
} // namespace tbox
