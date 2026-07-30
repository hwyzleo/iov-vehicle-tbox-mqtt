// AppliedSubscriptionManifest (CR-007 §2, §3.1)
#include "applied_subscription_manifest.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <nlohmann/json.hpp>
#include <chrono>

namespace tbox {
namespace mqtt {

void AppliedSubscriptionManifestStore::setStore(std::shared_ptr<hwyz::store::Store> store) {
    store_ = store;
}

bool AppliedSubscriptionManifestStore::save(const AppliedSubscriptionManifest& manifest) {
    if (!store_ || !store_->isReady()) {
        return false;
    }
    try {
        nlohmann::json j;
        j["client_id_hash"] = manifest.client_id_hash;
        j["identity_generation"] = manifest.identity_generation;
        j["broker_manifest_version"] = manifest.broker_manifest_version;
        j["updated_at"] = manifest.updated_at;
        nlohmann::json topics = nlohmann::json::object();
        for (const auto& [topic, qos] : manifest.active_topics) {
            topics[topic] = static_cast<int>(qos);
        }
        j["active_topics"] = topics;
        store_->save<std::string>(StoreKeys::kSubsManifest, j.dump());
        return true;
    } catch (const std::exception& e) {
        MqttLogAdapter::subscribe().warn(
            "mqtt.subscription.manifest_save_failed",
            "已应用清单持久化失败",
            {{"reason", tbox::fw::log::FieldValue::makeString(e.what())}}
        );
        return false;
    }
}

AppliedSubscriptionManifest AppliedSubscriptionManifestStore::load() const {
    AppliedSubscriptionManifest manifest;
    if (!store_ || !store_->isReady()) {
        return manifest;  // 空
    }
    try {
        std::string blob = store_->loadOr<std::string>(StoreKeys::kSubsManifest, "");
        if (blob.empty()) {
            return manifest;  // 缺失
        }
        auto j = nlohmann::json::parse(blob);
        manifest.client_id_hash = j.value("client_id_hash", "");
        manifest.identity_generation = j.value("identity_generation", 0ULL);
        manifest.broker_manifest_version = j.value("broker_manifest_version", 0ULL);
        manifest.updated_at = j.value("updated_at", "");
        if (j.contains("active_topics") && j["active_topics"].is_object()) {
            for (auto it = j["active_topics"].begin(); it != j["active_topics"].end(); ++it) {
                manifest.active_topics[it.key()] = static_cast<uint8_t>(it.value().get<int>());
            }
        }
    } catch (const std::exception& e) {
        // 损坏：返回空清单，reconciler 保守全量重订阅 (CR-007 §8.2)
        MqttLogAdapter::subscribe().warn(
            "mqtt.subscription.manifest_corrupt",
            "已应用清单损坏，将全量重订阅",
            {{"reason", tbox::fw::log::FieldValue::makeString(e.what())}}
        );
        return AppliedSubscriptionManifest{};
    }
    return manifest;
}

void AppliedSubscriptionManifestStore::clear() {
    if (!store_ || !store_->isReady()) {
        return;
    }
    try {
        store_->save<std::string>(StoreKeys::kSubsManifest, "");
    } catch (const std::exception&) {
        // 忽略
    }
}

bool AppliedSubscriptionManifestStore::isIdentityMatch(const std::string& client_id_hash,
                                                  uint64_t identity_generation) const {
    auto manifest = load();
    if (manifest.client_id_hash.empty() && manifest.active_topics.empty()) {
        return false;  // 无清单
    }
    return manifest.client_id_hash == client_id_hash &&
           manifest.identity_generation == identity_generation;
}

} // namespace mqtt
} // namespace tbox
