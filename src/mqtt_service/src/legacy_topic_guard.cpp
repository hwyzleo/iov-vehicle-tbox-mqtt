// LegacyTopicGuard (CR-009 §10.2)
#include "legacy_topic_guard.h"

namespace tbox {
namespace mqtt {

LegacyTopicGuard::LegacyTopicGuard(size_t ecu_uid_layer_index)
    : ecu_uid_layer_index_(ecu_uid_layer_index) {
}

LegacyTopicGuard::~LegacyTopicGuard() = default;

std::string LegacyTopicGuard::extractLayer(const std::string& topic,
                                           size_t layer_index) const {
    size_t start = 0;
    size_t current = 0;
    for (size_t i = 0; i <= layer_index; ++i) {
        size_t end = topic.find('/', start);
        if (end == std::string::npos) {
            if (i == layer_index) {
                return topic.substr(start);
            }
            return "";
        }
        if (i == layer_index) {
            return topic.substr(start, end - start);
        }
        start = end + 1;
    }
    return "";
}

bool LegacyTopicGuard::validate(const std::string& topic,
                                const TopicTemplateResolver& resolver) const {
    // CR-009 §10.2: identity generation 当前有效
    if (!resolver.isIdentityValid()) {
        return false;
    }

    // CR-009 §10.2: 设备层等于当前 ecu_uid
    std::string device_layer = extractLayer(topic, ecu_uid_layer_index_);
    if (device_layer.empty()) {
        return false;
    }

    std::string current_ecu_uid = resolver.ecuUid();
    if (device_layer != current_ecu_uid) {
        return false;
    }

    return true;
}

bool LegacyTopicGuard::isRouteApiEnabled(const std::string& owner,
                                         const OwnerSnapshotRegistry& registry) const {
    auto snap = registry.getLatestSnapshot(owner);
    return snap != nullptr;
}

} // namespace mqtt
} // namespace tbox
