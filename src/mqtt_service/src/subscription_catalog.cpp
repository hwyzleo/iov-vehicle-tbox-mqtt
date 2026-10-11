// SubscriptionCatalog (DSN-CR-015 §3, §18.2)
#include "subscription_catalog.h"

namespace tbox {
namespace mqtt {

namespace {

const char* kKeyprovTemplateUp   = "vehicle/{ecu_uid}/up/keyprov";
const char* kKeyprovTemplateDown = "vehicle/{ecu_uid}/down/keyprov";

CatalogEntry makeKeyprovEntry(const std::string& route_id,
                              SnapshotDirection direction,
                              const std::string& topic_template,
                              const std::string& target) {
    CatalogEntry e;
    e.owner = "tsp";
    e.route_id = route_id;
    e.direction = direction;
    e.topic_template = topic_template;
    e.qos = 1;
    e.target = target;
    e.readiness_scope = kCapabilitySecurityKeyprov;
    e.criticality = SubscriptionCriticality::CAPABILITY_MANDATORY;
    e.persist_offline = false;  // keyprov 上行默认不持久化 (DSN-CR-015 §18.6)
    return e;
}

} // anonymous namespace

SubscriptionCatalog::SubscriptionCatalog() {
    CatalogEntry up = makeKeyprovEntry(kKeyprovUplinkRouteId,
                                       SnapshotDirection::UP,
                                       kKeyprovTemplateUp,
                                       /*target=*/"");
    CatalogEntry down = makeKeyprovEntry(kKeyprovDownlinkRouteId,
                                         SnapshotDirection::DOWN,
                                         kKeyprovTemplateDown,
                                         /*target=*/"tsp.keyprov");
    entries_[{up.owner, up.route_id}] = std::move(up);
    entries_[{down.owner, down.route_id}] = std::move(down);
}

const SubscriptionCatalog& SubscriptionCatalog::instance() {
    static const SubscriptionCatalog catalog;
    return catalog;
}

const CatalogEntry* SubscriptionCatalog::lookup(const std::string& owner,
                                                const std::string& route_id) const {
    auto it = entries_.find({owner, route_id});
    if (it == entries_.end()) return nullptr;
    return &it->second;
}

bool SubscriptionCatalog::isCatalogRoute(const std::string& owner,
                                         const std::string& route_id) const {
    return lookup(owner, route_id) != nullptr;
}

} // namespace mqtt
} // namespace tbox
