// OwnerSnapshotRegistry (CR-007 §2, §4)
#include "owner_snapshot_registry.h"
#include <algorithm>

namespace tbox {
namespace mqtt {

OwnerSnapshotRegistry::OwnerSnapshotRegistry()
    : state_(std::make_shared<const State>()) {
}

OwnerSnapshotRegistry::~OwnerSnapshotRegistry() = default;

namespace {

bool validQos(uint8_t qos) { return qos <= 2; }

} // anonymous namespace

OwnerSnapshotRegistry::ValidationResult OwnerSnapshotRegistry::validate(
        const OwnerSubscriptionSnapshot& snapshot) const {
    ValidationResult r;
    if (snapshot.owner.empty()) {
        r.reason_code = "invalid_owner";
        return r;
    }
    if (snapshot.generation == 0) {
        r.reason_code = "invalid_generation";
        return r;
    }
    if (snapshot.content_digest.empty()) {
        r.reason_code = "invalid_digest";
        return r;
    }

    const auto& catalog = SubscriptionCatalog::instance();
    r.normalized = snapshot;  // 保留 owner/generation/digest/registration_complete/contract_version
    r.normalized.items.clear();
    r.normalized.items.reserve(snapshot.items.size());

    std::vector<std::string> seen;
    for (const auto& raw : snapshot.items) {
        SubscriptionItem item = raw;

        const CatalogEntry* entry = catalog.lookup(snapshot.owner, raw.route_id);
        const bool catalog_route = (entry != nullptr);
        if (entry != nullptr) {
            // CR-015: catalog-governed route —— 模板/方向/QoS/target/scope 由 catalog 冻结，
            // 调用方只引用 route_id；显式传入冲突模板/scope 时整体拒绝，禁止任意 Topic 注入。
            if (!raw.readiness_scope.empty() &&
                raw.readiness_scope != entry->readiness_scope) {
                r.rejected_route_ids.push_back(raw.route_id);
                r.reason_code = "catalog_scope_mismatch";
                return r;
            }
            if (!raw.topic_template.empty() &&
                raw.topic_template != entry->topic_template) {
                r.rejected_route_ids.push_back(raw.route_id);
                r.reason_code = "catalog_template_mismatch";
                return r;
            }
            item.topic_template = entry->topic_template;
            item.direction = entry->direction;
            item.qos = entry->qos;
            item.target = entry->target;
            item.readiness_scope = entry->readiness_scope;
            item.criticality = entry->criticality;
            item.mandatory = false;  // capability-gated，不进入全局 mandatory 集合
            // capability-gated route：字段缺失必须解释为 enabled=false
            item.enabled = raw.enabled.value_or(false);
        } else {
            // CR-015: 非 catalog route 不得携带 readiness_scope（未受控能力路由）
            if (!raw.readiness_scope.empty()) {
                r.rejected_route_ids.push_back(raw.route_id);
                r.reason_code = "unknown_capability_route";
                return r;
            }
            // legacy route：字段缺失解释为 enabled=true（保持兼容）
            item.enabled = raw.enabled.value_or(true);
            item.readiness_scope.clear();
            item.criticality = raw.mandatory
                ? SubscriptionCriticality::GLOBAL_MANDATORY
                : SubscriptionCriticality::OPTIONAL;
        }

        if (item.route_id.empty()) {
            r.rejected_route_ids.push_back("<empty>");
            r.reason_code = "invalid_route_id";
            return r;
        }
        if (std::find(seen.begin(), seen.end(), item.route_id) != seen.end()) {
            r.rejected_route_ids.push_back(item.route_id);
            r.reason_code = "duplicate_route_id";
            return r;
        }
        seen.push_back(item.route_id);
        if (item.topic_template.empty()) {
            r.rejected_route_ids.push_back(item.route_id);
            r.reason_code = "invalid_topic_template";
            return r;
        }
        // CR-009 §4.2: 模板语法/变量白名单/静态结构校验
        auto tvr = template_validator_.validate(item.topic_template);
        if (!tvr.ok) {
            r.rejected_route_ids.push_back(item.route_id);
            r.reason_code = tvr.reason_code;
            return r;
        }
        if (!validQos(item.qos)) {
            r.rejected_route_ids.push_back(item.route_id);
            r.reason_code = "invalid_qos";
            return r;
        }
        if (item.target.empty() && !catalog_route) {
            // catalog-governed route 的 target 可为空（由 catalog 冻结，如 keyprov.uplink）；
            // legacy route 仍要求 target 非空（保持 CR-007 校验）。
            r.rejected_route_ids.push_back(item.route_id);
            r.reason_code = "invalid_target";
            return r;
        }
        r.normalized.items.push_back(std::move(item));
    }
    r.ok = true;
    return r;
}

ReplaceSnapshotResult OwnerSnapshotRegistry::replaceSnapshot(
        const OwnerSubscriptionSnapshot& snapshot) {
    ReplaceSnapshotResult result;
    result.status = SnapshotAcceptStatus::UNKNOWN;

    // 影子校验（不触碰当前 state）
    auto vr = validate(snapshot);
    if (!vr.ok) {
        result.status = SnapshotAcceptStatus::REJECTED;
        result.rejected_route_ids = std::move(vr.rejected_route_ids);
        result.reason_code = vr.reason_code;
        return result;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = state_->owners.find(snapshot.owner);
    if (it != state_->owners.end()) {
        const auto& existing = it->second.snapshot;
        if (existing.generation == snapshot.generation) {
            if (existing.content_digest == snapshot.content_digest) {
                // 幂等：相同 owner+generation+digest 返回已有结果
                result.status = SnapshotAcceptStatus::ACCEPTED;
                result.accepted_generation = existing.generation;
                result.reason_code = "idempotent";
                return result;
            }
            // 相同 generation 但 digest 不同 -> MQTT-1010
            result.status = SnapshotAcceptStatus::REJECTED;
            result.reason_code = "digest_conflict";
            return result;
        }
        if (existing.generation > snapshot.generation) {
            // 低于当前 generation -> stale，不覆盖
            result.status = SnapshotAcceptStatus::ACCEPTED;
            result.accepted_generation = existing.generation;
            result.reason_code = "stale_generation";
            return result;
        }
    }

    // 接受：影子副本完成全量校验后单指针切换 (CR-007 §4)
    auto next = std::make_shared<State>(*state_);
    OwnerEntry entry;
    entry.snapshot = vr.normalized;  // CR-015: 存储归一化后的快照
    entry.reason_code = "accepted";
    next->owners[snapshot.owner] = std::move(entry);
    state_ = next;  // 原子替换（mutex 串行）

    result.status = SnapshotAcceptStatus::ACCEPTED;
    result.accepted_generation = snapshot.generation;
    result.reason_code = "accepted";
    return result;
}

SnapshotStatusResult OwnerSnapshotRegistry::getSnapshotStatus(
        const std::string& owner, uint64_t generation) const {
    std::lock_guard<std::mutex> lock(mutex_);
    SnapshotStatusResult r;
    auto it = state_->owners.find(owner);
    if (it == state_->owners.end()) {
        r.status = SnapshotAcceptStatus::UNKNOWN;
        r.generation = generation;
        return r;
    }
    const auto& snap = it->second.snapshot;
    r.generation = snap.generation;
    r.content_digest = snap.content_digest;
    r.registration_complete = snap.registration_complete;
    if (snap.generation == generation) {
        r.status = SnapshotAcceptStatus::ACCEPTED;
    } else {
        r.status = (snap.generation > generation)
            ? SnapshotAcceptStatus::ACCEPTED  // 已有更高 generation
            : SnapshotAcceptStatus::UNKNOWN; // 查询的更高，尚未接受
    }
    return r;
}

std::shared_ptr<const OwnerSubscriptionSnapshot> OwnerSnapshotRegistry::getLatestSnapshot(
        const std::string& owner) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = state_->owners.find(owner);
    if (it == state_->owners.end()) {
        return nullptr;
    }
    return std::make_shared<OwnerSubscriptionSnapshot>(it->second.snapshot);
}

std::map<std::string, OwnerSubscriptionSnapshot> OwnerSnapshotRegistry::getAllLatestSnapshots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::map<std::string, OwnerSubscriptionSnapshot> out;
    for (const auto& [owner, entry] : state_->owners) {
        out[owner] = entry.snapshot;
    }
    return out;
}

bool OwnerSnapshotRegistry::hasRegistrationComplete(const std::string& owner) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = state_->owners.find(owner);
    return it != state_->owners.end() && it->second.snapshot.registration_complete;
}

void OwnerSnapshotRegistry::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = std::make_shared<const State>();
}

size_t OwnerSnapshotRegistry::ownerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_->owners.size();
}

} // namespace mqtt
} // namespace tbox
