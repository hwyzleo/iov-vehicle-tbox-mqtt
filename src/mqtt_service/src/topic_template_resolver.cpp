// TopicTemplateResolver (CR-009 §2, §4.2)
#include "topic_template_resolver.h"

namespace tbox {
namespace mqtt {

TopicTemplateResolver::TopicTemplateResolver() = default;
TopicTemplateResolver::~TopicTemplateResolver() = default;

void TopicTemplateResolver::setIdentity(const std::string& ecu_uid,
                                        uint64_t identity_generation) {
    ecu_uid_ = ecu_uid;
    identity_generation_ = identity_generation;
}

uint64_t TopicTemplateResolver::identityGeneration() const {
    return identity_generation_;
}

std::string TopicTemplateResolver::ecuUid() const {
    return ecu_uid_;
}

TopicResolveResult TopicTemplateResolver::resolve(const std::string& topic_template) const {
    TopicResolveResult r;

    // 身份有效性检查 (CR-009 §4.2: PROV BOUND + ecu_uid valid)
    if (!isIdentityValid()) {
        r.reason_code = "identity_unavailable";
        return r;
    }

    // 展开 {ecu_uid} 占位 (CR-009 §4.2)
    std::string expanded = topic_template;
    const std::string placeholder = "{ecu_uid}";
    size_t pos = 0;
    while ((pos = expanded.find(placeholder, pos)) != std::string::npos) {
        expanded.replace(pos, placeholder.size(), ecu_uid_);
        pos += ecu_uid_.size();
    }

    r.expanded_topic = std::move(expanded);
    r.identity_generation = identity_generation_;
    r.ok = true;
    return r;
}

TopicResolveResult TopicTemplateResolver::resolveAndValidate(
        const std::string& topic_template) const {
    TopicResolveResult r;

    // Stage 1: 模板语法校验 (CR-009 §4.2)
    auto vr = validator_.validate(topic_template);
    if (!vr.ok) {
        r.reason_code = vr.reason_code;
        return r;
    }

    // Stage 2: 展开 (CR-009 §4.2)
    auto resolve_r = resolve(topic_template);
    if (!resolve_r.ok) {
        r.reason_code = resolve_r.reason_code;
        return r;
    }

    // Stage 2: 展开后校验 (CR-009 §4.2)
    auto evr = validator_.validateExpanded(resolve_r.expanded_topic);
    if (!evr.ok) {
        r.reason_code = evr.reason_code;
        return r;
    }

    r = std::move(resolve_r);
    return r;
}

bool TopicTemplateResolver::isGenerationCurrent(uint64_t generation) const {
    return identity_generation_ != 0 && identity_generation_ == generation;
}

bool TopicTemplateResolver::isIdentityValid() const {
    return !ecu_uid_.empty() && identity_generation_ != 0;
}

const TopicTemplateValidator& TopicTemplateResolver::validator() const {
    return validator_;
}

} // namespace mqtt
} // namespace tbox
