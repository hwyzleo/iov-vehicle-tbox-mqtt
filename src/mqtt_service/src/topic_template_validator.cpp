// TopicTemplateValidator (CR-009 §4)
#include "topic_template_validator.h"
#include <algorithm>
#include <cctype>

namespace tbox {
namespace mqtt {

// v1 允许的变量白名单
namespace {
const std::vector<std::string> kAllowedVariables = {"ecu_uid"};
}

TopicTemplateValidator::TopicTemplateValidator() = default;
TopicTemplateValidator::~TopicTemplateValidator() = default;

const std::vector<std::string>& TopicTemplateValidator::allowedVariables() {
    return kAllowedVariables;
}

bool TopicTemplateValidator::isVariableAllowed(const std::string& name) const {
    const auto& allowed = allowedVariables();
    return std::find(allowed.begin(), allowed.end(), name) != allowed.end();
}

bool TopicTemplateValidator::isValidVariableName(const std::string& name) {
    if (name.empty()) return false;
    for (char c : name) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
            return false;
        }
    }
    return true;
}

bool TopicTemplateValidator::checkWildcardConstraints(const std::string& topic_template,
                                                      std::string& reason) {
    // 按 '/' 分层检查 (CR-009 §4.1)
    // 设备键层（含 {ecu_uid}）不得使用通配符；# 必须为最后一层
    size_t start = 0;
    size_t level_idx = 0;
    bool last_level = false;
    while (start <= topic_template.size()) {
        size_t end = topic_template.find('/', start);
        std::string level;
        if (end == std::string::npos) {
            level = topic_template.substr(start);
            last_level = true;
        } else {
            level = topic_template.substr(start, end - start);
        }

        bool has_ecu_uid = level.find("{ecu_uid}") != std::string::npos;
        bool has_plus = level.find('+') != std::string::npos;
        bool has_hash = level.find('#') != std::string::npos;

        // 设备键层不得使用通配符 (CR-009 §4.1)
        if (has_ecu_uid && (has_plus || has_hash)) {
            reason = "wildcard_in_device_key_layer";
            return false;
        }

        // # 必须为最后一层且独占该层 (MQTT spec)
        if (has_hash && (!last_level || level != "#")) {
            reason = "invalid_multi_level_wildcard";
            return false;
        }

        // + 不得与 # 同层
        if (has_plus && has_hash) {
            reason = "mixed_wildcards";
            return false;
        }

        if (last_level) break;
        start = end + 1;
        ++level_idx;
    }
    return true;
}

TopicTemplateValidationResult TopicTemplateValidator::validate(
        const std::string& topic_template) const {
    TopicTemplateValidationResult r;

    // 非空检查
    if (topic_template.empty()) {
        r.reason_code = "empty_template";
        r.reason_detail = "topic template is empty";
        return r;
    }

    // 长度检查
    if (topic_template.size() > kMaxTopicLength) {
        r.reason_code = "template_too_long";
        r.reason_detail = "topic template exceeds max length";
        return r;
    }

    // NUL 字节检查 (CR-009 §4.1)
    if (topic_template.find('\0') != std::string::npos) {
        r.reason_code = "nul_byte";
        r.reason_detail = "topic template contains NUL byte";
        return r;
    }

    // 基本 UTF-8 可表示性检查（拒绝非 UTF-8 的裸字节序列）
    // 简化检查：控制字符（除 '/' '+' '#' '{' '}' 外）拒绝
    for (unsigned char c : topic_template) {
        if (c < 0x20 && c != '\0') {
            r.reason_code = "invalid_control_char";
            r.reason_detail = "topic template contains control character";
            return r;
        }
    }

    // 变量解析与校验
    int var_count = 0;
    size_t i = 0;
    while (i < topic_template.size()) {
        char c = topic_template[i];

        if (c == '{') {
            // 查找匹配的 '}'
            size_t close = topic_template.find('}', i + 1);
            if (close == std::string::npos) {
                r.reason_code = "unclosed_bracket";
                r.reason_detail = "topic template has unclosed '{'";
                return r;
            }

            // 检查括号内是否有嵌套 '{' (CR-009 §4.1: 禁止嵌套变量)
            std::string inner = topic_template.substr(i + 1, close - i - 1);
            if (inner.find('{') != std::string::npos) {
                r.reason_code = "nested_variable";
                r.reason_detail = "topic template has nested variable";
                return r;
            }

            // 空变量名检查
            if (inner.empty()) {
                r.reason_code = "empty_variable_name";
                r.reason_detail = "topic template has empty variable name '{}'";
                return r;
            }

            // 默认值/函数/条件检查 (CR-009 §4.1)
            // v1 禁止 {ecu_uid:default}, {ecu_uid|func}, {ecu_uid?cond} 等
            for (char ch : inner) {
                if (ch == ':' || ch == '|' || ch == '?' || ch == '=') {
                    r.reason_code = "unsupported_expression";
                    r.reason_detail = "topic template uses unsupported expression syntax";
                    return r;
                }
            }

            // 变量名格式检查
            if (!isValidVariableName(inner)) {
                r.reason_code = "invalid_variable_name";
                r.reason_detail = "topic template has invalid variable name";
                return r;
            }

            // 变量白名单检查 (CR-009 §4.1: v1 仅 {ecu_uid})
            if (!isVariableAllowed(inner)) {
                r.reason_code = "unknown_variable";
                r.reason_detail = "topic template uses unknown variable: " + inner;
                return r;
            }

            ++var_count;
            if (var_count > static_cast<int>(kMaxVariables)) {
                r.reason_code = "too_many_variables";
                r.reason_detail = "topic template exceeds max variable count";
                return r;
            }

            i = close + 1;
        } else if (c == '}') {
            // 孤立的 '}'，无匹配的 '{'
            r.reason_code = "unmatched_bracket";
            r.reason_detail = "topic template has unmatched '}'";
            return r;
        } else {
            ++i;
        }
    }

    // 通配符位置约束 (CR-009 §4.1)
    std::string wildcard_reason;
    if (!checkWildcardConstraints(topic_template, wildcard_reason)) {
        r.reason_code = wildcard_reason;
        r.reason_detail = "topic template violates wildcard constraints";
        return r;
    }

    // 层级数检查
    size_t level_count = 1;
    for (char ch : topic_template) {
        if (ch == '/') ++level_count;
    }
    if (level_count > kMaxTopicLevels) {
        r.reason_code = "too_many_levels";
        r.reason_detail = "topic template exceeds max level count";
        return r;
    }

    // 空层级检查（连续 '/' 或首尾 '/'）
    // MQTT 允许空层级，但 CR-009 §4.1 要求检查；标记为无效
    if (topic_template.front() == '/' || topic_template.back() == '/' ||
        topic_template.find("//") != std::string::npos) {
        r.reason_code = "empty_level";
        r.reason_detail = "topic template has empty level";
        return r;
    }

    r.ok = true;
    return r;
}

TopicTemplateValidationResult TopicTemplateValidator::validateExpanded(
        const std::string& expanded_topic) const {
    TopicTemplateValidationResult r;

    if (expanded_topic.empty()) {
        r.reason_code = "empty_expanded_topic";
        r.reason_detail = "expanded topic is empty";
        return r;
    }

    // 展开后长度检查 (CR-009 §4.1)
    if (expanded_topic.size() > kMaxTopicLength) {
        r.reason_code = "expanded_too_long";
        r.reason_detail = "expanded topic exceeds max length";
        return r;
    }

    // NUL 检查
    if (expanded_topic.find('\0') != std::string::npos) {
        r.reason_code = "nul_byte";
        r.reason_detail = "expanded topic contains NUL byte";
        return r;
    }

    // 空层级检查
    if (expanded_topic.front() == '/' || expanded_topic.back() == '/' ||
        expanded_topic.find("//") != std::string::npos) {
        r.reason_code = "empty_level";
        r.reason_detail = "expanded topic has empty level";
        return r;
    }

    // 未展开变量残留检查（展开后不应有 {ecu_uid}）
    if (expanded_topic.find("{ecu_uid}") != std::string::npos) {
        r.reason_code = "unresolved_variable";
        r.reason_detail = "expanded topic still contains unresolved variable";
        return r;
    }

    // 层级数检查
    size_t level_count = 1;
    for (char ch : expanded_topic) {
        if (ch == '/') ++level_count;
    }
    if (level_count > kMaxTopicLevels) {
        r.reason_code = "too_many_levels";
        r.reason_detail = "expanded topic exceeds max level count";
        return r;
    }

    r.ok = true;
    return r;
}

std::vector<TopicTemplateVariable> TopicTemplateValidator::parseVariables(
        const std::string& topic_template) const {
    std::vector<TopicTemplateVariable> vars;
    size_t i = 0;
    while (i < topic_template.size()) {
        if (topic_template[i] == '{') {
            size_t close = topic_template.find('}', i + 1);
            if (close != std::string::npos) {
                std::string name = topic_template.substr(i + 1, close - i - 1);
                if (!name.empty() && isValidVariableName(name)) {
                    TopicTemplateVariable v;
                    v.name = name;
                    // 去重
                    bool found = false;
                    for (const auto& existing : vars) {
                        if (existing.name == name) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        vars.push_back(std::move(v));
                    }
                }
                i = close + 1;
            } else {
                break;
            }
        } else {
            ++i;
        }
    }
    return vars;
}

} // namespace mqtt
} // namespace tbox
