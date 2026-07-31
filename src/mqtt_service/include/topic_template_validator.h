#pragma once

// TopicTemplateValidator (CR-009 §4)
//
// 校验 Topic 模板语法、变量白名单、MQTT Topic 静态结构约束。
// v1 仅允许 {ecu_uid} 变量；不支持默认值、函数、条件、嵌套变量、环境变量或任意表达式。
//
// 校验阶段 (CR-009 §4.2):
// - 快照接受阶段: validate() 校验模板语法、变量白名单、静态 Topic 结构
// - 投影构建阶段: validateExpanded() 校验展开后 Topic 长度/层级/空层级/ACL 前缀
//
// 模板语法错误整体拒绝对应 owner 快照 (CR-009 §4.2)。

#include "mqtt_types.h"
#include <string>
#include <vector>

namespace tbox {
namespace mqtt {

/// 模板校验结果 (CR-009 §4)
struct TopicTemplateValidationResult {
    bool ok = false;
    std::string reason_code;    // e.g. "unknown_variable", "unclosed_bracket"
    std::string reason_detail;  // 人类可读详情（不含 UID 实例值）
};

class TopicTemplateValidator {
public:
    TopicTemplateValidator();
    ~TopicTemplateValidator();

    /// 校验模板语法与静态结构 (CR-009 §4.1, §4.2 stage 1)
    /// 检查: UTF-8、无 NUL、变量格式 {name}、变量白名单 ({ecu_uid} only)、
    /// 无空变量名、无未闭合括号、无嵌套变量、无默认值/函数/条件/环境变量、
    /// 通配符位置约束、Topic 长度/层级/空层级约束。
    TopicTemplateValidationResult validate(const std::string& topic_template) const;

    /// 校验展开后的完整 Topic (CR-009 §4.1, §4.2 stage 2)
    /// 检查: 总长度、层级数、空层级、ACL 前缀。
    TopicTemplateValidationResult validateExpanded(const std::string& expanded_topic) const;

    /// 解析模板中的变量列表 (CR-009 §3)
    /// 返回模板中出现的变量名列表（如 ["ecu_uid"]）。
    /// 不校验合法性；仅提取 {name} 模式。解析失败时返回空列表。
    std::vector<TopicTemplateVariable> parseVariables(const std::string& topic_template) const;

private:
    // MQTT Topic 约束 (MQTT 5.0 spec)
    static constexpr size_t kMaxTopicLength = 65535;   // MQTT spec max
    static constexpr size_t kMaxTopicLevels = 128;      // 实用上限
    static constexpr size_t kMaxVariables = 16;         // 单模板变量数上限

    // v1 允许的变量白名单
    static const std::vector<std::string>& allowedVariables();

    /// 检查变量名是否在白名单中
    bool isVariableAllowed(const std::string& name) const;

    /// 检查变量名格式（字母/数字/下划线，非空）
    static bool isValidVariableName(const std::string& name);

    /// 检查通配符位置约束 (CR-009 §4.1)
    /// 设备键层（含 {ecu_uid} 的层级）不得使用通配符；# 必须为最后一层。
    static bool checkWildcardConstraints(const std::string& topic_template,
                                         std::string& reason);
};

} // namespace mqtt
} // namespace tbox
