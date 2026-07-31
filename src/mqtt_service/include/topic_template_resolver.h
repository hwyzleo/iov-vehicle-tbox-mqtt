#pragma once

// TopicTemplateResolver (CR-009 §2, §4.2)
//
// 使用指定 identity generation 的变量上下文展开模板。
// 展开前校验模板语法；展开后校验 Topic 约束。
// 绑定 identity generation，发送/订阅前可再次核对 generation 是否当前有效。
//
// 线程模型：ecu_uid_ 和 identity_generation_ 的设置在单写线程（投影构建线程）完成，
// resolve() 为只读操作。多线程读取时调用方需确保 setIdentity 与 resolve 不并发，
// 或由 TopicProjectionBuilder 在构建完成后以不可变快照方式传递。

#include "mqtt_types.h"
#include "topic_template_validator.h"
#include <cstdint>
#include <string>

namespace tbox {
namespace mqtt {

/// 模板展开结果 (CR-009 §4.2)
struct TopicResolveResult {
    bool ok = false;
    std::string expanded_topic;      // 成功时为展开后的完整 Topic（内存，不持久化）
    uint64_t identity_generation = 0;  // 使用的身份 generation
    std::string reason_code;
};

class TopicTemplateResolver {
public:
    TopicTemplateResolver();
    ~TopicTemplateResolver();

    /// 设置当前身份变量上下文 (CR-009 §4.2)
    /// @param ecu_uid 当前有效的 ecu_uid（空表示身份不可用）
    /// @param identity_generation 当前身份 generation
    void setIdentity(const std::string& ecu_uid, uint64_t identity_generation);

    /// 获取当前 identity generation
    uint64_t identityGeneration() const;

    /// 获取当前 ecu_uid（便捷方法）
    std::string ecuUid() const;

    /// 展开模板 (CR-009 §4.2 stage 2)
    /// 假定模板已通过 validate()；直接执行 {ecu_uid} 替换。
    /// @param topic_template 已校验的模板
    /// @return 展开结果；身份无效时 ok=false, reason_code="identity_unavailable"
    TopicResolveResult resolve(const std::string& topic_template) const;

    /// 展开模板并完整校验 (CR-009 §4.2 stage 1+2)
    /// 先用 validator 校验模板语法，再展开，再校验展开结果。
    TopicResolveResult resolveAndValidate(const std::string& topic_template) const;

    /// 检查给定 generation 是否与当前一致 (CR-009 §4.2 stage 3)
    bool isGenerationCurrent(uint64_t generation) const;

    /// 身份是否有效（ecu_uid 非空且 generation > 0）
    bool isIdentityValid() const;

    /// 获取内部 validator（供外部复用校验逻辑）
    const TopicTemplateValidator& validator() const;

private:
    TopicTemplateValidator validator_;
    std::string ecu_uid_;
    uint64_t identity_generation_ = 0;
};

} // namespace mqtt
} // namespace tbox
