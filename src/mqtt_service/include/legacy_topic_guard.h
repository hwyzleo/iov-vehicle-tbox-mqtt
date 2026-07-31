#pragma once

// LegacyTopicGuard (CR-009 §10.2)
//
// Legacy 完整 Topic 必须经过 LegacyTopicGuard 校验：
// - 设备层等于当前 ecu_uid
// - ACL 前缀合法
// - identity generation 当前有效
//
// 同一 owner 一旦启用快照/route API，不允许再通过 legacy 注册相同 route。

#include "mqtt_types.h"
#include "topic_template_resolver.h"
#include "owner_snapshot_registry.h"
#include <string>

namespace tbox {
namespace mqtt {

class LegacyTopicGuard {
public:
    /// @param ecu_uid_layer_index 设备键在 Topic 层级中的位置（0-based，默认 1）
    explicit LegacyTopicGuard(size_t ecu_uid_layer_index = 1);
    ~LegacyTopicGuard();

    /// 校验 legacy 完整 Topic (CR-009 §10.2)
    /// @param topic 完整 Topic
    /// @param resolver 当前模板解析器（含当前身份）
    /// @return true 通过校验；false 拒绝
    bool validate(const std::string& topic,
                  const TopicTemplateResolver& resolver) const;

    /// 检查 owner 是否已启用 route API (CR-009 §10.2)
    /// @return true 表示该 owner 已提交过快照，不允许 legacy 注册
    bool isRouteApiEnabled(const std::string& owner,
                           const OwnerSnapshotRegistry& registry) const;

private:
    size_t ecu_uid_layer_index_;

    /// 提取 Topic 指定层级的值
    std::string extractLayer(const std::string& topic, size_t layer_index) const;
};

} // namespace mqtt
} // namespace tbox
