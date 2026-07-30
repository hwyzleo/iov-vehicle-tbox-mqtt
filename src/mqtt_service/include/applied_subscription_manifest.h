#pragma once

// AppliedSubscriptionManifest (CR-007 §2, §3.1)
//
// 记录最后已确认的 Broker 订阅清单和 client/session identity，仅用于进程重启后的
// 差异计算。清单是 MQTT 传输状态，不是业务事实来源；没有 owner 当前快照时，不得仅凭
// 清单进入 Cloud Ready (CR-007 §3.1)。
//
// client_id 或 identity generation 变化时清单失效，按新会话重新订阅 (CR-007 §3.1)。

#include "mqtt_types.h"
#include "store.h"
#include <cstdint>
#include <map>
#include <memory>
#include <string>

namespace tbox {
namespace mqtt {

/// 已应用 Broker 订阅清单 (CR-007 §3.1)
struct AppliedSubscriptionManifest {
    std::string client_id_hash;
    uint64_t identity_generation = 0;
    std::map<std::string, uint8_t> active_topics;  // topic(模板键或受控展开值) -> qos
    uint64_t broker_manifest_version = 0;
    std::string updated_at;
};

class AppliedSubscriptionManifestStore {
public:
    void setStore(std::shared_ptr<hwyz::store::Store> store);

    /// 持久化清单（JSON blob 到 framework-store）
    bool save(const AppliedSubscriptionManifest& manifest);

    /// 加载清单；缺失或损坏返回空清单（保守策略：reconciler 全量重订阅）
    AppliedSubscriptionManifest load() const;

    /// 清除清单
    void clear();

    /// 当前清单 identity 是否匹配（client_id_hash + identity_generation）
    bool isIdentityMatch(const std::string& client_id_hash,
                         uint64_t identity_generation) const;

private:
    std::shared_ptr<hwyz::store::Store> store_;
};

} // namespace mqtt
} // namespace tbox
