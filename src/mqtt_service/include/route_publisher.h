#pragma once

// RoutePublisher (CR-009 §2, §6)
//
// 按 owner/route_id 接收上行、解析当前投影、执行离线入队与 Broker PUBLISH。
//
// 处理流程 (CR-009 §6.2):
// 1. 按 RouteKey 查询当前 immutable projection
// 2. 校验方向为 UP/BIDIRECTIONAL、状态 ACTIVE、identity generation 当前有效
// 3. 以 route 定义 QoS 为默认值；调用方覆盖仅可在策略允许范围内
// 4. 构造 OutboundMessage，保存 owner/route_id/msg_id/identity generation
// 5. 在线时解析当前 expanded_topic 并 PUBLISH；离线时进入队列
// 6. 真正发送前再次按当前 generation 解析；generation 不符时进入迁移策略
//
// accepted/PUBACK、msg_id 幂等、unknown outcome 和 QoS 重发语义保持不变 (CR-009 §6.2)。

#include "mqtt_types.h"
#include "tbox/mqtt/types.h"
#include "effective_route_table.h"
#include "topic_template_resolver.h"
#include <functional>
#include <memory>

namespace tbox {
namespace mqtt {

class RoutePublisher {
public:
    /// Broker PUBLISH 回调（在线时调用）
    /// @return true 表示已提交协议层（不代表 PUBACK）
    using BrokerPublishFn = std::function<bool(
        const std::string& topic,
        const std::vector<uint8_t>& payload,
        QoS qos)>;

    /// 离线队列入队回调
    using QueueEnqueueFn = std::function<bool(const OutboundMessage& msg)>;

    /// 连接状态查询回调
    using IsConnectedFn = std::function<bool()>;

    RoutePublisher();
    ~RoutePublisher();

    void setBrokerPublishFn(BrokerPublishFn fn);
    void setQueueEnqueueFn(QueueEnqueueFn fn);
    void setIsConnectedFn(IsConnectedFn fn);

    /// Route-based 发布 (CR-009 §6)
    /// @param req 发布请求（owner/route_id/msg_id/payload/qos/priority）
    /// @param route_table 当前不可变路由投影
    /// @param resolver 当前模板解析器（含当前身份 generation）
    /// @return PublishResult；accepted 仅表示 daemon 已接管
    PublishResult publishRoute(const RoutePublishRequest& req,
                               const std::shared_ptr<const EffectiveRouteTable>& route_table,
                               const TopicTemplateResolver& resolver);

private:
    BrokerPublishFn broker_publish_fn_;
    QueueEnqueueFn queue_enqueue_fn_;
    IsConnectedFn is_connected_fn_;
};

} // namespace mqtt
} // namespace tbox
