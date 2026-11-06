#pragma once

// TBOX-MQTT-DSN-CR-018 / TBOX-SEC-DSN-CR-024：SEC 配对可信时间提供方。
//
// 生产唯一 ITrustedTimeProvider 实现：经 tbox::sec::SecClient 只读查询
// （GET_TRUSTED_TIME，MethodId=24），映射 SEC TrustedTimeEnvelope → MQTT
// TrustedTimeSample，并承担消费方一跳扣减（SDK/本进程 RTT 与本地流逝只扣一次，
// 由 TrustedTimeGate 缓存/租约逻辑继续扣减）。
//
// 边界（§3.1 路线 A）：MQTT 不复制 SEC 信任算法；Unknown/Untrusted 是可查询
// 业务状态（envelope.trust_state/reason），与传输错误（error_class）分离，
// 传输/契约层错误映射为 ProviderUnavailable/QueryTimeout/ContractUnsupported/
// SampleMalformed（fail-closed）。
//

#include "trusted_time_gate.h"  // ITrustedTimeProvider
#include "trusted_time_types.h"
#include <memory>
#include <mutex>
#include <string>

namespace tbox {
namespace sec {
class SecClient;
struct TrustedTimeEnvelope;
}  // namespace sec
namespace mqtt {

/// SEC 配对可信时间提供方（生产）。
class SecTimeProvider final : public ITrustedTimeProvider {
public:
    /// 构造独立 SecClient（与凭据管理共用 socket 路径，独立连接）。
    explicit SecTimeProvider(std::string sec_socket_path);
    ~SecTimeProvider() override;

    SecTimeProvider(const SecTimeProvider&) = delete;
    SecTimeProvider& operator=(const SecTimeProvider&) = delete;

    TimeQueryResult query() override;
    TimeProviderInfo info() const override;

    /// 将 SEC envelope 映射为 MQTT 样本（暴露给单测；纯函数语义）。
    static TrustedTimeSample mapEnvelope(
        const tbox::sec::TrustedTimeEnvelope& e);

private:
    std::string sec_socket_path_;
    std::shared_ptr<tbox::sec::SecClient> client_;
    mutable std::mutex mu_;
    uint64_t last_token_ = 0;   ///< 上次成功响应的 sec_daemon_epoch
    bool have_token_ = false;
};

/// 生产工厂：以 SEC socket 路径构造真实 provider。
std::shared_ptr<ITrustedTimeProvider> createSecTimeProvider(
    const std::string& sec_socket_path);

}  // namespace mqtt
}  // namespace tbox
