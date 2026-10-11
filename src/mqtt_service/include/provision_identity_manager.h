#pragma once

#include "mqtt_types.h"
#include "dependency_recovery.h"
#include "tbox/prov/client.h"
#include "tbox/prov/types.h"
#include "tbox/prov/errors.h"
#include <string>
#include <cstdint>
#include <memory>
#include <mutex>
#include <functional>
#include <random>
#include <string_view>

namespace tbox {
namespace mqtt {

/// ProvisionIdentityManager (CR-006 §2, §3, §5, §6)
///
/// 封装 tbox::prov_client，负责读取、校验、缓存和比较 PROV 绑定快照。
/// ClientIdBuilder 与 TopicResolver 作为公开方法分别消费不同字段，
/// 禁止共享通用 device-id 逻辑 (CR-006 §2)。
///
/// 职责：
/// - 经 tbox::prov_client::read_binding() 获取 {vin, ecu_uid, sn, state, locked}
/// - 校验 state==BOUND 且 VIN/SN/ECU UID 完整 (CR-006 §5.2)
/// - 维护 ProvisionIdentitySnapshot 和 generation（身份变化时递增）
/// - buildClientId(vin, sn) 生成 {vin}-{sn} 格式 clientId (CR-006 §6.1)
/// - resolveTopic(template) 用当前 ecu_uid 注入设备 Topic 键 (CR-006 §6.2)
/// - 不直接接触 PROV socket、JSON、存储或配置 (CR-006 §5.1)
///
/// 不作为绑定事实持久化到 framework-store (CR-006 §3)。
class ProvisionIdentityManager {
public:
    /// @param prov_socket_path PROV daemon IPC socket 路径
    explicit ProvisionIdentityManager(
        const std::string& prov_socket_path = "/tmp/tbox-prov.sock");
    ~ProvisionIdentityManager();

    ProvisionIdentityManager(const ProvisionIdentityManager&) = delete;
    ProvisionIdentityManager& operator=(const ProvisionIdentityManager&) = delete;

    // ---- 生命周期 ----

    /// 初始化 PROV client 并建立连接 (CR-006 §4)
    /// PROV 不可用时 MQTT 服务仍可启动内部 IPC 与离线队列，
    /// 但云端连接保持 PROV_WAIT (CR-006 §4)。
    bool initialize();

    // ---- CR-016 §5: PROV IPC transport 自动恢复 ----

    /// 探测并恢复 PROV IPC transport（设计 §5）。
    /// 未连接或连接类错误时销毁旧 transport、递增 client_generation 并按退避重连。
    /// @return true 表示当前 transport 健康
    bool ensureConnected();

    /// 获取当前 PROV 依赖恢复状态（线程安全）
    RetryState getRetryState() const;

    /// 依赖恢复成功（绑定 BOUND）后重置连续失败计数（设计 §6）
    void markDependencyReady();

    /// 设置 PROV_WAIT 退避配置（供 ensureConnected 记录 backoff_ms）
    void setProvisionRetryConfig(const ProvisionRetryConfig& config);

    /// 当前 PROV IPC client generation / transport 状态
    uint64_t getClientGeneration() const;
    IpcState getIpcState() const;
    bool isClientGenerationCurrent(uint64_t generation) const;

    /// 最后稳定 reason（sec/prov 未连接、连接失败、未就绪等）
    std::string getLastError() const;

    // ---- 测试钩子 (CR-016 §11) ----

    /// 在 IPC 查询返回与 generation 提交校验之间执行，用于模拟迟到回调。
    void setPreCommitHookForTest(std::function<void()> hook);
    /// 强制失效当前 transport（测试用，递增 client_generation）
    void invalidateTransportForTest();

    // ---- PROV 绑定访问 (CR-006 §5) ----

    /// 读取绑定、校验身份并更新快照 (CR-006 §5.1, §5.2)
    /// @return true 如果 state==BOUND 且 vin/ecu_uid/sn 均有效；false 时保持 PROV_WAIT
    bool readBinding();

    // ---- 快照与 generation (CR-006 §3, §7) ----

    /// 获取当前身份快照（线程安全）(CR-006 §3)
    ProvisionIdentitySnapshot getSnapshot() const;

    /// 获取当前身份 generation (CR-006 §7)
    uint64_t getGeneration() const;

    /// 检查身份是否有效（state==BOUND 且字段完整）
    bool isIdentityValid() const;

    /// 检查 PROV 是否已绑定（state==BOUND）
    bool isBound() const;

    /// 检查身份自给定 generation 以来是否已变化 (CR-006 §7)
    bool hasIdentityChanged(uint64_t since_generation) const;

    /// 获取当前 ecu_uid（便捷方法，CR-006 §6.2）
    std::string getEcuUid() const;

    // ---- ClientIdBuilder (CR-006 §6.1) ----

    /// 按 client_id_format 生成 clientId (CR-006 §6.1)
    /// 逻辑值为 VIN 与 SN 的确定性组合；禁止加入 ecu_uid 或静默截断。
    /// @return 生成的 client_id；若 vin 或 sn 为空返回空串
    std::string buildClientId(std::string_view vin,
                              std::string_view sn) const;

    // ---- TopicResolver (CR-006 §6.2) ----

    /// 用当前 ecu_uid 注入 Topic 模板中的 {ecu_uid} 占位 (CR-006 §6.2)
    /// @param topic_template 含 {ecu_uid} 占位的 Topic 模板
    /// @return 注入实例值后的 Topic；身份无效时返回空串
    std::string resolveTopic(const std::string& topic_template) const;

    // ---- 配置 ----

    /// 设置 client_id_format（只读云端接入契约，CR-006 §9）
    void setClientIdFormat(const std::string& format);

    // ---- PROV 连接状态 ----

    bool isProvConnected() const;

    // ---- 测试模拟 ----

    /// 模拟绑定有效（测试用，绕过 PROV client）
    void simulateBinding(const ProvisionIdentitySnapshot& snapshot);

    /// 模拟未绑定（测试用）
    void simulateUnbound();

    /// 检查是否处于模拟模式
    bool isSimulated() const { return simulated_; }

private:
    /// 校验绑定字段 (CR-006 §5.2)
    bool validate(const tbox::prov::VehicleBinding& binding) const;

    /// 从 VehicleBinding 更新快照，必要时递增 generation (CR-006 §3, §7)
    void updateSnapshot(const tbox::prov::VehicleBinding& binding);

    /// 记录 mqtt_dependency_* 事件 (CR-016 §8)
    void logDependencyEvent(const char* event,
                            const std::string& reason_code,
                            const std::string& error_code,
                            uint64_t elapsed_ms);

    std::string prov_socket_path_;
    // CR-016 §7: shared_ptr 使 IPC 调用可在锁外进行
    std::shared_ptr<tbox::prov::ProvClient> prov_client_;
    std::string client_id_format_ = "{vin}-{sn}";

    mutable std::mutex mutex_;
    // CR-016 §7: 串行化 transport 重建，保证同一依赖最多一个 connect attempt
    std::mutex recovery_mutex_;
    ProvisionIdentitySnapshot current_snapshot_;
    bool simulated_ = false;
    bool initialized_ = false;

    // ---- CR-016: PROV 依赖恢复状态 ----
    IpcState prov_ipc_state_ = IpcState::DISCONNECTED;
    RetryState prov_retry_;
    uint64_t prov_client_generation_ = 0;
    ProvisionRetryConfig provision_retry_config_;
    std::mt19937 prov_rng_{std::random_device{}()};
    std::function<void()> pre_commit_hook_for_test_;
    std::string last_error_;
};

} // namespace mqtt
} // namespace tbox
