#pragma once

// TBOX-MQTT-DSN-CR-018：可信时间门禁（TrustedTimeGate）
//
// 职责（§3/§4/§6/§7）：
//   - 消费 ITrustedTimeProvider 的判定（生产唯一来源为 SEC 配对契约），
//     管理短租约缓存、独立退避与 time_gate_generation；
//   - 强制模式（require=true）：trust_state=Trusted 且样本仍有效且系统时钟对齐
//     才 time_gate_ready=true；否则不放行连接/受理/发送；
//   - 开发旁路（require=false）：不修改门禁结果（时间仍不可信即不可信），
//     只标记 bypass 并提供 shadow 观测，不伪造 time_gate_ready=true；
//   - 租约耗尽/失信事件立即撤销并递增 generation，供 MqttService 有序断链。
//
// 调度（§4.2）：复用既有配置/调度体例（同 ConnectionStateMachine 的
// condition_variable timer 模式），不另建调度框架；同一门禁最多一个 armed
// retry timer、一个查询在途；租约过期 timer 与失败重试职责区分，租约撤销优先且幂等。

#include "trusted_time_types.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

namespace tbox {
namespace mqtt {

/// 时间提供方抽象。生产唯一实现将由 SEC 配对 CR（DEP-TIME-01）的
/// tbox::sec_client facade 提供；当前工厂返回 ContractUnsupportedProvider。
/// MQTT 不 include SEC 私有头、不伪造尚不存在的 SEC SDK 方法。
class ITrustedTimeProvider {
public:
    virtual ~ITrustedTimeProvider() = default;

    /// 同步查询；不得抛出；实现必须把所有失败映射为不可用结果（fail-closed）。
    virtual TimeQueryResult query() = 0;

    /// 提供方标识（SEC daemon epoch 或 fake 标识），用于缓存失效。
    virtual TimeProviderInfo info() const = 0;
};

/// 生产工厂：SEC 配对契约落地前始终返回 ContractUnsupportedProvider
/// （强制模式 fail-closed → TIME_WAIT，不建立云连接）。
/// 测试不得经此工厂注入 Fake；Fake 由测试显式构造并 setProvider。
std::shared_ptr<ITrustedTimeProvider> createDefaultTimeProvider();

/// dev/test 专用 fake（仅测试 fixture；production 构造/schema 拒绝）
std::shared_ptr<ITrustedTimeProvider> createFakeTimeProvider(
    const TrustedTimeSample& sample);

class TrustedTimeGate {
public:
    /// 状态变化回调：ready 翻转、generation 递增、retry 调度、bypass 变化时调用。
    /// 回调在锁外执行，不得阻塞查询线程。
    using ChangeCallback =
        std::function<void(const TimeGateStatus& status, bool ready_changed)>;

    /// 可观测事件（§11：日志事件映射，与 ChangeCallback 分离消费）
    enum class EventKind {
        kReady,            ///< 门禁恢复（mqtt.time_gate.ready）
        kTrustLost,        ///< 在线失信（mqtt.time_gate.trust_lost）
        kRetryScheduled,   ///< 失败退避调度（mqtt.time_gate.retry_scheduled）
        kQueryFailed,      ///< 查询失败（mqtt.time_gate.query_failed，上层限频）
        kBypassChanged,    ///< 旁路状态变化（mqtt.time_gate.bypassed）
        kWaiting           ///< 进入 TIME_WAIT 等待（mqtt.time_gate.waiting）
    };
    using EventCallback =
        std::function<void(EventKind kind, const TimeGateStatus& status)>;

    TrustedTimeGate();
    ~TrustedTimeGate();

    /// 配置（纯校验，非法配置拒绝并保持 fail-closed）。
    bool configure(const TrustedTimeConfig& cfg, std::string* err = nullptr);

    /// 注入提供方（生产工厂之外；测试注入 Fake）。
    void setProvider(std::shared_ptr<ITrustedTimeProvider> provider);

    void setChangeCallback(ChangeCallback cb);
    /// 可观测事件回调（§11 日志事件；锁外调用）
    void setEventCallback(EventCallback cb);

    /// 启动定时器线程（首次查询立即执行，不计 retry）。
    void start();
    /// 取消全部 timer、禁止新查询（幂等）。
    void stop();

    // ---- 状态查询（§8 readiness / §11 日志）----
    TimeGateStatus getStatus() const;
    bool isReady() const;        ///< 门禁放行（强制模式 Trusted+租约+对齐；旁路恒 true）
    bool isBypassed() const;     ///< require=false（开发旁路）
    bool isEnforced() const;     ///< require=true
    uint64_t getGeneration() const;
    TrustedTimeConfig getConfig() const;

    // ---- 受理/发送门禁（§7.1）----
    /// 缓存有效且租约未耗尽 → allowed；缓存无效/租约耗尽 → 触发异步刷新并拒绝。
    /// 不在调用线程执行查询（Broker/IPC 回调线程不得阻塞）。
    TimeAdmission checkAdmission();

    // ---- 运行期失信/撤销（§6）----
    /// 外部失信事件/租约耗尽：立即撤销并递增 generation（幂等）。
    void revoke(const TimeGateReason& reason);

    // ---- dev/test 注入（仅测试）----
    void simulateSample(const TrustedTimeSample& sample);
    void simulateUnavailable(TimeGateReason reason);

private:
    void timerLoop();
    void doQuery();                       // 执行一次查询（timer 线程）
    void scheduleRefreshLocked();         // 成功态定期刷新（≤ refresh_interval_ms）
    void scheduleRetryLocked();           // 失败退避（jitter 后硬裁剪）
    std::chrono::milliseconds calculateBackoffLocked() const;
    void resetRetryLocked();              // 成功离开等待态后清连续计数
    bool cacheUsableLocked() const;       // 缓存样本+租约仍有效
    /// 校验并缓存样本；返回 ready 是否发生翻转（供事件回调）。
    bool commitSampleLocked(const TrustedTimeSample& s,
                            std::chrono::milliseconds rtt);
    TimeGateStatus buildStatusForCallbackLocked() const;

    mutable std::mutex mutex_;
    ChangeCallback callback_;
    EventCallback event_callback_;
    TrustedTimeConfig config_;
    std::shared_ptr<ITrustedTimeProvider> provider_;
    mutable std::mt19937 rng_{std::random_device{}()};

    // 缓存与租约（§3.2/§3.3）
    TrustedTimeSample sample_;
    bool has_sample_ = false;
    std::chrono::steady_clock::time_point sample_received_at_;
    std::chrono::steady_clock::time_point lease_deadline_;
    bool ready_ = false;

    // 查询与退避（§4.2）
    bool query_inflight_ = false;
    std::chrono::steady_clock::time_point query_started_at_;
    uint64_t retry_count_ = 0;
    uint64_t total_attempts_ = 0;
    std::chrono::steady_clock::time_point next_query_at_;

    // 门禁代际（§6.2/§7.1）
    uint64_t gate_generation_ = 0;
    bool bypass_ = false;
    bool started_ = false;

    // 定时器线程
    std::thread timer_thread_;
    std::condition_variable timer_cv_;
    std::atomic<bool> timer_running_{false};
};

} // namespace mqtt
} // namespace tbox
