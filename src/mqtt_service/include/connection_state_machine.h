#pragma once

#include "mqtt_types.h"
#include "store.h"
#include <functional>
#include <chrono>
#include <mutex>
#include <random>
#include <memory>
#include <thread>
#include <condition_variable>
#include <atomic>

namespace tbox {
namespace mqtt {

class ConnectionStateMachine {
public:
    using StateChangeCallback = std::function<void(ConnectionState)>;

    ConnectionStateMachine();
    ~ConnectionStateMachine();

    void setStateChangeCallback(StateChangeCallback callback);
    void start();
    void stop();
    ConnectionState getCurrentState() const;
    ConnectionStateInfo getStateInfo() const;
    void setMaxRetries(uint32_t max_retries);

    // Set framework-store for state persistence (Design §5)
    void setStore(std::shared_ptr<hwyz::store::Store> store);

    // ---- CR-005 §6: 凭据轮询退避配置 ----
    void setCredentialPollConfig(const CredentialPollConfig& config);

    // ---- CR-006 §4: PROV 绑定前置 ----
    void setProvisionRetryConfig(const ProvisionRetryConfig& config);

    // ---- 原有事件 ----
    void onCertificateReady();
    void onConnectionSuccess();
    void onConnectionFailure(const std::string& error);
    void onRetryTimeout();
    void onNetworkRestored();
    void onDisconnect();

    // ---- CR-005 §6: 凭据事件 ----
    /// 凭据无效/过期/撤销 (SEC-1011~1014)：CONNECTING -> CERT_WAIT
    void onCredentialInvalid(const std::string& reason);
    /// 凭据变化/SEC 重启：CONNECTED -> CERT_WAIT
    void onCredentialChanged();
    /// 凭据状态变化：RECONNECTING -> CERT_WAIT
    void onCredentialStateChanged();
    /// CERT_WAIT 退避超时，重新检查凭据状态
    void onCredentialPollTimeout();

    // ---- CR-006 §4: PROV 绑定事件 ----
    /// 绑定有效：PROV_WAIT -> CERT_WAIT
    void onProvisionReady();
    /// 绑定无效/失效：CONNECTING/CONNECTED/RECONNECTING -> PROV_WAIT；PROV_WAIT 保持
    void onProvisionInvalid(const std::string& reason);
    /// 绑定变化：CONNECTED/CONNECTING/RECONNECTING -> PROV_WAIT
    void onProvisionChanged();
    /// PROV_WAIT 退避超时，重新读取绑定
    void onProvisionPollTimeout();

    // ---- CR-007 §7: 订阅门闩与 Cloud Ready 事件 ----
    /// 门闩满足：SUBSCRIPTION_WAIT -> CONNECTING
    void onGateSatisfied();
    /// 门闩超时：SUBSCRIPTION_WAIT -> CONNECTING（标记 degraded，不得 Cloud Ready）
    void onGateTimeout();
    /// CONNACK 后需收敛：CONNECTED -> SUBSCRIBING
    void onSubscribing();
    /// Mandatory 全部 ACTIVE：SUBSCRIBING/CONNECTED/DEGRADED -> CLOUD_READY
    void onSubscriptionsReady();
    /// Mandatory 失败/超时：-> CONNECTED_DEGRADED
    void onMandatoryFailed(const std::string& reason);
    /// Cloud Ready 丢失（新 generation/mandatory 变化）：CLOUD_READY -> SUBSCRIBING
    void onCloudReadyLost();
    bool isDegraded() const;
    void setDegraded(bool d);

    // CR-007 §6: 门闩超时定时
    void setSubscriptionGateDeadline(std::chrono::steady_clock::time_point deadline);
    void onSubscriptionGateTimeout();

    // ---- CR-005 §7: connection generation ----
    uint64_t getConnectionGeneration() const;
    void invalidateConnectionGeneration();

private:
    StateChangeCallback transitionTo(ConnectionState new_state);
    void calculateNextRetry();
    void calculateNextCredentialPoll();  // CR-005 §6: CERT_WAIT 独立退避
    void calculateNextProvisionPoll();   // CR-006 §4: PROV_WAIT 独立退避
    void persistState();
    void restoreState();
    void timerLoop();

    mutable std::mutex mutex_;
    ConnectionStateInfo state_info_;
    StateChangeCallback callback_;
    uint32_t max_retries_ = 10;
    std::chrono::milliseconds base_retry_delay_{1000};
    std::chrono::milliseconds max_retry_delay_{30000};
    std::mt19937 rng_{std::random_device{}()};
    std::shared_ptr<hwyz::store::Store> store_;

    // CR-005 §6: CERT_WAIT 独立退避（不与 Broker 网络重连计数混用）
    CredentialPollConfig credential_poll_config_;
    uint32_t credential_poll_count_ = 0;
    std::chrono::steady_clock::time_point next_credential_poll_at_;

    // CR-006 §4: PROV_WAIT 独立退避（不与 CERT_WAIT / 网络重连混用）
    ProvisionRetryConfig provision_retry_config_;
    uint32_t provision_poll_count_ = 0;
    std::chrono::steady_clock::time_point next_provision_poll_at_;

    // CR-007 §6: SUBSCRIPTION_WAIT 门闩超时 deadline
    std::chrono::steady_clock::time_point subscription_gate_deadline_;

    // CR-005 §7: connection generation（凭据变化时递增，阻止旧 callback 重入 CONNECTED）
    uint64_t connection_generation_ = 0;

    // CR-007 §7: 门闩超时降级标记（不得发布 Cloud Ready）
    bool degraded_ = false;

    std::thread timer_thread_;
    std::condition_variable timer_cv_;
    std::atomic<bool> timer_running_{false};
};

} // namespace mqtt
} // namespace tbox
