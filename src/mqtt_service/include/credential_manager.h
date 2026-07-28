#pragma once

#include "mqtt_types.h"
#include "tbox/sec/client.h"
#include "tbox/sec/types.h"
#include "tbox/sec/errors.h"
#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include <functional>
#include <mutex>

namespace tbox {
namespace mqtt {

/// CredentialManager (CR-005 §4, §7)
///
/// 替代旧 SecCertInterface，使用 tbox::sec_client 的 get/state/sign/subscribe
/// facade 获取 TLS 凭据和远程签名能力。
///
/// 职责：
/// - 经 getTlsCredential("mqtt") 获取完整凭据 bundle（根 CA + 客户端证书链 + opaque private_key_ref）
/// - 经 getTlsCredentialState("mqtt") 周期核对状态
/// - 经 signTls() 委托 TLS CertificateVerify 私钥签名（禁止自动重放）
/// - 经 subscribeTlsCredentialChanged("mqtt") 监听凭据变化（独立 IPC 连接）
/// - 维护 credential generation（版本变化/SEC 重启时递增）
/// - 提供线程安全快照访问（网络线程、TLS callback、SEC event callback 共用）
///
/// 不直接接触 SEC socket、method_id 或 JSON (CR-005 §2)。
class CredentialManager {
public:
    using CredentialChangedCallback = std::function<void()>;

    /// @param sec_socket_path SEC daemon IPC socket 路径
    /// @param profile SEC TLS profile 名（固定 "mqtt"）
    explicit CredentialManager(const std::string& sec_socket_path = "/tmp/tbox-sec.sock",
                               const std::string& profile = "mqtt");
    ~CredentialManager();

    CredentialManager(const CredentialManager&) = delete;
    CredentialManager& operator=(const CredentialManager&) = delete;

    // ---- 生命周期 ----

    /// 初始化 SEC client 并建立连接（CR-005 §4.2）
    /// SEC client 不可用时 MQTT 服务仍可启动内部 IPC 与离线队列，
    /// 但云端连接保持 CERT_WAIT (CR-005 §4.2)。
    bool initialize();

    // ---- SEC facade 封装 (CR-005 §4.1) ----

    /// 获取完整凭据 bundle (CR-005 §4.1)
    /// 只读，传输失败允许单次重试。
    /// @return true 如果 status==READY 且 bundle 完整；false 并设置 last_error_
    bool getTlsCredential(ActiveTlsCredential& credential);

    /// 查询凭据状态摘要（不含凭据内容）(CR-005 §4.1)
    /// 只读，允许单次重试。
    bool getTlsCredentialState(tbox::sec::TlsCredentialState& state);

    /// 远程 TLS 签名 (CR-005 §4.1, §5.2)
    /// 禁止自动重放；超时为 unknown outcome，调用方应终止本次握手。
    /// @return true 签名成功；false 签名失败（SEC-1012~1014 或传输故障）
    bool signTls(const tbox::sec::TlsSignRequest& request,
                 std::vector<uint8_t>& signature);

    /// 订阅凭据变化事件 (CR-005 §4.1, §7)
    /// 独立 IPC 订阅连接，断线退避恢复。
    /// payload 仅用于触发，不直接替换凭据；MQTT 必须重新调用 getTlsCredential()。
    bool subscribeTlsCredentialChanged(CredentialChangedCallback callback);

    // ---- 快照与 generation (CR-005 §7) ----

    /// 获取当前凭据快照（线程安全）(CR-005 §7)
    /// 网络线程、TLS callback 和 SEC event callback 只通过快照访问当前凭据。
    ActiveTlsCredential getSnapshot() const;

    /// 获取当前 credential generation (CR-005 §7)
    uint64_t getGeneration() const;

    /// 检查凭据是否就绪（便捷方法）
    bool isCredentialReady() const;

    /// 获取当前 profile
    const std::string& getProfile() const { return profile_; }

    // ---- 凭据失效与清理 (CR-005 §7) ----

    /// 标记当前凭据 generation 失效（递增 generation）(CR-005 §7)
    /// 收到凭据变化事件时先调用此方法，再停止 TLS session、断开 Broker、
    /// 清理旧 credential/context，避免旧连接回调竞态重新进入 CONNECTED。
    void invalidateCredential();

    /// 清理当前凭据（清零 CA/cert/key_ref 缓冲）(CR-005 §7 cleanup 顺序)
    void cleanupCredential();

    // ---- SEC 连接状态 ----

    bool isSecConnected() const;
    std::string getLastError() const;

    // ---- 错误映射 (CR-005 §9) ----

    /// 将 SEC 错误码映射为 MQTT 事件名 (CR-005 §9)
    static std::string secErrorToMqttEvent(tbox::sec::ErrorCode sec_error);

    /// 将 SEC 错误码映射为可读 reason_code (CR-005 §9)
    static std::string secErrorToReasonCode(tbox::sec::ErrorCode sec_error);

    // ---- 测试模拟 ----

    /// 模拟凭据就绪（测试用，绕过 SEC client）
    void simulateReady(const ActiveTlsCredential& credential = {});

    /// 模拟凭据未就绪
    void simulateNotReady();

    /// 模拟凭据状态变化（触发 changed callback）
    void simulateStateChanged();

    /// 检查是否处于模拟模式
    bool isSimulated() const { return simulated_; }

private:
    /// 从 SEC bundle 映射为 ActiveTlsCredential
    void mapBundleToCredential(const tbox::sec::TlsCredentialBundle& bundle,
                               ActiveTlsCredential& credential);

    std::string sec_socket_path_;
    std::string profile_;
    std::unique_ptr<tbox::sec::SecClient> sec_client_;
    tbox::sec::TlsCredentialSubscription subscription_;

    mutable std::mutex mutex_;
    ActiveTlsCredential current_credential_;
    uint64_t generation_ = 0;
    bool simulated_ = false;
    std::string last_error_;
    CredentialChangedCallback changed_callback_;
};

} // namespace mqtt
} // namespace tbox
