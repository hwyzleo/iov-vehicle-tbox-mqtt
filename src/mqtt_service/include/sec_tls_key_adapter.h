#pragma once

#include "mqtt_types.h"
#include "credential_manager.h"
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <string>
#include <vector>
#include <memory>
#include <mutex>

namespace tbox {
namespace mqtt {

/// SecTlsKeyAdapter (CR-005 §5)
///
/// OpenSSL SEC 私钥适配器：将 TLS CertificateVerify 私钥签名委托给 SEC/HSM。
///
/// 职责：
/// - 构建独立 SSL_CTX，装入 SEC 提供的 root CA bundle 和客户端证书链
/// - 经 OpenSSL ENGINE 注册不可导出私钥对象（sign callback 委托 signTls）
/// - 校验 OpenSSL 请求算法属于 SEC bundle 声明的允许集合
/// - 不生成临时 key 文件、不伪装文件路径、不直接调用 HSM (CR-005 §5.1)
///
/// 签名流程 (CR-005 §5.2)：
///   OpenSSL CertificateVerify
///     -> SecTlsKeyAdapter::sign(algorithm, digest, context)
///     -> CredentialManager::signTls(private_key_ref, ...)
///     -> framework-ipc -> SEC/HSM sign -> signature -> OpenSSL continues
class SecTlsKeyAdapter {
public:
    SecTlsKeyAdapter();
    ~SecTlsKeyAdapter();

    SecTlsKeyAdapter(const SecTlsKeyAdapter&) = delete;
    SecTlsKeyAdapter& operator=(const SecTlsKeyAdapter&) = delete;

    /// 基于 SEC 凭据构建 SSL_CTX (CR-005 §5.1)
    /// @param credential 当前活跃凭据快照
    /// @param cred_mgr CredentialManager 引用（用于 signTls 委托）
    /// @param broker_host 当前生效的 broker_host（TCP 目标 + hostname verification 唯一依据, CR-013 §10.2）
    /// @param min_tls 最低 TLS 版本
    /// @param max_tls 最高 TLS 版本
    /// @return 非 nullptr 表示成功；调用方通过 SSL_CTX_free 或 destroyContext 释放
    SSL_CTX* buildTlsContext(const ActiveTlsCredential& credential,
                             CredentialManager& cred_mgr,
                             const std::string& broker_host,
                             TlsVersion min_tls,
                             TlsVersion max_tls);

    /// 销毁当前 SSL_CTX 并清零缓冲 (CR-005 §7 cleanup)
    void destroyContext();

    /// 获取当前 SSL_CTX（可能为 nullptr）
    SSL_CTX* getSslCtx() const { return ssl_ctx_; }

    /// 检查当前 context 是否有效
    bool isContextValid() const { return ssl_ctx_ != nullptr; }

    /// 校验算法是否属于 SEC bundle 允许集合 (CR-005 §5.2)
    static bool isAlgorithmAllowed(tbox::sec::KeyAlgorithm key_alg,
                                   tbox::sec::SignatureAlgorithm sig_alg);

    /// 直接签名接口（供 ENGINE callback 和测试调用）(CR-005 §5.2)
    /// @return true 签名成功；false 终止握手（不得在同一握手中自动重放）
    bool sign(tbox::sec::SignatureAlgorithm algorithm,
              const std::vector<uint8_t>& digest,
              const std::vector<uint8_t>& context,
              std::vector<uint8_t>& signature);

    /// 获取最后错误
    std::string getLastError() const { return last_error_; }

    /// 获取本次 TLS context 实际装载的 leaf 客户端证书指纹 (DSN-CR-014 §4.1)
    /// 规范 DER 的 SHA-256，格式 "sha256:<hex>"；无 leaf 时为空字符串。
    const std::string& getLeafFingerprint() const { return leaf_fingerprint_; }

    /// CR-017 §3: 本次 TLS context 实际装载的 leaf 客户端证书 CN 规范化后的 ecu_uid。
    /// 仅在 CN 合法时非空；缺失/歧义/非法时为空。
    const std::string& getLeafEcUid() const { return leaf_ecu_uid_; }

    /// 最后一次错误的 MQTT 错误码（如 "MQTT-1009" / "MQTT-1001"）。
    /// CR-017：CN 身份错误保留 MQTT-1009，不因函数名统一成 MQTT-1001。
    const std::string& getLastErrorCode() const { return last_error_code_; }

    /// 最后一次错误的稳定 reason code（如 SESSION_IDENTITY_INVALID）。
    const std::string& getLastReasonCode() const { return last_reason_code_; }

    /// CR-017 §3: 将 leaf CN 规范化为 ecu_uid（去首尾空白、ASCII 大写、^[A-Z0-9-]{1,64}$）。
    /// @return 合法返回 true；否则 false（不 fallback）。
    static bool normalizeEcuUid(const std::string& raw, std::string& out);

private:
    /// 解析 DER root CA bundle 并装入 X509_STORE (CR-005 §5.1)
    bool loadRootCaBundle(SSL_CTX* ctx, const std::vector<uint8_t>& der_bundle);

    /// 解析 DER 客户端证书链并配置到 SSL_CTX (leaf -> intermediate) (CR-005 §5.1)
    bool loadClientCertChain(SSL_CTX* ctx, const std::vector<uint8_t>& der_chain);

    /// 设置 TLS 版本范围
    void setTlsVersionRange(SSL_CTX* ctx, TlsVersion min_tls, TlsVersion max_tls);

    /// 配置服务器证书主机名校验 (CR-013 §10.2)
    ///
    /// hostname verification 挂到 SSL_CTX verify param，所有基于该 context 的连接继承。
    /// - 域名：X509_VERIFY_PARAM_set1_host + X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS（RFC 6125，
    ///   仅允许最左标签完整 *，禁止部分通配符）
    /// - IP 地址：X509_VERIFY_PARAM_set1_ip_asc（仅 IP SAN，不以 DNS SAN/CN 代替）
    /// - broker_host 为空：fail-closed，拒绝构建
    /// @return true 设置成功；false 设置失败（调用方应终止构建）
    bool setServerHostnameVerification(SSL_CTX* ctx, const std::string& broker_host);

    /// 注册 ENGINE 远程签名私钥 (CR-005 §5.1)
    bool registerRemoteKey(SSL_CTX* ctx,
                           const ActiveTlsCredential& credential,
                           CredentialManager& cred_mgr);

    SSL_CTX* ssl_ctx_ = nullptr;
    std::string last_error_;
    // DSN-CR-014 §4.1: 实际写入 SSL_CTX 的 leaf 证书指纹 "sha256:<hex>"
    std::string leaf_fingerprint_;
    // CR-017 §3: 实际写入 SSL_CTX 的 leaf 客户端证书 CN 规范化 ecu_uid
    std::string leaf_ecu_uid_;
    // CR-017: 错误分类（CN 身份错误保留 MQTT-1009，TLS 材料/配置错误 MQTT-1001）
    std::string last_error_code_ = "MQTT-1001";
    std::string last_reason_code_;

    // 持有当前凭据快照和 CredentialManager 指针，供 ENGINE sign callback 使用
    // 注意：ENGINE callback 是 C 函数，通过 ex_data 或全局指针访问此对象
    ActiveTlsCredential current_credential_;
    CredentialManager* cred_mgr_ = nullptr;
    mutable std::mutex mutex_;
};

} // namespace mqtt
} // namespace tbox
