#include "sec_tls_key_adapter.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <openssl/err.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/x509_vfy.h>
#include <openssl/ssl.h>
#include <openssl/evp.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/sha.h>
#include <arpa/inet.h>
#include <cstring>

namespace tbox {
namespace mqtt {

// CR-005 §5.1: 通过受控 ENGINE/private-key callback 注册不可导出私钥对象
// 使用 ECDSA_METHOD (OpenSSL 3.0 仍兼容，仅 deprecated 警告) 注册远程签名回调
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif

namespace {

/// EC_KEY ex_data index for adapter pointer (lazy init, process-wide)
int g_ec_key_ex_index = -1;

int getEcKeyExIndex() {
    if (g_ec_key_ex_index < 0) {
        g_ec_key_ex_index = EC_KEY_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
    }
    return g_ec_key_ex_index;
}

/// 计算 SHA-256 并输出小写 hex (DSN-CR-014 §3: 规范 DER 指纹)
std::string sha256Hex(const unsigned char* data, size_t len) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(data, len, digest);
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(SHA256_DIGEST_LENGTH * 2);
    for (unsigned char c : digest) {
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0x0F]);
    }
    return out;
}

/// ECDSA sign callback: OpenSSL CertificateVerify -> SEC signTls (CR-005 §5.2)
/// 禁止自动重放；超时为 unknown outcome，返回 0 终止握手。
int sec_ecdsa_sign(int /*type*/, const unsigned char* dgst, int dgst_len,
                   unsigned char* sig, unsigned int* sig_len,
                   const BIGNUM* /*kinv*/, const BIGNUM* /*r*/, EC_KEY* eckey) {
    auto* adapter = static_cast<SecTlsKeyAdapter*>(
        EC_KEY_get_ex_data(eckey, getEcKeyExIndex()));
    MqttLogAdapter::tls().info(
        "mqtt.tls.sign.enter",
        "TLS CertificateVerify 触发 SEC 远程签名回调",
        {
            {"digest_len", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(dgst_len))},
            {"sig_cap", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(sig_len ? *sig_len : -1))},
            {"adapter_found", tbox::fw::log::FieldValue::makeInt(adapter ? 1 : 0)}
        }
    );
    if (!adapter) {
        MqttLogAdapter::tls().error(
            "mqtt.tls.sign.no_adapter",
            "EC_KEY ex_data 未取到 adapter，无法远程签名",
            {{"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}}
        );
        return 0;
    }

    std::vector<uint8_t> digest(dgst, dgst + dgst_len);
    std::vector<uint8_t> context;
    std::vector<uint8_t> signature;

    if (!adapter->sign(tbox::sec::SignatureAlgorithm::ECDSA_SECP256R1_SHA256,
                       digest, context, signature)) {
        // CR-005 §5.2: sign 超时、IPC 断开或 SEC-1012~1014 时，当前握手立即失败
        MqttLogAdapter::tls().error(
            "mqtt.tls.sign.failed",
            "SEC 远程签名失败，握手中止",
            {{"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}}
        );
        return 0;
    }

    // 注意：OpenSSL ECDSA sign 回调入参 *sig_len 不是缓冲区容量（是未初始化的输出参数）。
    // 真实缓冲区 sig 由调用方按 ECDSA_size(eckey) 预分配，故用它做容量上限校验。
    int cap = ECDSA_size(eckey);
    MqttLogAdapter::tls().info(
        "mqtt.tls.sign.ok",
        "SEC 远程签名返回",
        {
            {"sig_len", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(signature.size()))},
            {"ecdsa_size", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(cap))}
        }
    );

    if (cap <= 0 || static_cast<int>(signature.size()) > cap) {
        MqttLogAdapter::tls().error(
            "mqtt.tls.sign.overflow",
            "SEC 返回签名长度超过 ECDSA 缓冲区容量",
            {
                {"sig_len", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(signature.size()))},
                {"ecdsa_size", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(cap))},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return 0;
    }
    std::memcpy(sig, signature.data(), signature.size());
    *sig_len = static_cast<unsigned int>(signature.size());
    return 1;
}

/// 进程级共享的 EC_KEY_METHOD（仅覆写 sign 回调）。
///
/// EC_KEY_set_method() 仅保存指针、不复制方法结构，因此该方法必须与使用它的
/// EC_KEY/EVP_PKEY/SSL_CTX 至少同生命周期。此方法本身是无状态的（每次签名的
/// 上下文均通过 EC_KEY ex_data 传入），故用进程级单例并有意"泄漏"到进程退出，
/// 避免过早释放导致握手时对已释放方法解引用（use-after-free -> SIGBUS）。
const EC_KEY_METHOD* getSecEcKeyMethod() {
    static EC_KEY_METHOD* method = [] {
        EC_KEY_METHOD* m = EC_KEY_METHOD_new(EC_KEY_OpenSSL());
        if (m) {
            EC_KEY_METHOD_set_sign(m, sec_ecdsa_sign, nullptr, nullptr);
        }
        return m;
    }();
    return method;
}

/// Parse a single DER X509 cert from a byte buffer, advancing the pointer
X509* parseDerX509(const uint8_t* data, size_t len, size_t* consumed) {
    const unsigned char* p = data;
    const unsigned char* start = p;
    X509* cert = d2i_X509(nullptr, &p, static_cast<long>(len));
    if (cert) {
        *consumed = static_cast<size_t>(p - start);
    } else {
        *consumed = 0;
    }
    return cert;
}

} // anonymous namespace

SecTlsKeyAdapter::SecTlsKeyAdapter() = default;

SecTlsKeyAdapter::~SecTlsKeyAdapter() {
    destroyContext();
}

void SecTlsKeyAdapter::destroyContext() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ssl_ctx_) {
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
    }
    // CR-005 §7: 清零 CA/cert/key_ref 缓冲
    current_credential_.root_ca_bundle.clear();
    current_credential_.root_ca_bundle.shrink_to_fit();
    current_credential_.client_cert_chain.clear();
    current_credential_.client_cert_chain.shrink_to_fit();
    current_credential_.private_key_ref.data.clear();
    current_credential_.private_key_ref.data.shrink_to_fit();
    // CR-014 §4.2: 旧会话指纹不跨 context 复用（已由 gate 固化为快照）
    leaf_fingerprint_.clear();
    // CR-017 §3: 同样清理 leaf ecu_uid 与错误分类，避免旧身份被当作新上下文
    leaf_ecu_uid_.clear();
    last_error_code_ = "MQTT-1001";
    last_reason_code_.clear();
    cred_mgr_ = nullptr;
}

bool SecTlsKeyAdapter::normalizeEcuUid(const std::string& raw, std::string& out) {
    out.clear();
    const auto is_ws = [](unsigned char c) {
        return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
    };
    size_t begin = 0;
    size_t end = raw.size();
    while (begin < end && is_ws(static_cast<unsigned char>(raw[begin]))) ++begin;
    while (end > begin && is_ws(static_cast<unsigned char>(raw[end - 1]))) --end;

    std::string norm;
    norm.reserve(end - begin);
    for (size_t i = begin; i < end; ++i) {
        unsigned char c = static_cast<unsigned char>(raw[i]);
        if (c >= 'a' && c <= 'z') c = static_cast<unsigned char>(c - 'a' + 'A');
        norm.push_back(static_cast<char>(c));
    }
    if (norm.empty() || norm.size() > 64) return false;
    for (char c : norm) {
        const bool ok = (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-';
        if (!ok) return false;
    }
    out = std::move(norm);
    return true;
}

bool SecTlsKeyAdapter::isAlgorithmAllowed(tbox::sec::KeyAlgorithm key_alg,
                                          tbox::sec::SignatureAlgorithm sig_alg) {
    // CR-005 §5.2: adapter SHALL 校验 OpenSSL 请求算法属于 SEC bundle 声明的允许集合
    if (key_alg == tbox::sec::KeyAlgorithm::ECDSA_P256 &&
        sig_alg == tbox::sec::SignatureAlgorithm::ECDSA_SECP256R1_SHA256) {
        return true;
    }
    return false;
}

bool SecTlsKeyAdapter::sign(tbox::sec::SignatureAlgorithm algorithm,
                            const std::vector<uint8_t>& digest,
                            const std::vector<uint8_t>& context,
                            std::vector<uint8_t>& signature) {
    // CR-005 §5.2: 校验算法属于 SEC bundle 允许集合
    if (!isAlgorithmAllowed(current_credential_.key_algorithm, algorithm)) {
        MqttLogAdapter::tls().error(
            "mqtt.tls.algorithm_rejected",
            "签名算法不在 SEC bundle 允许集合",
            {
                {"algorithm", tbox::fw::log::FieldValue::makeString(
                    tbox::sec::signature_algorithm_to_string(algorithm))},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
    }

    if (!cred_mgr_) {
        last_error_ = "CredentialManager not set";
        return false;
    }

    tbox::sec::TlsSignRequest req;
    req.private_key_ref = current_credential_.private_key_ref;
    req.algorithm = algorithm;
    req.digest = digest;
    req.context = context;
    req.request_id = generateRequestId();

    // CR-005 §5.2: sign 超时、IPC 断开或 SEC-1012~1014 时，当前握手立即失败
    // 不得在同一握手中自动重放
    bool ok = cred_mgr_->signTls(req, signature);
    MqttLogAdapter::tls().info(
        "mqtt.tls.sign.sec_result",
        "SEC signTls 调用返回",
        {
            {"ok", tbox::fw::log::FieldValue::makeInt(ok ? 1 : 0)},
            {"sig_len", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(signature.size()))},
            {"digest_len", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(digest.size()))},
            {"key_ref_len", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(req.private_key_ref.data.size()))},
            {"last_error", tbox::fw::log::FieldValue::makeString(cred_mgr_->getLastError())}
        }
    );
    return ok;
}

SSL_CTX* SecTlsKeyAdapter::buildTlsContext(const ActiveTlsCredential& credential,
                                           CredentialManager& cred_mgr,
                                           const std::string& broker_host,
                                           TlsVersion min_tls,
                                           TlsVersion max_tls) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Clean up any previous context
    if (ssl_ctx_) {
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
    }

    current_credential_ = credential;
    cred_mgr_ = &cred_mgr;
    // CR-017 §3: 每次构建重置 leaf 身份与错误分类，避免旧会话身份被复用。
    leaf_fingerprint_.clear();
    leaf_ecu_uid_.clear();
    last_error_code_ = "MQTT-1001";
    last_reason_code_.clear();

    if (!credential.ready) {
        last_error_ = "Credential not ready";
        return nullptr;
    }

    // Create SSL_CTX for TLS client
    ssl_ctx_ = SSL_CTX_new(TLS_client_method());
    if (!ssl_ctx_) {
        last_error_ = "SSL_CTX_new failed";
        return nullptr;
    }

    // Set TLS version range (CR-005 §5.1)
    setTlsVersionRange(ssl_ctx_, min_tls, max_tls);

    // Load root CA bundle into X509_STORE (CR-005 §5.1)
    if (!loadRootCaBundle(ssl_ctx_, credential.root_ca_bundle)) {
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
        return nullptr;
    }

    // Load client cert chain (leaf -> intermediate) (CR-005 §5.1)
    if (!loadClientCertChain(ssl_ctx_, credential.client_cert_chain)) {
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
        return nullptr;
    }

    // Register remote key via ECDSA_METHOD (CR-005 §5.1)
    if (!registerRemoteKey(ssl_ctx_, credential, cred_mgr)) {
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
        return nullptr;
    }

    // Require mutual TLS (verify server cert) (CR-005 §5.1)
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_PEER, nullptr);
    SSL_CTX_set_verify_depth(ssl_ctx_, 4);

    // Bind server certificate identity to current broker_host (CR-013 §10.2)
    // hostname verification 挂到 SSL_CTX verify param，所有连接继承；
    // 链合法但 SAN/CN 与 broker_host 不匹配时握手失败（MQTT-1001），fail-closed。
    if (!setServerHostnameVerification(ssl_ctx_, broker_host)) {
        SSL_CTX_free(ssl_ctx_);
        ssl_ctx_ = nullptr;
        return nullptr;
    }

    MqttLogAdapter::tls().info(
        "mqtt.tls.context_built",
        "TLS context 构建完成（SEC 凭据）",
        {
            {"profile", tbox::fw::log::FieldValue::makeString(cred_mgr.getProfile())},
            {"version", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(credential.version))}
        }
    );

    return ssl_ctx_;
}

bool SecTlsKeyAdapter::setServerHostnameVerification(SSL_CTX* ctx,
                                                     const std::string& broker_host) {
    if (broker_host.empty()) {
        last_error_ = "broker_host is empty, cannot configure hostname verification";
        return false;
    }

    X509_VERIFY_PARAM* param = SSL_CTX_get0_param(ctx);
    if (!param) {
        last_error_ = "SSL_CTX_get0_param failed";
        return false;
    }

    // RFC 6125：仅允许最左标签完整 '*'，禁止部分通配符（如 www*.example.com）。
    // 如量产证书策略确认不使用通配符，可进一步收紧为 X509_CHECK_FLAG_NO_WILDCARDS。
    X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);

    struct in_addr addr4{};
    struct in6_addr addr6{};
    const bool is_ip =
        (inet_pton(AF_INET, broker_host.c_str(), &addr4) == 1) ||
        (inet_pton(AF_INET6, broker_host.c_str(), &addr6) == 1);

    if (is_ip) {
        // IP 地址：仅接受匹配的 IP SAN，不以 DNS SAN 或 CN 代替 (CR-013 §10.2)
        if (X509_VERIFY_PARAM_set1_ip_asc(param, broker_host.c_str()) != 1) {
            last_error_ = "X509_VERIFY_PARAM_set1_ip_asc failed";
            return false;
        }
    } else {
        // 域名：按 SAN 优先、兼容 CN 的 RFC 6125/OpenSSL 规则匹配
        if (X509_VERIFY_PARAM_set1_host(param, broker_host.c_str(), 0) != 1) {
            last_error_ = "X509_VERIFY_PARAM_set1_host failed";
            return false;
        }
    }
    return true;
}

bool SecTlsKeyAdapter::loadRootCaBundle(SSL_CTX* ctx,
                                        const std::vector<uint8_t>& der_bundle) {
    if (der_bundle.empty()) {
        last_error_ = "root CA bundle is empty";
        return false;
    }

    X509_STORE* store = SSL_CTX_get_cert_store(ctx);
    if (!store) {
        store = X509_STORE_new();
        SSL_CTX_set_cert_store(ctx, store);
    }

    // Parse concatenated DER certs and add to store (CR-005 §5.1)
    const uint8_t* p = der_bundle.data();
    size_t remaining = der_bundle.size();
    int count = 0;

    while (remaining > 0) {
        size_t consumed = 0;
        X509* cert = parseDerX509(p, remaining, &consumed);
        if (!cert) {
            break;
        }
        X509_STORE_add_cert(store, cert);
        X509_free(cert);
        p += consumed;
        remaining -= consumed;
        count++;
    }

    if (count == 0) {
        last_error_ = "failed to parse any root CA from DER bundle";
        return false;
    }

    MqttLogAdapter::tls().debug(
        "mqtt.tls.root_ca_loaded",
        "根 CA bundle 已装入 X509_STORE",
        {{"ca_count", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(count))}}
    );
    return true;
}

bool SecTlsKeyAdapter::loadClientCertChain(SSL_CTX* ctx,
                                           const std::vector<uint8_t>& der_chain) {
    if (der_chain.empty()) {
        last_error_ = "client cert chain is empty";
        return false;
    }

    // Parse concatenated DER certs: first is leaf, rest are intermediates (CR-005 §5.1)
    const uint8_t* p = der_chain.data();
    size_t remaining = der_chain.size();
    int index = 0;

    while (remaining > 0) {
        size_t consumed = 0;
        X509* cert = parseDerX509(p, remaining, &consumed);
        if (!cert) {
            break;
        }

        if (index == 0) {
            // Leaf cert: set as the client certificate
            // CR-005 §5.1: 校验有效期、EKU、算法和链完整性
            // CR-014 §4.1: 固化实际写入 SSL_CTX 的 leaf 证书身份（规范 DER SHA-256）
            int der_len = i2d_X509(cert, nullptr);
            if (der_len > 0) {
                std::vector<unsigned char> der(static_cast<size_t>(der_len));
                unsigned char* der_ptr = der.data();
                if (i2d_X509(cert, &der_ptr) == der_len) {
                    leaf_fingerprint_ = "sha256:" + sha256Hex(der.data(), der.size());
                }
            }
            if (leaf_fingerprint_.empty()) {
                // 回退：直接对输入 DER 片段求指纹（保证身份仍可用，不阻断连接）
                leaf_fingerprint_ = "sha256:" + sha256Hex(p, consumed);
            }

            // CR-017 §3: 从实际 leaf 证书 CN 提取并规范化 ecu_uid。
            // CN 缺失/歧义/非法时 fail-closed（保留 MQTT-1009 分类），
            // 不 fallback 到 PROV/clientId/配置。
            {
                X509_NAME* subject = X509_get_subject_name(cert);
                int first = (subject != nullptr)
                    ? X509_NAME_get_index_by_NID(subject, NID_commonName, -1)
                    : -1;
                if (first < 0) {
                    last_error_ = "client leaf certificate has no CN";
                    last_error_code_ = "MQTT-1009";
                    last_reason_code_ = session_reason::kSessionIdentityMissing;
                    X509_free(cert);
                    return false;
                }
                int second = X509_NAME_get_index_by_NID(subject, NID_commonName, first);
                if (second >= 0) {
                    last_error_ = "client leaf certificate has ambiguous (multiple) CN";
                    last_error_code_ = "MQTT-1009";
                    last_reason_code_ = session_reason::kSessionIdentityInvalid;
                    X509_free(cert);
                    return false;
                }
                X509_NAME_ENTRY* entry = X509_NAME_get_entry(subject, first);
                ASN1_STRING* cn_data = (entry != nullptr) ? X509_NAME_ENTRY_get_data(entry) : nullptr;
                std::string cn;
                if (cn_data != nullptr) {
                    unsigned char* utf8 = nullptr;
                    int len = ASN1_STRING_to_UTF8(&utf8, cn_data);
                    if (len >= 0 && utf8 != nullptr) {
                        cn.assign(reinterpret_cast<char*>(utf8), static_cast<size_t>(len));
                    }
                    if (utf8 != nullptr) OPENSSL_free(utf8);
                }
                std::string normalized;
                if (!normalizeEcuUid(cn, normalized)) {
                    last_error_ = "client leaf certificate CN is not a valid ecu_uid";
                    last_error_code_ = "MQTT-1009";
                    last_reason_code_ = session_reason::kSessionIdentityInvalid;
                    X509_free(cert);
                    return false;
                }
                leaf_ecu_uid_ = std::move(normalized);
            }

            if (SSL_CTX_use_certificate(ctx, cert) != 1) {
                last_error_ = "SSL_CTX_use_certificate failed for leaf cert";
                X509_free(cert);
                return false;
            }
            X509_free(cert);  // SSL_CTX_use_certificate increments ref, free our copy
        } else {
            // Intermediate: add as extra chain cert (ownership transfers)
            if (SSL_CTX_add_extra_chain_cert(ctx, cert) != 1) {
                X509_free(cert);
                // Non-fatal: continue without this intermediate
            }
            // Do NOT free on success - ownership transferred to SSL_CTX
        }

        p += consumed;
        remaining -= consumed;
        index++;
    }

    if (index == 0) {
        last_error_ = "failed to parse any client cert from DER chain";
        return false;
    }

    MqttLogAdapter::tls().debug(
        "mqtt.tls.cert_chain_loaded",
        "客户端证书链已配置",
        {{"cert_count", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(index))}}
    );
    return true;
}

void SecTlsKeyAdapter::setTlsVersionRange(SSL_CTX* ctx, TlsVersion min_tls, TlsVersion max_tls) {
    int min_ver = TLS1_2_VERSION;
    int max_ver = TLS1_3_VERSION;

    if (min_tls == TlsVersion::TLS_1_2) min_ver = TLS1_2_VERSION;
    else if (min_tls == TlsVersion::TLS_1_3) min_ver = TLS1_3_VERSION;

    if (max_tls == TlsVersion::TLS_1_2) max_ver = TLS1_2_VERSION;
    else if (max_tls == TlsVersion::TLS_1_3) max_ver = TLS1_3_VERSION;

    SSL_CTX_set_min_proto_version(ctx, min_ver);
    SSL_CTX_set_max_proto_version(ctx, max_ver);

    // CR-005 §5.2: SEC 仅允许 ecdsa_secp256r1_sha256（digest 必须为 32 字节）。
    // 不约束 sigalgs 时 TLS 1.2 下 OpenSSL 可能协商 ECDSA+SHA384/SHA512，导致
    // 远程签名回调收到 48/64 字节摘要，SEC 以 SEC-1013 fail-closed 拒绝并中止握手。
    // 这里把客户端可用签名算法收敛到唯一允许项，使协商结果与 SEC 约束一致。
    if (SSL_CTX_set1_sigalgs_list(ctx, "ECDSA+SHA256") != 1) {
        MqttLogAdapter::tls().warn(
            "mqtt.tls.sigalgs_restrict_failed",
            "签名算法列表约束失败，可能协商到 SEC 不允许的摘要算法",
            {{"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}}
        );
    }
    if (SSL_CTX_set1_client_sigalgs_list(ctx, "ECDSA+SHA256") != 1) {
        MqttLogAdapter::tls().warn(
            "mqtt.tls.client_sigalgs_restrict_failed",
            "客户端签名算法列表约束失败",
            {{"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}}
        );
    }
}

bool SecTlsKeyAdapter::registerRemoteKey(SSL_CTX* ctx,
                                         const ActiveTlsCredential& credential,
                                         CredentialManager& cred_mgr) {
    // CR-005 §5.1: 通过受控 ENGINE/private-key callback 注册不可导出私钥对象
    // TLS adapter 使用 private_key_ref 标识签名能力，不生成临时 key 文件

    if (credential.key_algorithm != tbox::sec::KeyAlgorithm::ECDSA_P256) {
        last_error_ = "unsupported key algorithm (only ECDSA_P256 supported)";
        MqttLogAdapter::tls().error(
            "mqtt.tls.algorithm_rejected",
            "不支持的密钥算法",
            {{"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}}
        );
        return false;
    }

    // Get the public key from the client cert (already loaded)
    X509* cert = SSL_CTX_get0_certificate(ctx);
    if (!cert) {
        last_error_ = "no client certificate loaded";
        return false;
    }

    // X509_get_pubkey returns a new EVP_PKEY reference (must free)
    EVP_PKEY* pubkey = X509_get_pubkey(cert);
    if (!pubkey) {
        last_error_ = "failed to get public key from cert";
        return false;
    }

    // Get EC_KEY from public key (deprecated in 3.0 but functional for ECDSA_METHOD)
    EC_KEY* ec_key = EVP_PKEY_get1_EC_KEY(pubkey);  // increments ref count
    EVP_PKEY_free(pubkey);
    if (!ec_key) {
        last_error_ = "certificate public key is not EC";
        return false;
    }

    // Create custom EC key method with remote sign (CR-005 §5.2)
    // Use EC_KEY_METHOD (OpenSSL 1.1.0+, replaces deprecated ECDSA_METHOD)。
    // 使用进程级共享单例：EC_KEY_set_method 只保存指针不复制，方法必须与 key 同生命周期，
    // 不能在 set 之后释放（否则握手签名时 use-after-free -> SIGBUS）。
    const EC_KEY_METHOD* ec_method = getSecEcKeyMethod();
    if (!ec_method) {
        EC_KEY_free(ec_key);
        last_error_ = "EC_KEY_METHOD_new failed";
        return false;
    }

    // Set custom EC key method on EC_KEY
    if (EC_KEY_set_method(ec_key, ec_method) != 1) {
        EC_KEY_free(ec_key);
        last_error_ = "EC_KEY_set_method failed";
        return false;
    }
    // 注意：不要释放 ec_method —— 它是进程级单例，且 EC_KEY 持有其指针。

    // Set adapter pointer in EC_KEY ex_data (for sign callback lookup)
    EC_KEY_set_ex_data(ec_key, getEcKeyExIndex(), this);

    // Create EVP_PKEY from EC_KEY
    EVP_PKEY* pkey = EVP_PKEY_new();
    if (!pkey) {
        EC_KEY_free(ec_key);
        last_error_ = "EVP_PKEY_new failed";
        return false;
    }
    if (EVP_PKEY_set1_EC_KEY(pkey, ec_key) != 1) {
        EVP_PKEY_free(pkey);
        EC_KEY_free(ec_key);
        last_error_ = "EVP_PKEY_set1_EC_KEY failed";
        return false;
    }
    EC_KEY_free(ec_key);  // EVP_PKEY_set1_EC_KEY incremented ref, free our copy

    // Set as SSL_CTX private key
    if (SSL_CTX_use_PrivateKey(ctx, pkey) != 1) {
        EVP_PKEY_free(pkey);
        last_error_ = "SSL_CTX_use_PrivateKey failed";
        MqttLogAdapter::tls().error(
            "mqtt.tls.handshake_failed",
            "SSL_CTX_use_PrivateKey 失败",
            {{"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}}
        );
        return false;
    }
    EVP_PKEY_free(pkey);  // SSL_CTX increments ref

    MqttLogAdapter::tls().info(
        "mqtt.tls.remote_key_registered",
        "SEC 远程私钥已注册（ECDSA_METHOD）",
        {{"profile", tbox::fw::log::FieldValue::makeString(cred_mgr.getProfile())}}
    );
    return true;
}

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif

} // namespace mqtt
} // namespace tbox
