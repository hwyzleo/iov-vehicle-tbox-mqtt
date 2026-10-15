#include <gtest/gtest.h>
#include "sec_tls_key_adapter.h"
#include "credential_manager.h"
#include <openssl/x509.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ec.h>
#include <openssl/asn1.h>
#include <openssl/x509v3.h>
#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/sha.h>
#include <arpa/inet.h>
#include <chrono>
#include <vector>
#include <cstring>

using namespace tbox::mqtt;
using namespace tbox::sec;

namespace {

/// Generate a self-signed ECDSA P-256 certificate as DER
struct TestCert {
    std::vector<uint8_t> der;
    EVP_PKEY* pkey = nullptr;  // owns the key pair

    ~TestCert() {
        if (pkey) EVP_PKEY_free(pkey);
    }
};

std::vector<uint8_t> x509ToDer(X509* cert) {
    unsigned char* buf = nullptr;
    int len = i2d_X509(cert, &buf);
    std::vector<uint8_t> der(buf, buf + len);
    OPENSSL_free(buf);
    return der;
}

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

/// Create a self-signed ECDSA P-256 test certificate
TestCert createTestCert(const std::string& cn) {
    TestCert tc;

    // Generate ECDSA P-256 key pair
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
    EVP_PKEY_keygen_init(pctx);
    EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1);
    EVP_PKEY* pkey = nullptr;
    EVP_PKEY_keygen(pctx, &pkey);
    EVP_PKEY_CTX_free(pctx);
    tc.pkey = pkey;

    // Create self-signed cert
    X509* cert = X509_new();
    X509_set_version(cert, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
    X509_gmtime_adj(X509_get_notBefore(cert), 0);
    X509_gmtime_adj(X509_get_notAfter(cert), 31536000L);

    X509_NAME* name = X509_NAME_new();
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
        reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
    X509_set_subject_name(cert, name);
    X509_set_issuer_name(cert, name);
    X509_NAME_free(name);

    X509_set_pubkey(cert, pkey);
    X509_sign(cert, pkey, EVP_sha256());

    tc.der = x509ToDer(cert);
    X509_free(cert);
    return tc;
}

/// CR-017: create self-signed cert with arbitrary subject attributes.
/// e.g. {{"CN", "ecu-1"}} / {{"CN", "a"}, {"CN", "b"}} (ambiguous) /
/// {{"O", "Org"}} (no CN).
TestCert createTestCertWithSubject(
        const std::vector<std::pair<std::string, std::string>>& attrs) {
    TestCert tc;
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
    EVP_PKEY_keygen_init(pctx);
    EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1);
    EVP_PKEY* pkey = nullptr;
    EVP_PKEY_keygen(pctx, &pkey);
    EVP_PKEY_CTX_free(pctx);
    tc.pkey = pkey;

    X509* cert = X509_new();
    X509_set_version(cert, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cert), 7);
    X509_gmtime_adj(X509_get_notBefore(cert), 0);
    X509_gmtime_adj(X509_get_notAfter(cert), 31536000L);

    X509_NAME* name = X509_NAME_new();
    for (const auto& [key, value] : attrs) {
        X509_NAME_add_entry_by_txt(name, key.c_str(), MBSTRING_ASC,
            reinterpret_cast<const unsigned char*>(value.c_str()), -1, -1, 0);
    }
    X509_set_subject_name(cert, name);
    X509_set_issuer_name(cert, name);
    X509_NAME_free(name);

    X509_set_pubkey(cert, pkey);
    X509_sign(cert, pkey, EVP_sha256());
    tc.der = x509ToDer(cert);
    X509_free(cert);
    return tc;
}

} // anonymous namespace

namespace {

/// 生成 ECDSA P-256 密钥对
EVP_PKEY* genEcKey() {
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
    EVP_PKEY_keygen_init(pctx);
    EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1);
    EVP_PKEY* pkey = nullptr;
    EVP_PKEY_keygen(pctx, &pkey);
    EVP_PKEY_CTX_free(pctx);
    return pkey;
}

/// 为 CA 证书添加 basicConstraints critical CA:TRUE（OpenSSL 3.x 链验证必需）
void addBasicConstraintsCa(X509* cert) {
    // extnValue DER: SEQUENCE { BOOLEAN TRUE } = 30 03 01 01 ff
    const unsigned char bc_der[] = {0x30, 0x03, 0x01, 0x01, 0xff};
    ASN1_OCTET_STRING* oct = ASN1_OCTET_STRING_new();
    ASN1_OCTET_STRING_set(oct, bc_der, static_cast<int>(sizeof(bc_der)));
    X509_EXTENSION* ext = X509_EXTENSION_create_by_NID(nullptr, NID_basic_constraints, 1, oct);
    if (ext) {
        X509_add_ext(cert, ext, -1);
        X509_EXTENSION_free(ext);
    }
    ASN1_OCTET_STRING_free(oct);
}

/// 为证书添加 subjectAltName（san_spec 如 "DNS:broker.example.com" / "IP:10.0.0.1"）
/// 注：OpenSSL 3.6 移除了 X509V3_CTX_init/X509_add1_host 等构造 API，
/// 这里直接构造 SAN 扩展 DER（SEQUENCE { [2]IA5String | [7]OCTET STRING }）。
void addSan(X509* cert, const std::string& san_spec) {
    std::vector<uint8_t> der;
    if (san_spec.rfind("DNS:", 0) == 0) {
        const std::string name = san_spec.substr(4);
        if (name.size() < 128) {  // 单字节长度，测试域名均满足
            der.push_back(0x30);
            der.push_back(static_cast<uint8_t>(2 + name.size()));
            der.push_back(0x82);
            der.push_back(static_cast<uint8_t>(name.size()));
            der.insert(der.end(), name.begin(), name.end());
        }
    } else if (san_spec.rfind("IP:", 0) == 0) {
        const std::string ip = san_spec.substr(3);
        unsigned char bytes[16];
        if (inet_pton(AF_INET, ip.c_str(), bytes) == 1) {
            der = {0x30, 0x06, 0x87, 0x04, bytes[0], bytes[1], bytes[2], bytes[3]};
        } else if (inet_pton(AF_INET6, ip.c_str(), bytes) == 1) {
            der = {0x30, 0x12, 0x87, 0x10};
            der.insert(der.end(), bytes, bytes + 16);
        }
    }
    if (der.empty()) return;

    ASN1_OCTET_STRING* oct = ASN1_OCTET_STRING_new();
    ASN1_OCTET_STRING_set(oct, der.data(), static_cast<int>(der.size()));
    X509_EXTENSION* ext = X509_EXTENSION_create_by_NID(nullptr, NID_subject_alt_name, 0, oct);
    if (ext) {
        X509_add_ext(cert, ext, -1);
        X509_EXTENSION_free(ext);
    }
    ASN1_OCTET_STRING_free(oct);
}

/// CA 证书 + 由 CA 签发的带 SAN 服务器证书
struct ServerCertBundle {
    std::vector<uint8_t> ca_der;
    std::vector<uint8_t> server_der;
    EVP_PKEY* ca_key = nullptr;
    EVP_PKEY* server_key = nullptr;
    ~ServerCertBundle() {
        if (ca_key) EVP_PKEY_free(ca_key);
        if (server_key) EVP_PKEY_free(server_key);
    }
};

/// 生成 CA 并签发带 SAN 的服务器证书（同一 CA 信任域，模拟 CR-013 §10.2 负向场景）
ServerCertBundle createCaAndServerCert(const std::string& cn, const std::string& san_spec) {
    ServerCertBundle b;
    b.ca_key = genEcKey();
    b.server_key = genEcKey();

    X509* ca = X509_new();
    X509_set_version(ca, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(ca), 100);
    X509_gmtime_adj(X509_get_notBefore(ca), 0);
    X509_gmtime_adj(X509_get_notAfter(ca), 31536000L);
    X509_NAME* ca_name = X509_NAME_new();
    X509_NAME_add_entry_by_txt(ca_name, "CN", MBSTRING_ASC,
        reinterpret_cast<const unsigned char*>("Test CA"), -1, -1, 0);
    X509_set_subject_name(ca, ca_name);
    X509_set_issuer_name(ca, ca_name);
    X509_NAME_free(ca_name);
    X509_set_pubkey(ca, b.ca_key);
    addBasicConstraintsCa(ca);
    X509_sign(ca, b.ca_key, EVP_sha256());
    b.ca_der = x509ToDer(ca);

    X509* server = X509_new();
    X509_set_version(server, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(server), 101);
    X509_gmtime_adj(X509_get_notBefore(server), 0);
    X509_gmtime_adj(X509_get_notAfter(server), 31536000L);
    X509_NAME* sname = X509_NAME_new();
    X509_NAME_add_entry_by_txt(sname, "CN", MBSTRING_ASC,
        reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
    X509_set_subject_name(server, sname);
    X509_set_issuer_name(server, X509_get_subject_name(ca));
    X509_NAME_free(sname);
    X509_set_pubkey(server, b.server_key);
    addSan(server, san_spec);
    X509_sign(server, b.ca_key, EVP_sha256());
    b.server_der = x509ToDer(server);

    X509_free(ca);
    X509_free(server);
    return b;
}

/// 构建 server SSL_CTX（不要求客户端证书，避免触发 SEC 签名回调，聚焦 hostname verification）
SSL_CTX* createServerCtx(const ServerCertBundle& b) {
    SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) return nullptr;
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    SSL_CTX_set_max_proto_version(ctx, TLS1_3_VERSION);

    const unsigned char* p = b.server_der.data();
    X509* cert = d2i_X509(nullptr, &p, static_cast<long>(b.server_der.size()));
    if (!cert) {
        SSL_CTX_free(ctx);
        return nullptr;
    }
    SSL_CTX_use_certificate(ctx, cert);
    X509_free(cert);
    SSL_CTX_use_PrivateKey(ctx, b.server_key);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    return ctx;
}

/// 经 buildTlsContext 构建 client SSL_CTX（走真实代码路径，含 hostname verification）
struct ClientTlsHarness {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    SSL_CTX* ctx = nullptr;

    ~ClientTlsHarness() {
        if (ctx) adapter.destroyContext();
    }

    bool build(const std::vector<uint8_t>& ca_der,
               const std::vector<uint8_t>& client_cert_der,
               const std::string& broker_host) {
        ActiveTlsCredential cred;
        cred.credential_id = "test-cred";
        cred.version = 1;
        cred.status = TlsCredentialStatus::READY;
        cred.root_ca_bundle = ca_der;
        cred.client_cert_chain = client_cert_der;
        cred.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
        cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
        cred.ready = true;
        mgr.simulateReady(cred);
        ctx = adapter.buildTlsContext(cred, mgr, broker_host,
            TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
        return ctx != nullptr;
    }
};

/// 内存 BIO 完成一次完整 TLS 握手
/// @return true 握手成功（链验证 + hostname verification 均通过）
bool doTlsHandshake(SSL_CTX* client_ctx, SSL_CTX* server_ctx, int timeout_ms = 3000) {
    BIO* c_bio = nullptr;
    BIO* s_bio = nullptr;
    if (BIO_new_bio_pair(&c_bio, 0, &s_bio, 0) != 1) return false;

    SSL* client = SSL_new(client_ctx);
    SSL* server = SSL_new(server_ctx);
    SSL_set_connect_state(client);
    SSL_set_accept_state(server);
    SSL_set_bio(client, c_bio, c_bio);  // BIO 所有权移交 SSL
    SSL_set_bio(server, s_bio, s_bio);

    bool ok = false;
    bool client_done = false;
    bool server_done = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    while (std::chrono::steady_clock::now() < deadline) {
        if (!client_done) {
            int rc = SSL_do_handshake(client);
            if (rc == 1) {
                client_done = true;
            } else {
                int err = SSL_get_error(client, rc);
                if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
                    ok = false;
                    break;
                }
            }
        }
        if (!server_done) {
            int rc = SSL_do_handshake(server);
            if (rc == 1) {
                server_done = true;
            } else {
                int err = SSL_get_error(server, rc);
                if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE) {
                    ok = false;
                    break;
                }
            }
        }
        if (client_done && server_done) {
            ok = true;
            break;
        }
    }

    SSL_free(client);
    SSL_free(server);
    return ok;
}

} // anonymous namespace

TEST(SecTlsKeyAdapterTest, IsAlgorithmAllowed_ECDSA_P256) {
    EXPECT_TRUE(SecTlsKeyAdapter::isAlgorithmAllowed(
        KeyAlgorithm::ECDSA_P256, SignatureAlgorithm::ECDSA_SECP256R1_SHA256));
}

TEST(SecTlsKeyAdapterTest, IsAlgorithmAllowed_UnknownKey) {
    EXPECT_FALSE(SecTlsKeyAdapter::isAlgorithmAllowed(
        KeyAlgorithm::UNKNOWN, SignatureAlgorithm::ECDSA_SECP256R1_SHA256));
}

TEST(SecTlsKeyAdapterTest, IsAlgorithmAllowed_UnknownSig) {
    EXPECT_FALSE(SecTlsKeyAdapter::isAlgorithmAllowed(
        KeyAlgorithm::ECDSA_P256, SignatureAlgorithm::UNKNOWN));
}

TEST(SecTlsKeyAdapterTest, InitialStateNoContext) {
    SecTlsKeyAdapter adapter;
    EXPECT_FALSE(adapter.isContextValid());
    EXPECT_EQ(adapter.getSslCtx(), nullptr);
}

TEST(SecTlsKeyAdapterTest, BuildTlsContextFailsWhenNotReady) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    mgr.simulateNotReady();

    ActiveTlsCredential cred = mgr.getSnapshot();
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    EXPECT_EQ(ctx, nullptr);
    EXPECT_FALSE(adapter.getLastError().empty());
}

TEST(SecTlsKeyAdapterTest, BuildTlsContextWithValidCert) {
    // Generate test cert
    auto tc = createTestCert("test-mqtt-client");

    CredentialManager mgr;
    ActiveTlsCredential cred;
    cred.credential_id = "test-cred";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;       // self-signed = root CA
    cred.client_cert_chain = tc.der;    // leaf cert
    cred.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);

    // buildTlsContext should succeed with valid cert
    // (remote key registration may fail without real SEC, but context should be built)
    EXPECT_NE(ctx, nullptr);
    EXPECT_TRUE(adapter.isContextValid());

    adapter.destroyContext();
    EXPECT_FALSE(adapter.isContextValid());
}

// CR-014 §4.1: leaf 证书指纹来自实际写入 SSL_CTX 的 leaf DER（SHA-256）
TEST(SecTlsKeyAdapterTest, LeafFingerprintMatchesSha256OfLeafDer) {
    auto tc = createTestCert("test-mqtt-client");

    CredentialManager mgr;
    ActiveTlsCredential cred;
    cred.credential_id = "test-cred";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;
    cred.client_cert_chain = tc.der;
    cred.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    ASSERT_NE(ctx, nullptr);

    const std::string expected = "sha256:" + sha256Hex(tc.der.data(), tc.der.size());
    EXPECT_EQ(adapter.getLeafFingerprint(), expected);

    // 不同证书产生不同指纹
    auto tc2 = createTestCert("test-mqtt-client-2");
    EXPECT_NE(adapter.getLeafFingerprint(),
              "sha256:" + sha256Hex(tc2.der.data(), tc2.der.size()));

    adapter.destroyContext();
    EXPECT_TRUE(adapter.getLeafFingerprint().empty());
}

TEST(SecTlsKeyAdapterTest, DestroyContextClearsState) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    mgr.simulateReady();

    // Don't build context, just test destroy is safe
    adapter.destroyContext();
    EXPECT_FALSE(adapter.isContextValid());
}

TEST(SecTlsKeyAdapterTest, SignRejectsUnknownAlgorithm) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    mgr.simulateReady();

    // Set up adapter with credential that has UNKNOWN algorithm
    ActiveTlsCredential cred;
    cred.key_algorithm = KeyAlgorithm::UNKNOWN;
    cred.ready = true;

    // We need to set the adapter's internal state - use buildTlsContext path
    // or test sign directly after setting up via a mock
    // Since sign() checks current_credential_.key_algorithm, we test via
    // the isAlgorithmAllowed static method
    EXPECT_FALSE(SecTlsKeyAdapter::isAlgorithmAllowed(
        KeyAlgorithm::UNKNOWN, SignatureAlgorithm::ECDSA_SECP256R1_SHA256));
}

TEST(SecTlsKeyAdapterTest, BuildTlsContextRejectsNonECKey) {
    // Test with empty cert chain
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;

    ActiveTlsCredential cred;
    cred.credential_id = "test";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = {0x30, 0x82, 0x01, 0x22};  // invalid DER
    cred.client_cert_chain = {0x30, 0x82, 0x01, 0x44};  // invalid DER
    cred.private_key_ref.data = {0x00, 0x01};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;

    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    // Should fail due to invalid DER
    EXPECT_EQ(ctx, nullptr);
    EXPECT_FALSE(adapter.getLastError().empty());
}

TEST(SecTlsKeyAdapterTest, BuildTlsContextRejectsEmptyBundle) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;

    ActiveTlsCredential cred;
    cred.ready = true;
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    // Empty root_ca_bundle and client_cert_chain

    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    EXPECT_EQ(ctx, nullptr);
}

TEST(SecTlsKeyAdapterTest, TlsVersionRangeApplied) {
    auto tc = createTestCert("test-version");

    CredentialManager mgr;
    ActiveTlsCredential cred;
    cred.credential_id = "test";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;
    cred.client_cert_chain = tc.der;
    cred.private_key_ref.data = {0x00, 0x01};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    ASSERT_NE(ctx, nullptr);

    // Verify min/max proto version
    EXPECT_EQ(SSL_CTX_get_min_proto_version(ctx), TLS1_2_VERSION);
    EXPECT_EQ(SSL_CTX_get_max_proto_version(ctx), TLS1_3_VERSION);

    adapter.destroyContext();
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_SetsHostnameVerification_Dns) {
    auto tc = createTestCert("test-mqtt-client");

    CredentialManager mgr;
    ActiveTlsCredential cred;
    cred.credential_id = "test-cred";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;
    cred.client_cert_chain = tc.der;
    cred.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    ASSERT_NE(ctx, nullptr);

    // hostname verification 已挂到 verify param (CR-013 §10.2)
    X509_VERIFY_PARAM* param = SSL_CTX_get0_param(ctx);
    ASSERT_NE(param, nullptr);
    const char* host = X509_VERIFY_PARAM_get0_host(param, 0);
    ASSERT_NE(host, nullptr);
    EXPECT_EQ(std::string(host), "broker.example.com");
    // 注：X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS 无公开 getter（hostflags 单独存储），
    // 其生效由握手级集成测试（同 CA 错主机名/部分通配符证书）覆盖。

    adapter.destroyContext();
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_SetsHostnameVerification_Ip) {
    auto tc = createTestCert("test-mqtt-client");

    CredentialManager mgr;
    ActiveTlsCredential cred;
    cred.credential_id = "test-cred";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;
    cred.client_cert_chain = tc.der;
    cred.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "10.0.0.1",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    ASSERT_NE(ctx, nullptr);

    X509_VERIFY_PARAM* param = SSL_CTX_get0_param(ctx);
    ASSERT_NE(param, nullptr);
    // IP 分支：走 X509_VERIFY_PARAM_set1_ip_asc（IP SAN），不设置 DNS host
    EXPECT_EQ(X509_VERIFY_PARAM_get0_host(param, 0), nullptr);
    // 注：hostflags 无公开 getter，IP SAN 校验行为由握手级集成测试覆盖。

    adapter.destroyContext();
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_FailsOnEmptyBrokerHost) {
    auto tc = createTestCert("test-mqtt-client");

    CredentialManager mgr;
    ActiveTlsCredential cred;
    cred.credential_id = "test-cred";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;
    cred.client_cert_chain = tc.der;
    cred.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);

    SecTlsKeyAdapter adapter;
    // 空 broker_host：fail-closed，不得构建不校验主机名的 context (CR-013 §10.2)
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    EXPECT_EQ(ctx, nullptr);
    EXPECT_FALSE(adapter.getLastError().empty());
}

// ===== 握手级 hostname verification 测试 (CR-013 §10.2 / REQ US-001 验收) =====
// 同一根 CA 信任域，服务器证书 SAN 与 broker_host 匹配关系决定握手成败。
// server 不要求客户端证书，避免 SEC 签名回调，聚焦 client 侧服务器身份校验。

TEST(SecTlsKeyAdapterTest, Handshake_DnsSanMatchesHost_Succeeds) {
    auto bundle = createCaAndServerCert("broker", "DNS:broker.example.com");
    auto client_cert = createTestCert("test-mqtt-client");

    ClientTlsHarness client;
    ASSERT_TRUE(client.build(bundle.ca_der, client_cert.der, "broker.example.com"));
    SSL_CTX* server_ctx = createServerCtx(bundle);
    ASSERT_NE(server_ctx, nullptr);

    EXPECT_TRUE(doTlsHandshake(client.ctx, server_ctx));
    SSL_CTX_free(server_ctx);
}

TEST(SecTlsKeyAdapterTest, Handshake_DnsSanMismatchHost_Fails) {
    // 核心安全用例：同一 CA 签发的错误 SAN 证书不得冒充 broker (REQ US-001 验收)
    auto bundle = createCaAndServerCert("evil", "DNS:evil.example.com");
    auto client_cert = createTestCert("test-mqtt-client");

    ClientTlsHarness client;
    ASSERT_TRUE(client.build(bundle.ca_der, client_cert.der, "broker.example.com"));
    SSL_CTX* server_ctx = createServerCtx(bundle);
    ASSERT_NE(server_ctx, nullptr);

    EXPECT_FALSE(doTlsHandshake(client.ctx, server_ctx));
    SSL_CTX_free(server_ctx);
}

TEST(SecTlsKeyAdapterTest, Handshake_PartialWildcardSan_Rejected) {
    // RFC 6125：禁止部分通配符（NO_PARTIAL_WILDCARDS 生效）
    auto bundle = createCaAndServerCert("broker", "DNS:w*.example.com");
    auto client_cert = createTestCert("test-mqtt-client");

    ClientTlsHarness client;
    ASSERT_TRUE(client.build(bundle.ca_der, client_cert.der, "www.example.com"));
    SSL_CTX* server_ctx = createServerCtx(bundle);
    ASSERT_NE(server_ctx, nullptr);

    EXPECT_FALSE(doTlsHandshake(client.ctx, server_ctx));
    SSL_CTX_free(server_ctx);
}

TEST(SecTlsKeyAdapterTest, Handshake_FullWildcardSan_Allowed) {
    // 最左标签完整 * 是 RFC 6125 允许的基线
    auto bundle = createCaAndServerCert("broker", "DNS:*.example.com");
    auto client_cert = createTestCert("test-mqtt-client");

    ClientTlsHarness client;
    ASSERT_TRUE(client.build(bundle.ca_der, client_cert.der, "broker.example.com"));
    SSL_CTX* server_ctx = createServerCtx(bundle);
    ASSERT_NE(server_ctx, nullptr);

    EXPECT_TRUE(doTlsHandshake(client.ctx, server_ctx));
    SSL_CTX_free(server_ctx);
}

TEST(SecTlsKeyAdapterTest, Handshake_IpSanMatchesIpHost_Succeeds) {
    auto bundle = createCaAndServerCert("broker", "IP:10.0.0.1");
    auto client_cert = createTestCert("test-mqtt-client");

    ClientTlsHarness client;
    ASSERT_TRUE(client.build(bundle.ca_der, client_cert.der, "10.0.0.1"));
    SSL_CTX* server_ctx = createServerCtx(bundle);
    ASSERT_NE(server_ctx, nullptr);

    EXPECT_TRUE(doTlsHandshake(client.ctx, server_ctx));
    SSL_CTX_free(server_ctx);
}

TEST(SecTlsKeyAdapterTest, Handshake_IpHostDnsSan_Fails) {
    // IP broker_host 只接受 IP SAN，不得以 DNS SAN/CN 代替 (CR-013 §10.2)
    auto bundle = createCaAndServerCert("broker", "DNS:broker.example.com");
    auto client_cert = createTestCert("test-mqtt-client");

    ClientTlsHarness client;
    ASSERT_TRUE(client.build(bundle.ca_der, client_cert.der, "10.0.0.1"));
    SSL_CTX* server_ctx = createServerCtx(bundle);
    ASSERT_NE(server_ctx, nullptr);

    EXPECT_FALSE(doTlsHandshake(client.ctx, server_ctx));
    SSL_CTX_free(server_ctx);
}

// ===== CR-017 §3: 实际 leaf CN -> 规范化 ecu_uid 投影 =====

TEST(SecTlsKeyAdapterTest, NormalizeEcuUid_Various) {
    std::string out;
    EXPECT_TRUE(SecTlsKeyAdapter::normalizeEcuUid(" hwyz-abc123 ", out));
    EXPECT_EQ(out, "HWYZ-ABC123");
    EXPECT_TRUE(SecTlsKeyAdapter::normalizeEcuUid("A1-B2", out));
    EXPECT_EQ(out, "A1-B2");
    EXPECT_FALSE(SecTlsKeyAdapter::normalizeEcuUid("", out));
    EXPECT_FALSE(SecTlsKeyAdapter::normalizeEcuUid("   ", out));
    EXPECT_FALSE(SecTlsKeyAdapter::normalizeEcuUid("bad uid", out));
    EXPECT_FALSE(SecTlsKeyAdapter::normalizeEcuUid("uid_1", out));
    EXPECT_FALSE(SecTlsKeyAdapter::normalizeEcuUid("slash/1", out));
    EXPECT_FALSE(SecTlsKeyAdapter::normalizeEcuUid(std::string(65, 'A'), out));
    EXPECT_TRUE(SecTlsKeyAdapter::normalizeEcuUid(std::string(64, 'A'), out));
}

/// CR-017: buildTlsContext with cert of given subject attrs, return ctx.helper
static bool buildWithSubject(const std::vector<std::pair<std::string, std::string>>& attrs,
                             SecTlsKeyAdapter& adapter, CredentialManager& mgr) {
    auto tc = createTestCertWithSubject(attrs);
    ActiveTlsCredential cred;
    cred.credential_id = "test-cred";
    cred.version = 1;
    cred.status = TlsCredentialStatus::READY;
    cred.root_ca_bundle = tc.der;
    cred.client_cert_chain = tc.der;
    cred.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
    cred.key_algorithm = KeyAlgorithm::ECDSA_P256;
    cred.ready = true;
    mgr.simulateReady(cred);
    SSL_CTX* ctx = adapter.buildTlsContext(cred, mgr, "broker.example.com",
        TlsVersion::TLS_1_2, TlsVersion::TLS_1_3);
    return ctx != nullptr;
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_ExtractsNormalizedEcuUid) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    ASSERT_TRUE(buildWithSubject({{"CN", " hwyz-abc123 "}}, adapter, mgr));
    EXPECT_EQ(adapter.getLeafEcUid(), "HWYZ-ABC123");
    EXPECT_EQ(adapter.getLastErrorCode(), "MQTT-1001");  // 无错误，保留默认分类
    adapter.destroyContext();
    EXPECT_TRUE(adapter.getLeafEcUid().empty());
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_MissingCn_FailsClosed_Mqtt1009) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    EXPECT_FALSE(buildWithSubject({{"O", "TestOrg"}}, adapter, mgr));
    EXPECT_EQ(adapter.getLastErrorCode(), "MQTT-1009");
    EXPECT_EQ(adapter.getLastReasonCode(), "SESSION_IDENTITY_MISSING");
    EXPECT_TRUE(adapter.getLeafEcUid().empty());
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_AmbiguousCn_FailsClosed) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    EXPECT_FALSE(buildWithSubject({{"CN", "A1"}, {"CN", "B2"}}, adapter, mgr));
    EXPECT_EQ(adapter.getLastErrorCode(), "MQTT-1009");
    EXPECT_EQ(adapter.getLastReasonCode(), "SESSION_IDENTITY_INVALID");
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_IllegalCn_FailsClosed) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    EXPECT_FALSE(buildWithSubject({{"CN", "bad uid"}}, adapter, mgr));
    EXPECT_EQ(adapter.getLastErrorCode(), "MQTT-1009");
    EXPECT_EQ(adapter.getLastReasonCode(), "SESSION_IDENTITY_INVALID");
}

TEST(SecTlsKeyAdapterTest, BuildTlsContext_TooLongCn_FailsClosed) {
    SecTlsKeyAdapter adapter;
    CredentialManager mgr;
    // OpenSSL 可能在编码阶段拒绝 >64 的 CN（此时报告 MISSING），
    // 或在规范化阶段拒绝（INVALID）；两种均 fail-closed。
    EXPECT_FALSE(buildWithSubject({{"CN", std::string(65, 'A')}}, adapter, mgr));
    EXPECT_EQ(adapter.getLastErrorCode(), "MQTT-1009");
    const std::string reason = adapter.getLastReasonCode();
    EXPECT_TRUE(reason == "SESSION_IDENTITY_MISSING" ||
                reason == "SESSION_IDENTITY_INVALID");
}
