#pragma once

#include "mqtt_types.h"
#include "tbox/sec/client.h"
#include <string>
#include <vector>
#include <cstdint>
#include <memory>

namespace tbox {
namespace mqtt {

// Certificate handle result (Design §5)
struct CertHandleResult {
    std::vector<uint8_t> cert_chain;
    void* key_handle = nullptr;  // PKCS#11 key handle
    bool valid = false;
    std::string error;
};

// SEC certificate interface (Design §5, CR-004 §1, §8)
//
// CR-004: MQTT 获取证书/私钥句柄只使用 tbox::sec_client。
// 证书就绪状态经 tbox::sec::SecClient::get_provision_status() 判定
// （state == CERT_INSTALLED 表示证书已安装可用）。
// 私钥不出件：本接口只持有 key_handle 引用，不持有私钥明文。
class SecCertInterface {
public:
    /// @param sec_socket_path SEC daemon IPC socket 路径（空则不查询 SEC，仅用本地标志）
    explicit SecCertInterface(const std::string& sec_socket_path = "/tmp/tbox-sec.sock");
    ~SecCertInterface();

    // Get certificate handle for mTLS (Design §5)
    CertHandleResult getCertHandle() const;

    // Check if certificate is ready (CR-004 §1: 经 tbox::sec_client 判定)
    bool isCertificateReady() const;

    // Get certificate chain
    std::vector<uint8_t> getCertificateChain() const;

    // Load and validate certificates from configured file paths
    // Returns true if all certificates are valid and loaded successfully
    bool loadCertificates(const std::string& root_ca_ref,
                          const std::string& tls_cert_ref,
                          const std::string& tls_key_ref,
                          KeySource key_source);

    // Get last error message from loadCertificates
    std::string getLastError() const;

    // Simulate certificate ready (for testing, bypasses SEC client)
    void simulateCertificateReady();

private:
    std::string sec_socket_path_;
    std::unique_ptr<tbox::sec::SecClient> sec_client_;
    bool certificate_ready_ = false;
    bool simulated_ = false;
    std::vector<uint8_t> cert_chain_;
    void* key_handle_ = nullptr;
    std::string last_error_;
};

} // namespace mqtt
} // namespace tbox
