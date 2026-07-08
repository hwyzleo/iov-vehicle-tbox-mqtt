#pragma once

#include "mqtt_types.h"
#include <string>
#include <vector>
#include <cstdint>

namespace tbox {
namespace mqtt {

// Certificate handle result (Design §5)
struct CertHandleResult {
    std::vector<uint8_t> cert_chain;
    void* key_handle = nullptr;  // PKCS#11 key handle
    bool valid = false;
    std::string error;
};

// SEC certificate interface (Design §5)
class SecCertInterface {
public:
    SecCertInterface();
    ~SecCertInterface();

    // Get certificate handle for mTLS (Design §5)
    CertHandleResult getCertHandle() const;

    // Check if certificate is ready
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

    // Simulate certificate ready (for testing)
    void simulateCertificateReady();

private:
    bool certificate_ready_ = false;
    std::vector<uint8_t> cert_chain_;
    void* key_handle_ = nullptr;
    std::string last_error_;
};

} // namespace mqtt
} // namespace tbox
