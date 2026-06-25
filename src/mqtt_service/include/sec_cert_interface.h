#pragma once

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

    // Simulate certificate ready (for testing)
    void simulateCertificateReady();

private:
    bool certificate_ready_ = false;
    std::vector<uint8_t> cert_chain_;
    void* key_handle_ = nullptr;
};

} // namespace mqtt
} // namespace tbox
