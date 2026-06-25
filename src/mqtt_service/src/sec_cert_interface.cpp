#include "sec_cert_interface.h"

namespace tbox {
namespace mqtt {

SecCertInterface::SecCertInterface() = default;

SecCertInterface::~SecCertInterface() = default;

CertHandleResult SecCertInterface::getCertHandle() const {
    CertHandleResult result;

    if (!certificate_ready_) {
        result.valid = false;
        result.error = "Certificate not ready";
        return result;
    }

    result.cert_chain = cert_chain_;
    result.key_handle = key_handle_;
    result.valid = true;
    return result;
}

bool SecCertInterface::isCertificateReady() const {
    return certificate_ready_;
}

std::vector<uint8_t> SecCertInterface::getCertificateChain() const {
    return cert_chain_;
}

void SecCertInterface::simulateCertificateReady() {
    certificate_ready_ = true;
    cert_chain_ = {0x30, 0x82, 0x01, 0x22};  // Example cert header
    key_handle_ = reinterpret_cast<void*>(0x1234);  // Example handle
}

} // namespace mqtt
} // namespace tbox
