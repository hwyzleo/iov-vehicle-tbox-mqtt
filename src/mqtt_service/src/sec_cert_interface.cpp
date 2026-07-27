#include "sec_cert_interface.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <fstream>
#include <filesystem>
#include <openssl/x509.h>
#include <openssl/pem.h>

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

bool SecCertInterface::loadCertificates(const std::string& root_ca_ref,
                                         const std::string& tls_cert_ref,
                                         const std::string& tls_key_ref,
                                         KeySource key_source) {
    certificate_ready_ = false;
    cert_chain_.clear();
    key_handle_ = nullptr;
    last_error_.clear();

    if (root_ca_ref.empty()) {
        last_error_ = "root_ca_ref is empty";
        return false;
    }
    if (!std::filesystem::exists(root_ca_ref)) {
        last_error_ = "root_ca_ref file not found: " + root_ca_ref;
        return false;
    }

    if (tls_cert_ref.empty()) {
        last_error_ = "tls_cert_ref is empty";
        return false;
    }
    if (!std::filesystem::exists(tls_cert_ref)) {
        last_error_ = "tls_cert_ref file not found: " + tls_cert_ref;
        return false;
    }

    std::unique_ptr<BIO, decltype(&BIO_free)> cert_bio(
        BIO_new_file(tls_cert_ref.c_str(), "r"), &BIO_free);
    if (!cert_bio) {
        last_error_ = "Failed to open cert file: " + tls_cert_ref;
        return false;
    }

    std::unique_ptr<X509, decltype(&X509_free)> cert(
        PEM_read_bio_X509(cert_bio.get(), nullptr, nullptr, nullptr), &X509_free);
    if (!cert) {
        last_error_ = "Failed to parse certificate (not valid PEM X509): " + tls_cert_ref;
        return false;
    }

    std::ifstream cert_file(tls_cert_ref, std::ios::binary);
    cert_chain_ = std::vector<uint8_t>(
        (std::istreambuf_iterator<char>(cert_file)),
        std::istreambuf_iterator<char>());

    if (tls_key_ref.empty()) {
        last_error_ = "tls_key_ref is empty";
        return false;
    }

    if (key_source == KeySource::FILE) {
        if (!std::filesystem::exists(tls_key_ref)) {
            last_error_ = "tls_key_ref file not found: " + tls_key_ref;
            return false;
        }
        key_handle_ = reinterpret_cast<void*>(0x1234);
    } else {
        if (tls_key_ref.find("pkcs11:") != 0) {
            last_error_ = "tls_key_ref is not a valid PKCS#11 URI: " + tls_key_ref;
            return false;
        }
        key_handle_ = reinterpret_cast<void*>(0x5678);
    }

    certificate_ready_ = true;
    MqttLogAdapter::tls().info(
        "mqtt.tls.certificates_loaded",
        "证书加载成功",
        {
            {"key_source", tbox::fw::log::FieldValue::makeString(
                key_source == KeySource::HSM ? "HSM" : "FILE")}
        }
    );
    return true;
}

std::string SecCertInterface::getLastError() const {
    return last_error_;
}

void SecCertInterface::simulateCertificateReady() {
    certificate_ready_ = true;
    cert_chain_ = {0x30, 0x82, 0x01, 0x22};  // Example cert header
    key_handle_ = reinterpret_cast<void*>(0x1234);  // Example handle
}

} // namespace mqtt
} // namespace tbox
