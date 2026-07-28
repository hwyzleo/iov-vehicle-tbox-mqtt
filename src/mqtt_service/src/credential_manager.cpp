#include "credential_manager.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <chrono>

namespace tbox {
namespace mqtt {

namespace {

/// 判断 SEC 错误码是否为传输层错误（允许单次重试）(CR-005 §4.1, §9)
bool isTransportError(tbox::sec::ErrorCode rc) {
    return rc == tbox::sec::ErrorCode::CONNECTION_FAILED ||
           rc == tbox::sec::ErrorCode::NOT_INITIALIZED ||
           rc == tbox::sec::ErrorCode::OPERATION_IN_PROGRESS;
}

} // anonymous namespace

CredentialManager::CredentialManager(const std::string& sec_socket_path,
                                     const std::string& profile)
    : sec_socket_path_(sec_socket_path), profile_(profile) {
}

CredentialManager::~CredentialManager() {
    // RAII: subscription_ 析构自动取消订阅
}

bool CredentialManager::initialize() {
    if (simulated_) {
        return true;
    }

    sec_client_ = std::make_unique<tbox::sec::SecClient>(sec_socket_path_);
    if (!sec_client_->connect()) {
        MqttLogAdapter::tls().warn(
            "mqtt.sec.transport_failed",
            "SEC client 连接失败，云端连接保持 CERT_WAIT",
            {
                {"profile", tbox::fw::log::FieldValue::makeString(profile_)},
                {"reason_code", tbox::fw::log::FieldValue::makeString("sec_connect_failed")},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        // SEC 不可用时仍返回 true：MQTT 服务可启动内部 IPC 与离线队列，
        // 云端连接保持 CERT_WAIT (CR-005 §4.2)
        return true;
    }

    MqttLogAdapter::tls().info(
        "mqtt.sec.connected",
        "SEC client 已连接",
        {{"profile", tbox::fw::log::FieldValue::makeString(profile_)}}
    );
    return true;
}

bool CredentialManager::getTlsCredential(ActiveTlsCredential& credential) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (simulated_) {
        credential = current_credential_;
        return credential.ready;
    }

    if (!sec_client_ || !sec_client_->is_connected()) {
        last_error_ = "SEC client not connected";
        return false;
    }

    tbox::sec::TlsCredentialBundle bundle;
    auto rc = sec_client_->get_tls_credential(profile_, bundle);

    // 传输失败允许单次重试 (CR-005 §4.1)
    if (rc != tbox::sec::ErrorCode::SUCCESS && isTransportError(rc)) {
        MqttLogAdapter::tls().warn(
            "mqtt.sec.transport_failed",
            "getTlsCredential 传输失败，单次重试",
            {
                {"profile", tbox::fw::log::FieldValue::makeString(profile_)},
                {"reason_code", tbox::fw::log::FieldValue::makeString(secErrorToReasonCode(rc))}
            }
        );
        rc = sec_client_->get_tls_credential(profile_, bundle);
    }

    if (rc != tbox::sec::ErrorCode::SUCCESS) {
        last_error_ = secErrorToReasonCode(rc);
        MqttLogAdapter::tls().warn(
            secErrorToMqttEvent(rc),
            "getTlsCredential 失败",
            {
                {"profile", tbox::fw::log::FieldValue::makeString(profile_)},
                {"reason_code", tbox::fw::log::FieldValue::makeString(secErrorToReasonCode(rc))},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
    }

    // 映射 bundle -> ActiveTlsCredential
    mapBundleToCredential(bundle, credential);

    // 版本变化或 SEC 重启时递增 generation (CR-005 §7)
    if (credential.version != current_credential_.version ||
        credential.ready != current_credential_.ready) {
        generation_++;
    }

    current_credential_ = credential;

    if (credential.ready) {
        MqttLogAdapter::tls().info(
            "mqtt.certificate.ready",
            "SEC 凭据就绪",
            {
                {"profile", tbox::fw::log::FieldValue::makeString(profile_)},
                {"version", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(credential.version))},
                {"generation", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(generation_))}
            }
        );
    }

    return credential.ready;
}

bool CredentialManager::getTlsCredentialState(tbox::sec::TlsCredentialState& state) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (simulated_) {
        state.credential_id = current_credential_.credential_id;
        state.version = current_credential_.version;
        state.status = current_credential_.status;
        state.reason_code = 0;
        return true;
    }

    if (!sec_client_ || !sec_client_->is_connected()) {
        last_error_ = "SEC client not connected";
        return false;
    }

    auto rc = sec_client_->get_tls_credential_state(profile_, state);

    // 传输失败允许单次重试 (CR-005 §4.1)
    if (rc != tbox::sec::ErrorCode::SUCCESS && isTransportError(rc)) {
        rc = sec_client_->get_tls_credential_state(profile_, state);
    }

    if (rc != tbox::sec::ErrorCode::SUCCESS) {
        last_error_ = secErrorToReasonCode(rc);
        return false;
    }

    return true;
}

bool CredentialManager::signTls(const tbox::sec::TlsSignRequest& request,
                                std::vector<uint8_t>& signature) {
    // 禁止自动重放 (CR-005 §4.1, §5.2)
    // 超时或响应丢失为 unknown outcome，调用方终止本次握手

    if (simulated_) {
        // 模拟签名：返回固定签名数据
        signature = {0x30, 0x45, 0x02, 0x21, 0x00, 0xAB, 0xCD};
        return true;
    }

    if (!sec_client_ || !sec_client_->is_connected()) {
        last_error_ = "SEC client not connected";
        MqttLogAdapter::tls().error(
            "mqtt.tls.sign_failed",
            "signTls 失败：SEC client 未连接",
            {
                {"profile", tbox::fw::log::FieldValue::makeString(profile_)},
                {"reason_code", tbox::fw::log::FieldValue::makeString("sec_not_connected")},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
    }

    auto start = std::chrono::steady_clock::now();
    auto rc = sec_client_->sign_tls(request, signature);
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    if (rc != tbox::sec::ErrorCode::SUCCESS) {
        last_error_ = secErrorToReasonCode(rc);
        MqttLogAdapter::tls().error(
            secErrorToMqttEvent(rc),
            "signTls 失败",
            {
                {"profile", tbox::fw::log::FieldValue::makeString(profile_)},
                {"reason_code", tbox::fw::log::FieldValue::makeString(secErrorToReasonCode(rc))},
                {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1001")}
            }
        );
        return false;
    }

    MqttLogAdapter::tls().debug(
        "mqtt.tls.sign_completed",
        "signTls 完成",
        {
            {"profile", tbox::fw::log::FieldValue::makeString(profile_)},
            {"duration_ms", tbox::fw::log::FieldValue::makeInt(duration.count())}
        }
    );
    return true;
}

bool CredentialManager::subscribeTlsCredentialChanged(CredentialChangedCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    changed_callback_ = std::move(callback);

    if (simulated_) {
        // 模拟模式：仅保存 callback，不建立真实订阅
        return true;
    }

    if (!sec_client_ || !sec_client_->is_connected()) {
        last_error_ = "SEC client not connected";
        return false;
    }

    // 独立 IPC 订阅连接 (CR-005 §4.1, §7)
    // payload 仅用于触发，不直接替换凭据 (CR-005 §7)
    auto* cb_ptr = &changed_callback_;
    subscription_ = sec_client_->subscribe_tls_credential_changed(
        profile_,
        [cb_ptr, this](const tbox::sec::TlsCredentialChangedEvent& /*event*/) {
            // 收到变化事件：先标记 generation 失效，再通知上层重新获取凭据 (CR-005 §7)
            MqttLogAdapter::tls().info(
                "mqtt.credential.changed",
                "收到 SEC 凭据变化事件",
                {{"profile", tbox::fw::log::FieldValue::makeString(profile_)}}
            );
            // 注意：不在此处直接替换凭据，仅触发上层重新调用 getTlsCredential()
            CredentialChangedCallback cb;
            {
                std::lock_guard<std::mutex> inner_lock(mutex_);
                generation_++;  // 标记当前 generation 失效
                cb = *cb_ptr;
            }
            if (cb) cb();
        });

    if (!subscription_.isActive()) {
        MqttLogAdapter::tls().warn(
            "mqtt.sec.subscription_failed",
            "凭据变化订阅建立失败",
            {{"profile", tbox::fw::log::FieldValue::makeString(profile_)}}
        );
        return false;
    }

    MqttLogAdapter::tls().info(
        "mqtt.sec.subscribed",
        "凭据变化订阅已建立",
        {{"profile", tbox::fw::log::FieldValue::makeString(profile_)}}
    );
    return true;
}

ActiveTlsCredential CredentialManager::getSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_credential_;
}

uint64_t CredentialManager::getGeneration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return generation_;
}

bool CredentialManager::isCredentialReady() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_credential_.ready;
}

void CredentialManager::invalidateCredential() {
    std::lock_guard<std::mutex> lock(mutex_);
    generation_++;  // 标记当前 generation 失效 (CR-005 §7)
}

void CredentialManager::cleanupCredential() {
    // CR-005 §7 cleanup 顺序：
    // 禁止新握手 -> 标记 generation 失效 -> 停止/等待旧 TLS callback ->
    // 释放 SSL/SSL_CTX -> 清零 CA/cert/key_ref 缓冲 -> 获取新版本
    std::lock_guard<std::mutex> lock(mutex_);
    generation_++;
    current_credential_.root_ca_bundle.clear();
    current_credential_.root_ca_bundle.shrink_to_fit();
    current_credential_.client_cert_chain.clear();
    current_credential_.client_cert_chain.shrink_to_fit();
    current_credential_.private_key_ref.data.clear();
    current_credential_.private_key_ref.data.shrink_to_fit();
    current_credential_.ready = false;
    current_credential_.status = tbox::sec::TlsCredentialStatus::NOT_READY;
}

bool CredentialManager::isSecConnected() const {
    if (simulated_) {
        return true;
    }
    return sec_client_ && sec_client_->is_connected();
}

std::string CredentialManager::getLastError() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_error_;
}

// ============================================================
// 错误映射 (CR-005 §9)
// ============================================================

std::string CredentialManager::secErrorToMqttEvent(tbox::sec::ErrorCode sec_error) {
    switch (sec_error) {
        case tbox::sec::ErrorCode::TLS_CREDENTIAL_NOT_READY:
            return "mqtt.certificate.waiting";
        case tbox::sec::ErrorCode::TLS_CREDENTIAL_INVALID:
            return "mqtt.credential.invalid";
        case tbox::sec::ErrorCode::TLS_KEY_REF_INVALID:
            return "mqtt.credential.reference_expired";
        case tbox::sec::ErrorCode::TLS_SIGN_ALGORITHM_NOT_ALLOWED:
            return "mqtt.tls.algorithm_rejected";
        case tbox::sec::ErrorCode::TLS_HSM_SIGN_FAILED:
            return "mqtt.tls.sign_failed";
        case tbox::sec::ErrorCode::CONNECTION_FAILED:
        case tbox::sec::ErrorCode::NOT_INITIALIZED:
            return "mqtt.sec.transport_failed";
        default:
            return "mqtt.sec.error";
    }
}

std::string CredentialManager::secErrorToReasonCode(tbox::sec::ErrorCode sec_error) {
    switch (sec_error) {
        case tbox::sec::ErrorCode::TLS_CREDENTIAL_NOT_READY:
            return "SEC-1010";
        case tbox::sec::ErrorCode::TLS_CREDENTIAL_INVALID:
            return "SEC-1011";
        case tbox::sec::ErrorCode::TLS_KEY_REF_INVALID:
            return "SEC-1012";
        case tbox::sec::ErrorCode::TLS_SIGN_ALGORITHM_NOT_ALLOWED:
            return "SEC-1013";
        case tbox::sec::ErrorCode::TLS_HSM_SIGN_FAILED:
            return "SEC-1014";
        case tbox::sec::ErrorCode::CONNECTION_FAILED:
            return "FW-0301";
        case tbox::sec::ErrorCode::NOT_INITIALIZED:
            return "FW-0302";
        default:
            return "SEC-" + std::to_string(static_cast<uint32_t>(sec_error));
    }
}

// ============================================================
// 内部辅助
// ============================================================

void CredentialManager::mapBundleToCredential(const tbox::sec::TlsCredentialBundle& bundle,
                                              ActiveTlsCredential& credential) {
    credential.credential_id = bundle.credential_id;
    credential.version = bundle.version;
    credential.status = bundle.status;
    credential.root_ca_bundle = bundle.root_ca_bundle_der;
    credential.client_cert_chain = bundle.client_cert_chain_der;
    credential.private_key_ref = bundle.private_key_ref;
    credential.key_algorithm = bundle.key_algorithm;
    credential.not_before = bundle.not_before;
    credential.not_after = bundle.not_after;
    credential.ready = (bundle.status == tbox::sec::TlsCredentialStatus::READY);
}

// ============================================================
// 测试模拟
// ============================================================

void CredentialManager::simulateReady(const ActiveTlsCredential& credential) {
    std::lock_guard<std::mutex> lock(mutex_);
    simulated_ = true;
    if (credential.credential_id.empty()) {
        // 默认模拟凭据
        current_credential_.credential_id = "mqtt-test-credential";
        current_credential_.version = 1;
        current_credential_.status = tbox::sec::TlsCredentialStatus::READY;
        current_credential_.root_ca_bundle = {0x30, 0x82, 0x01, 0x22};
        current_credential_.client_cert_chain = {0x30, 0x82, 0x01, 0x44};
        current_credential_.private_key_ref.data = {0x00, 0x01, 0x02, 0x03};
        current_credential_.key_algorithm = tbox::sec::KeyAlgorithm::ECDSA_P256;
        current_credential_.not_before = 1000000000;
        current_credential_.not_after = 2000000000;
        current_credential_.ready = true;
    } else {
        current_credential_ = credential;
    }
    generation_++;
}

void CredentialManager::simulateNotReady() {
    std::lock_guard<std::mutex> lock(mutex_);
    simulated_ = true;
    current_credential_.ready = false;
    current_credential_.status = tbox::sec::TlsCredentialStatus::NOT_READY;
    generation_++;
}

void CredentialManager::simulateStateChanged() {
    CredentialChangedCallback cb;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        generation_++;
        cb = changed_callback_;
    }
    if (cb) cb();
}

} // namespace mqtt
} // namespace tbox
