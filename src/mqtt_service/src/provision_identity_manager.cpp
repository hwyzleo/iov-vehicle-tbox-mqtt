#include "provision_identity_manager.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include <algorithm>

namespace tbox {
namespace mqtt {

namespace {

/// Replace all occurrences of placeholder in a string (CR-006 §6.1, §6.2)
std::string replaceAll(std::string str, const std::string& placeholder,
                       const std::string& value) {
    if (placeholder.empty()) return str;
    size_t pos = 0;
    while ((pos = str.find(placeholder, pos)) != std::string::npos) {
        str.replace(pos, placeholder.length(), value);
        pos += value.length();
    }
    return str;
}

/// Convert ProvisionState to readable string for logging
std::string provisionStateToString(tbox::prov::ProvisionState state) {
    switch (state) {
        case tbox::prov::ProvisionState::NONE:        return "NONE";
        case tbox::prov::ProvisionState::VIN_WRITTEN: return "VIN_WRITTEN";
        case tbox::prov::ProvisionState::BOUND:       return "BOUND";
        case tbox::prov::ProvisionState::FAILED:      return "FAILED";
        default: return "UNKNOWN";
    }
}

} // anonymous namespace

ProvisionIdentityManager::ProvisionIdentityManager(
    const std::string& prov_socket_path)
    : prov_socket_path_(prov_socket_path) {
}

ProvisionIdentityManager::~ProvisionIdentityManager() {
    if (prov_client_) {
        prov_client_->disconnect();
    }
}

bool ProvisionIdentityManager::initialize() {
    if (simulated_) {
        return true;
    }

    prov_client_ = std::make_unique<tbox::prov::ProvClient>(prov_socket_path_);
    if (!prov_client_->connect()) {
        MqttLogAdapter::provision().warn(
            "mqtt.provision.waiting",
            "PROV client 连接失败，云端连接保持 PROV_WAIT",
            {
                {"binding_state", tbox::fw::log::FieldValue::makeString("NONE")},
                {"reason_code", tbox::fw::log::FieldValue::makeString("prov_connect_failed")},
                {"retry_count", tbox::fw::log::FieldValue::makeInt(0)},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}
            }
        );
        // PROV 不可用时仍返回 true：MQTT 服务可启动内部 IPC 与离线队列，
        // 云端连接保持 PROV_WAIT (CR-006 §4)
        initialized_ = true;
        return true;
    }

    MqttLogAdapter::provision().info(
        "mqtt.provision.connected",
        "PROV client 已连接",
        {}
    );
    initialized_ = true;
    return true;
}

bool ProvisionIdentityManager::readBinding() {
    if (simulated_) {
        std::lock_guard<std::mutex> lock(mutex_);
        // 模拟模式下直接返回当前快照有效性
        return current_snapshot_.state == tbox::prov::ProvisionState::BOUND &&
               !current_snapshot_.vin.empty() &&
               !current_snapshot_.ecu_uid.empty() &&
               !current_snapshot_.sn.empty();
    }

    if (!prov_client_ || !prov_client_->is_connected()) {
        MqttLogAdapter::provision().warn(
            "mqtt.provision.waiting",
            "PROV client 未连接，无法读取绑定",
            {
                {"binding_state", tbox::fw::log::FieldValue::makeString("NONE")},
                {"reason_code", tbox::fw::log::FieldValue::makeString("prov_not_connected")},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}
            }
        );
        return false;
    }

    auto binding = prov_client_->read_binding();

    if (!validate(binding)) {
        // CR-006 §5.2: 校验失败，更新快照（可能 state 变化）并记录
        std::string reason;
        if (binding.state != tbox::prov::ProvisionState::BOUND) {
            reason = "not_bound";
        } else if (binding.vin.empty()) {
            reason = "vin_empty";
        } else if (binding.sn.empty()) {
            reason = "sn_empty";
        } else if (binding.ecu_uid.empty()) {
            reason = "ecu_uid_empty";
        } else {
            reason = "validation_failed";
        }

        updateSnapshot(binding);

        ProvisionIdentitySnapshot snap;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            snap = current_snapshot_;
        }

        MqttLogAdapter::provision().warn(
            "mqtt.provision.waiting",
            "PROV 绑定无效或未完成，保持 PROV_WAIT",
            {
                {"binding_state", tbox::fw::log::FieldValue::makeString(
                    provisionStateToString(snap.state))},
                {"vin_hash", tbox::fw::log::FieldValue::makeString(vinHash(snap.vin))},
                {"sn_hash", tbox::fw::log::FieldValue::makeString(snHash(snap.sn))},
                {"ecu_uid_hash", tbox::fw::log::FieldValue::makeString(ecuUidHash(snap.ecu_uid))},
                {"generation", tbox::fw::log::FieldValue::makeInt(
                    static_cast<int64_t>(snap.generation))},
                {"reason_code", tbox::fw::log::FieldValue::makeString(reason)},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}
            }
        );
        return false;
    }

    // 绑定有效：更新快照
    updateSnapshot(binding);

    ProvisionIdentitySnapshot snap;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        snap = current_snapshot_;
    }

    MqttLogAdapter::provision().info(
        "mqtt.provision.ready",
        "PROV 绑定有效，身份就绪",
        {
            {"binding_state", tbox::fw::log::FieldValue::makeString(
                provisionStateToString(snap.state))},
            {"client_id_hash", tbox::fw::log::FieldValue::makeString(clientIdHash(snap.client_id))},
            {"vin_hash", tbox::fw::log::FieldValue::makeString(vinHash(snap.vin))},
            {"sn_hash", tbox::fw::log::FieldValue::makeString(snHash(snap.sn))},
            {"ecu_uid_hash", tbox::fw::log::FieldValue::makeString(ecuUidHash(snap.ecu_uid))},
            {"generation", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(snap.generation))}
        }
    );

    return true;
}

ProvisionIdentitySnapshot ProvisionIdentityManager::getSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_snapshot_;
}

uint64_t ProvisionIdentityManager::getGeneration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_snapshot_.generation;
}

bool ProvisionIdentityManager::isIdentityValid() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_snapshot_.state == tbox::prov::ProvisionState::BOUND &&
           !current_snapshot_.vin.empty() &&
           !current_snapshot_.ecu_uid.empty() &&
           !current_snapshot_.sn.empty() &&
           !current_snapshot_.client_id.empty();
}

bool ProvisionIdentityManager::isBound() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_snapshot_.state == tbox::prov::ProvisionState::BOUND;
}

bool ProvisionIdentityManager::hasIdentityChanged(uint64_t since_generation) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_snapshot_.generation != since_generation;
}

std::string ProvisionIdentityManager::getEcuUid() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_snapshot_.ecu_uid;
}

std::string ProvisionIdentityManager::buildClientId(std::string_view vin,
                                                    std::string_view sn) const {
    // CR-006 §6.1: VIN 与 SN 的确定性组合
    // 禁止静默截断、加入 ecu_uid 或回退旧 clientId
    if (vin.empty() || sn.empty()) {
        return "";
    }

    std::string format;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        format = client_id_format_;
    }

    // Replace {vin} and {sn} placeholders with actual values
    std::string result = replaceAll(format, "{vin}", std::string(vin));
    result = replaceAll(result, "{sn}", std::string(sn));

    // CR-006 §6.1: 不得引用 ecu_uid
    // If the format still contains unresolved placeholders, it's invalid
    if (result.find('{') != std::string::npos || result.find('}') != std::string::npos) {
        MqttLogAdapter::provision().error(
            "mqtt.provision.identity_invalid",
            "client_id_format 包含无法解析的占位符",
            {
                {"reason_code", tbox::fw::log::FieldValue::makeString("unresolved_placeholder")},
                {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1009")}
            }
        );
        return "";
    }

    return result;
}

std::string ProvisionIdentityManager::resolveTopic(const std::string& topic_template) const {
    // CR-006 §6.2: 用当前 ecu_uid 注入 Topic 模板中的 {ecu_uid} 占位
    // ecu_uid 取自同一次 readBinding() 快照
    std::string ecu_uid;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ecu_uid = current_snapshot_.ecu_uid;
    }

    if (ecu_uid.empty()) {
        return "";
    }

    return replaceAll(topic_template, "{ecu_uid}", ecu_uid);
}

void ProvisionIdentityManager::setClientIdFormat(const std::string& format) {
    std::lock_guard<std::mutex> lock(mutex_);
    client_id_format_ = format;
}

bool ProvisionIdentityManager::isProvConnected() const {
    if (simulated_) {
        return true;
    }
    return prov_client_ && prov_client_->is_connected();
}

// ============================================================
// 内部辅助
// ============================================================

bool ProvisionIdentityManager::validate(
    const tbox::prov::VehicleBinding& binding) const {
    // CR-006 §5.2: 至少检查
    // - state == BOUND
    // - VIN 非空
    // - SN 非空
    // - ECU UID 非空
    // ecu_uid 与 SEC credential/HSM 身份一致性由 MqttService 校验 (CR-006 §5.2)
    if (binding.state != tbox::prov::ProvisionState::BOUND) {
        return false;
    }
    if (binding.vin.empty()) {
        return false;
    }
    if (binding.sn.empty()) {
        return false;
    }
    if (binding.ecu_uid.empty()) {
        return false;
    }
    return true;
}

void ProvisionIdentityManager::updateSnapshot(
    const tbox::prov::VehicleBinding& binding) {
    std::lock_guard<std::mutex> lock(mutex_);

    // CR-006 §3, §7: generation 在 VIN、SN、ECU UID、state 任一变化时递增
    bool identity_changed =
        current_snapshot_.vin != binding.vin ||
        current_snapshot_.ecu_uid != binding.ecu_uid ||
        current_snapshot_.sn != binding.sn ||
        current_snapshot_.state != binding.state;

    if (identity_changed) {
        current_snapshot_.generation++;

        // Log identity change (CR-006 §8)
        // Only log if we had a previous valid identity
        if (!current_snapshot_.vin.empty() || !current_snapshot_.ecu_uid.empty()) {
            // Build client_id inline (mutex already held)
            std::string new_client_id;
            if (!binding.vin.empty() && !binding.sn.empty()) {
                std::string result = replaceAll(client_id_format_, "{vin}", binding.vin);
                result = replaceAll(result, "{sn}", binding.sn);
                if (result.find('{') == std::string::npos &&
                    result.find('}') == std::string::npos) {
                    new_client_id = result;
                }
            }
            MqttLogAdapter::provision().warn(
                "mqtt.provision.identity_changed",
                "PROV 绑定身份发生变化",
                {
                    {"binding_state", tbox::fw::log::FieldValue::makeString(
                        provisionStateToString(binding.state))},
                    {"client_id_hash", tbox::fw::log::FieldValue::makeString(clientIdHash(new_client_id))},
                    {"vin_hash", tbox::fw::log::FieldValue::makeString(vinHash(binding.vin))},
                    {"sn_hash", tbox::fw::log::FieldValue::makeString(snHash(binding.sn))},
                    {"ecu_uid_hash", tbox::fw::log::FieldValue::makeString(ecuUidHash(binding.ecu_uid))},
                    {"generation", tbox::fw::log::FieldValue::makeInt(
                        static_cast<int64_t>(current_snapshot_.generation))}
                }
            );
        }
    }

    current_snapshot_.vin = binding.vin;
    current_snapshot_.ecu_uid = binding.ecu_uid;
    current_snapshot_.sn = binding.sn;
    current_snapshot_.state = binding.state;
    current_snapshot_.locked = binding.locked;

    // Build client_id inline (mutex already held, avoid re-entrant lock)
    if (!binding.vin.empty() && !binding.sn.empty()) {
        std::string result = replaceAll(client_id_format_, "{vin}", binding.vin);
        result = replaceAll(result, "{sn}", binding.sn);
        if (result.find('{') == std::string::npos &&
            result.find('}') == std::string::npos) {
            current_snapshot_.client_id = result;
        } else {
            current_snapshot_.client_id = "";
        }
    } else {
        current_snapshot_.client_id = "";
    }
}

// ============================================================
// 测试模拟
// ============================================================

void ProvisionIdentityManager::simulateBinding(
    const ProvisionIdentitySnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    simulated_ = true;

    // CR-006 §3, §7: generation 在 VIN、SN、ECU UID、state 任一变化时递增
    bool identity_changed =
        current_snapshot_.vin != snapshot.vin ||
        current_snapshot_.ecu_uid != snapshot.ecu_uid ||
        current_snapshot_.sn != snapshot.sn ||
        current_snapshot_.state != snapshot.state;

    uint64_t new_generation = current_snapshot_.generation;
    if (identity_changed) {
        new_generation++;
    }

    current_snapshot_ = snapshot;
    current_snapshot_.generation = new_generation;

    // Build client_id inline (mutex already held, avoid re-entrant lock)
    if (!snapshot.vin.empty() && !snapshot.sn.empty()) {
        std::string result = replaceAll(client_id_format_, "{vin}", snapshot.vin);
        result = replaceAll(result, "{sn}", snapshot.sn);
        if (result.find('{') == std::string::npos &&
            result.find('}') == std::string::npos) {
            current_snapshot_.client_id = result;
        } else {
            current_snapshot_.client_id = "";
        }
    }
}

void ProvisionIdentityManager::simulateUnbound() {
    std::lock_guard<std::mutex> lock(mutex_);
    simulated_ = true;
    current_snapshot_.state = tbox::prov::ProvisionState::NONE;
    current_snapshot_.vin.clear();
    current_snapshot_.ecu_uid.clear();
    current_snapshot_.sn.clear();
    current_snapshot_.client_id.clear();
    current_snapshot_.locked = false;
    current_snapshot_.generation++;
}

} // namespace mqtt
} // namespace tbox
