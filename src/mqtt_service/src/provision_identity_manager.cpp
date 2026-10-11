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

    std::lock_guard<std::mutex> lock(mutex_);
    prov_client_ = std::make_shared<tbox::prov::ProvClient>(prov_socket_path_);
    prov_client_generation_++;
    prov_retry_.client_generation = prov_client_generation_;
    if (!prov_client_->connect()) {
        prov_ipc_state_ = IpcState::DISCONNECTED;
        last_error_ = "prov_connect_failed";
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

    prov_ipc_state_ = IpcState::CONNECTED;
    MqttLogAdapter::provision().info(
        "mqtt.provision.connected",
        "PROV client 已连接",
        {}
    );
    initialized_ = true;
    return true;
}

// ============================================================
// CR-016 §5: PROV IPC transport 自动恢复
// ============================================================

bool ProvisionIdentityManager::ensureConnected() {
    if (simulated_) {
        return true;
    }

    // 同一依赖最多一个 connect attempt（设计 §6/§7）
    std::lock_guard<std::mutex> recovery_lock(recovery_mutex_);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (prov_client_ && prov_client_->is_connected() &&
            prov_ipc_state_ == IpcState::CONNECTED) {
            prov_retry_.timer_armed = false;
            return true;
        }
        prov_retry_.timer_armed = true;
        prov_retry_.retry_count++;
        prov_retry_.total_attempts++;
        prov_retry_.next_backoff = computeDependencyBackoff(
            prov_retry_.retry_count > 0 ? prov_retry_.retry_count - 1 : 0,
            provision_retry_config_.initial_backoff,
            provision_retry_config_.max_backoff,
            prov_rng_);
        prov_ipc_state_ = IpcState::CONNECTING;
    }

    logDependencyEvent("mqtt_dependency_ipc_connect_attempt",
                       "prov_connect_failed", "MQTT-1009", 0);

    std::shared_ptr<tbox::prov::ProvClient> client;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (prov_client_) {
            prov_client_->disconnect();
        }
        prov_client_ = std::make_shared<tbox::prov::ProvClient>(prov_socket_path_);
        prov_client_generation_++;
        prov_retry_.client_generation = prov_client_generation_;
        client = prov_client_;
    }

    auto start = std::chrono::steady_clock::now();
    bool ok = client->connect();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (ok) {
            prov_ipc_state_ = IpcState::CONNECTED;
            prov_retry_.timer_armed = false;
        } else {
            prov_ipc_state_ = IpcState::DISCONNECTED;
            last_error_ = "prov_connect_failed";
        }
    }

    if (ok) {
        logDependencyEvent("mqtt_dependency_ipc_connected",
                           "", "", static_cast<uint64_t>(elapsed.count()));
    } else {
        logDependencyEvent("mqtt_dependency_ipc_connect_failed",
                           "prov_connect_failed", "MQTT-1009",
                           static_cast<uint64_t>(elapsed.count()));
    }
    return ok;
}

RetryState ProvisionIdentityManager::getRetryState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return prov_retry_;
}

void ProvisionIdentityManager::markDependencyReady() {
    std::lock_guard<std::mutex> lock(mutex_);
    prov_retry_.retry_count = 0;
    prov_retry_.next_backoff = Duration(0);
    prov_retry_.timer_armed = false;
    prov_retry_.cycle_id++;
    last_error_.clear();
}

void ProvisionIdentityManager::setProvisionRetryConfig(const ProvisionRetryConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    provision_retry_config_ = config;
}

uint64_t ProvisionIdentityManager::getClientGeneration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return prov_client_generation_;
}

IpcState ProvisionIdentityManager::getIpcState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return prov_ipc_state_;
}

bool ProvisionIdentityManager::isClientGenerationCurrent(uint64_t generation) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return generation == prov_client_generation_;
}

std::string ProvisionIdentityManager::getLastError() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_error_;
}

void ProvisionIdentityManager::setPreCommitHookForTest(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(mutex_);
    pre_commit_hook_for_test_ = std::move(hook);
}

void ProvisionIdentityManager::invalidateTransportForTest() {
    std::lock_guard<std::mutex> lock(mutex_);
    prov_ipc_state_ = IpcState::DISCONNECTED;
    prov_client_generation_++;
    prov_retry_.client_generation = prov_client_generation_;
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

    // CR-016 §5/§7.4: 探测 transport 后于锁外执行查询
    std::shared_ptr<tbox::prov::ProvClient> client;
    uint64_t gen = 0;
    std::function<void()> pre_commit_hook;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!prov_client_ || !prov_client_->is_connected() ||
            prov_ipc_state_ != IpcState::CONNECTED) {
            last_error_ = "prov_not_connected";
            prov_ipc_state_ = IpcState::DISCONNECTED;
            logDependencyEvent("mqtt_dependency_query_failed",
                               "prov_not_connected", "MQTT-1009", 0);
            return false;
        }
        client = prov_client_;
        gen = prov_client_generation_;
        pre_commit_hook = pre_commit_hook_for_test_;
    }

    auto start = std::chrono::steady_clock::now();
    auto binding = client->read_binding();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    // 测试钩子：模拟迟到回调（生产路径为 nullptr）
    if (pre_commit_hook) {
        pre_commit_hook();
    }

    // CR-016 §7.2: 提交结果前核对 generation，旧结果丢弃
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (gen != prov_client_generation_) {
            logDependencyEvent("mqtt_dependency_stale_callback_dropped",
                               "prov_binding_not_ready", "", 0);
            return false;
        }
    }

    // CR-016 §5: PROV 查询无独立错误码（read_binding 返回默认值）；
    // 以 client 连接状态区分业务 NOT_BOUND 与连接类传输故障。
    if (!client->is_connected()) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (gen == prov_client_generation_) {
                prov_ipc_state_ = IpcState::DISCONNECTED;
                last_error_ = "dependency_query_transport_failed";
            }
        }
        logDependencyEvent("mqtt_dependency_query_failed",
                           "dependency_query_transport_failed", "MQTT-1009",
                           static_cast<uint64_t>(elapsed.count()));
        return false;
    }

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

        {
            std::lock_guard<std::mutex> lock(mutex_);
            last_error_ = "prov_binding_not_ready";
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
        logDependencyEvent("mqtt_dependency_not_ready",
                           "prov_binding_not_ready", "MQTT-1009",
                           static_cast<uint64_t>(elapsed.count()));
        return false;
    }

    // 绑定有效：更新快照
    updateSnapshot(binding);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        last_error_.clear();
    }

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
    std::lock_guard<std::mutex> lock(mutex_);
    return prov_client_ && prov_client_->is_connected();
}

// ============================================================
// CR-016 §8: 依赖恢复事件
// ============================================================

void ProvisionIdentityManager::logDependencyEvent(const char* event,
                                                  const std::string& reason_code,
                                                  const std::string& error_code,
                                                  uint64_t elapsed_ms) {
    MqttLogAdapter::dependency().info(
        event,
        "PROV IPC 依赖恢复事件",
        {
            {"dependency", tbox::fw::log::FieldValue::makeString(
                dependencyName(Dependency::PROV))},
            {"state", tbox::fw::log::FieldValue::makeString(ipcStateName(prov_ipc_state_))},
            {"cycle_id", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(prov_retry_.cycle_id))},
            {"retry_count", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(prov_retry_.retry_count))},
            {"total_attempts", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(prov_retry_.total_attempts))},
            {"backoff_ms", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(prov_retry_.next_backoff.count()))},
            {"client_generation", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(prov_client_generation_))},
            {"reason_code", tbox::fw::log::FieldValue::makeString(reason_code)},
            {"error_code", tbox::fw::log::FieldValue::makeString(error_code)},
            {"elapsed_ms", tbox::fw::log::FieldValue::makeInt(
                static_cast<int64_t>(elapsed_ms))}
        }
    );
    // CR-016 §10: 仅关键 warn/error 路径集中落盘
    if (isCriticalDependencyEvent(event)) {
        MqttLogAdapter::flushCritical();
    }
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
