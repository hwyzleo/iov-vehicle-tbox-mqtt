//
// TBOX-MQTT-DSN-CR-008: MqttApplication 实现。
// 组合根装配顺序：Store -> 外部 clients/adapters -> MqttService(注入) ->
//                 initializeLocal -> startExternal
// 清理顺序（§7.1）：beginShutdown -> stopBrokerAndPersistInflight -> shutdown(IPC) ->
//                   stopExternalCallbacks -> service_.reset -> tls/cred/prov reset ->
//                   store flush+reset
//

#include "mqtt_application.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "config.h"
#include "log.h"

#include <chrono>

namespace tbox {
namespace mqtt {

MqttApplication::MqttApplication() = default;

// 析构定义于 .cpp：此时 MqttService 等已完整可见，unique_ptr 析构可实例化。
MqttApplication::~MqttApplication() = default;

// ============================================================
// 服务标识
// ============================================================

std::string MqttApplication::getServiceName() const {
    return "mqtt";
}

// ============================================================
// 初始化
// ============================================================

bool MqttApplication::initialize() {
    // Application::run 已完成 Config::load("mqtt") + Logger::init + 信号安装。
    auto cfg = getConfigSnapshot();
    if (!cfg) {
        MqttLogAdapter::service().error(
            "mqtt.application.config_unavailable",
            "Config snapshot unavailable after Application::load_config");
        return false;
    }

    // ---- a. Store ----
    std::string store_root = cfg->getString("common.store.root", "/var/lib/tbox");
    state_store_ = std::make_shared<hwyz::store::Store>(
        hwyz::store::Store::open("mqtt", store_root));
    if (!state_store_->isReady()) {
        MqttLogAdapter::ipc().error(
            "mqtt.store.open_failed",
            "framework-store 打开失败",
            {{"store_root", tbox::fw::log::FieldValue::makeString(store_root)}});
        state_store_.reset();
        return false;
    }
    init_stage_ = InitStage::StoreOpened;

    // ---- b. 外部 clients/adapters（PROV/SEC/TLS）----
    // CR-006: PROV client
    std::string prov_socket_path =
        cfg->getString("prov.socket_path", "/tmp/tbox-prov.sock");
    prov_client_ = std::make_unique<ProvisionIdentityManager>(prov_socket_path);

#ifdef SIMULATE_PROV
    {
        ProvisionIdentitySnapshot sim_snapshot;
        sim_snapshot.vin = "HWYZTEST900000001";
        sim_snapshot.ecu_uid = "00000001AA00000001";
        sim_snapshot.sn = "TBOX00000001";
        sim_snapshot.state = tbox::prov::ProvisionState::BOUND;
        sim_snapshot.locked = true;
        prov_client_->simulateBinding(sim_snapshot);
    }
#endif

    // CR-005: SEC client + CredentialManager
    std::string sec_socket_path =
        cfg->getString("sec.socket_path", "/tmp/tbox-sec.sock");
    std::string credential_profile =
        cfg->getString("mqtt.tls.credential_profile", "mqtt");
    credential_manager_ =
        std::make_unique<CredentialManager>(sec_socket_path, credential_profile);

#ifdef SIMULATE_CERT
    credential_manager_->simulateReady();
#endif

    // CR-005: TLS key adapter
    tls_manager_ = std::make_unique<SecTlsKeyAdapter>();
    init_stage_ = InitStage::ClientsReady;

    // ---- c. MqttService（注入外部依赖）----
    service_ = std::make_unique<MqttService>(
        state_store_, *prov_client_, *credential_manager_, *tls_manager_);

    // ---- d. initializeLocal（本地能力：配置/队列/订阅/IPC 先可用）----
    if (!service_->initializeLocal()) {
        MqttLogAdapter::service().error(
            "mqtt.application.local_init_failed",
            "MqttService::initializeLocal failed");
        rollbackInitialization();
        return false;
    }
    init_stage_ = InitStage::LocalReady;

    // ---- e. startExternal（PROV/SEC/TLS/Broker 状态机）----
    if (!service_->startExternal()) {
        MqttLogAdapter::service().error(
            "mqtt.application.external_start_failed",
            "MqttService::startExternal failed");
        rollbackInitialization();
        return false;
    }
    init_stage_ = InitStage::BrokerRuntimeReady;

    MqttLogAdapter::service().info(
        "mqtt.application.initialized",
        "MqttApplication initialized");
    return true;
}

void MqttApplication::rollbackInitialization() {
    // 逆序释放已完成的阶段（CR-008 §4.4）
    if (init_stage_ == InitStage::BrokerRuntimeReady) {
        if (service_) {
            service_->beginShutdown();
            service_->stopBrokerAndPersistInflight();
        }
        init_stage_ = InitStage::LocalReady;
    }
    if (init_stage_ == InitStage::LocalReady) {
        if (service_) {
            service_->shutdown();
            service_->stopExternalCallbacks();
            service_.reset();
        }
        init_stage_ = InitStage::ClientsReady;
    }
    if (init_stage_ == InitStage::ClientsReady) {
        // service_ 已销毁，可安全释放外部依赖
        tls_manager_.reset();
        credential_manager_.reset();
        prov_client_.reset();
        init_stage_ = InitStage::StoreOpened;
    }
    if (init_stage_ == InitStage::StoreOpened) {
        if (state_store_) {
            state_store_->flush();
            state_store_.reset();
        }
        init_stage_ = InitStage::None;
    }
}

// ============================================================
// 长驻执行
// ============================================================

int MqttApplication::execute() {
    // CR-008 §5: mqtt.service.ready 仅表示本地服务已完成初始化，
    // 不代表 Broker CONNECTED 或 CLOUD_READY。
    MqttLogAdapter::connection().info(
        "mqtt.service.ready", "MQTT local service is ready");
    // 不维护私有 running 标志，不依赖 EINTR；统一等待 Application 退出状态。
    waitForShutdown(std::chrono::milliseconds{100});
    return 0;
}

// ============================================================
// 清理（幂等有序停机）
// ============================================================

void MqttApplication::cleanup() {
    // CR-008 §7.1 清理顺序
    // 1. Quiesce：STOPPING、退出 Cloud Ready、拒绝新请求
    if (service_) {
        service_->beginShutdown();
    }
    // 2. 停止 Broker 重连/TLS/signTls，保存未 PUBACK 消息
    if (service_) {
        service_->stopBrokerAndPersistInflight();
    }
    // 3. 停止 IPC Server + dispatch queue（join 本地连接/dispatcher 线程）
    if (service_) {
        service_->shutdown();
    }
    // 4. 取消 SEC credential 订阅并 join 回调，清理 TLS context/凭据缓冲
    if (service_) {
        service_->stopExternalCallbacks();
    }
    // 5. 销毁 MqttService（broker 运行时/订阅子系统/IPC 栈）
    service_.reset();
    // 6. 释放 TLS adapter / CredentialManager / PROV client
    //    （Broker 网络线程已退出，回调已取消并 join）
    tls_manager_.reset();
    credential_manager_.reset();
    prov_client_.reset();
    // 7. flush 并释放 Store
    if (state_store_) {
        state_store_->flush();
        state_store_.reset();
    }
    init_stage_ = InitStage::None;
    // 最终日志 flush 由 Application::run 在 cleanup 后统一完成。
}

} // namespace mqtt
} // namespace tbox
