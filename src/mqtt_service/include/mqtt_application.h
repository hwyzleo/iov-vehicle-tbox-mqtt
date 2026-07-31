#pragma once

//
// TBOX-MQTT-DSN-CR-008: MqttApplication 作为组合根（Composition Root）。
// 继承 hwyz::Application，统一编排配置加载、framework-log 初始化、信号安装、
// 长驻执行与最终 flush；装配并释放 Store / ProvisionIdentityManager /
// CredentialManager / SecTlsKeyAdapter / MqttService。
//
// 归属（设计 AI 确认方案 B）：
//   MqttApplication 直接持有外部依赖与传输生命周期资源：
//     state_store_ / prov_client_ / credential_manager_ / tls_manager_ / service_
//   MqttService 内部持有 broker 运行时、离线队列、订阅子系统与 IPC 栈（保持内聚）。
//
// 生命周期不变量（CR-008 §7）：
//   初始化顺序：Store -> 外部 clients/adapters -> MqttService(注入) ->
//               initializeLocal -> startExternal
//   停机顺序：beginShutdown -> stopBrokerAndPersistInflight -> shutdown(IPC) ->
//             stopExternalCallbacks -> service_.reset -> tls/cred/prov reset ->
//             store flush+reset
//   Broker 网络线程退出前 sec_client/credential/TLS adapter 仍存活；
//   SEC credential 订阅在销毁 MqttService 前取消并 join。
//

#include "application.h"
#include "store.h"
#include "provision_identity_manager.h"
#include "credential_manager.h"
#include "sec_tls_key_adapter.h"
#include "mqtt_service.h"

#include <memory>
#include <optional>
#include <string>

namespace tbox {
namespace mqtt {

// 注：未标记 final，以便白盒单元测试通过派生访问 protected 生命周期钩子。
class MqttApplication : public hwyz::Application {
public:
    MqttApplication();
    ~MqttApplication() override;

protected:
    // ============ 服务标识 ============
    std::string getServiceName() const override;

    // ============ 生命周期 ============
    bool initialize() override;
    int execute() override;
    void cleanup() override;

private:
    enum class InitStage {
        None,
        StoreOpened,
        ClientsReady,        // prov/cred/tls 构造完成
        LocalReady,          // service 构造 + initializeLocal 完成（IPC 可用）
        BrokerRuntimeReady   // startExternal 完成（Broker 状态机运行）
    };

    // 逆序释放已完成的初始化阶段（initialize 失败时调用）
    void rollbackInitialization();

    InitStage init_stage_{InitStage::None};

    // 组合根持有的外部依赖（RAII）
    std::shared_ptr<hwyz::store::Store> state_store_;
    std::unique_ptr<ProvisionIdentityManager> prov_client_;
    std::unique_ptr<CredentialManager> credential_manager_;
    std::unique_ptr<SecTlsKeyAdapter> tls_manager_;

    // broker 运行时（内聚持有离线队列/订阅子系统/IPC 栈）
    std::unique_ptr<MqttService> service_;
};

} // namespace mqtt
} // namespace tbox
