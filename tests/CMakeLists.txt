# tests/CMakeLists.txt

# Fetch GoogleTest (cached outside build dir to survive clean builds)
include(FetchContent)
set(FETCHCONTENT_BASE_DIR "${CMAKE_CURRENT_LIST_DIR}/../.cache/deps" CACHE PATH "Dependency cache directory" FORCE)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(googletest)

# Find dependencies
find_package(PkgConfig REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json REQUIRED)
pkg_check_modules(MOSQUITTO REQUIRED libmosquitto)

# TBox Framework：统一通过 find_package 消费（自包含 yaml-cpp / spdlog）
# 查找前缀由 scripts/build.sh 从 iov-vehicle-tbox-framework/scripts/tbox-env.sh
# 取得的 TBOX_PREFIX 决定，切换本地/生产环境无需改动本文件。
if(NOT TARGET TBoxFramework::tbox-framework)
    find_package(TBoxFramework REQUIRED)
endif()
set(FRAMEWORK_LIB TBoxFramework::tbox-framework)
set(FRAMEWORK_INCLUDE_DIR ${TBoxFramework_INCLUDE_DIRS})

# SEC client SDK (CR-004)
set(SEC_DIR ${PROJECT_SOURCE_DIR}/../iov-vehicle-tbox-sec)
set(SEC_INCLUDE_DIR ${SEC_DIR}/include)
find_library(SEC_CLIENT_LIB tbox_sec_client PATHS ${SEC_DIR}/build NO_DEFAULT_PATH)

# PROV client SDK (CR-006)
set(PROV_DIR ${PROJECT_SOURCE_DIR}/../iov-vehicle-tbox-prov)
set(PROV_INCLUDE_DIR ${PROV_DIR}/include)
find_library(PROV_CLIENT_LIB tbox_prov_client PATHS ${PROV_DIR}/build NO_DEFAULT_PATH)
find_library(PROV_DAEMON_LIB TboxProvLib PATHS ${PROV_DIR}/build NO_DEFAULT_PATH)

# yaml-cpp 头文件随 TBoxFramework 一起安装在 ${FRAMEWORK_INCLUDE_DIR} 下，
# 不再探测 brew / conda 路径。

set(TEST_COMMON_INCLUDES
    ${PROJECT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${SEC_INCLUDE_DIR}
    ${PROV_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)

# ============================================================
# Unit tests
# ============================================================

add_executable(test_connection_state_machine
    unit/test_connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_connection_state_machine PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_connection_state_machine GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_credential_manager
    unit/test_credential_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/credential_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_credential_manager PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_credential_manager GTest::gtest_main ${FRAMEWORK_LIB} ${SEC_CLIENT_LIB} OpenSSL::SSL OpenSSL::Crypto pthread)

# SEC TLS Key Adapter (CR-005 §5, §10)
add_executable(test_sec_tls_key_adapter
    unit/test_sec_tls_key_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_tls_key_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/credential_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_sec_tls_key_adapter PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_sec_tls_key_adapter GTest::gtest_main ${FRAMEWORK_LIB} ${SEC_CLIENT_LIB} OpenSSL::SSL OpenSSL::Crypto pthread)

# Credential lifecycle (CR-005 §7, §10)
add_executable(test_credential_lifecycle
    unit/test_credential_lifecycle.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/credential_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_tls_key_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_credential_lifecycle PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_credential_lifecycle GTest::gtest_main ${FRAMEWORK_LIB} ${SEC_CLIENT_LIB} OpenSSL::SSL OpenSSL::Crypto pthread)

add_executable(test_mqtt_client_manager
    unit/test_mqtt_client_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_client_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/credential_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_tls_key_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_mqtt_client_manager PRIVATE ${TEST_COMMON_INCLUDES})
target_link_directories(test_mqtt_client_manager PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_mqtt_client_manager GTest::gtest_main ${FRAMEWORK_LIB} ${SEC_CLIENT_LIB} ${MOSQUITTO_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto pthread)

add_executable(test_offline_cache_queue
    unit/test_offline_cache_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/offline_cache_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_offline_cache_queue PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_offline_cache_queue GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_route_bridge
    unit/test_route_bridge.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/route_bridge.cpp
)
target_include_directories(test_route_bridge PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_route_bridge GTest::gtest_main pthread)

add_executable(test_config_manager
    unit/test_config_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/config_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_config_manager PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_config_manager GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_service_integration
    unit/test_mqtt_service_integration.cpp
)
target_include_directories(test_mqtt_service_integration PRIVATE ${TEST_COMMON_INCLUDES})
target_link_directories(test_mqtt_service_integration PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_mqtt_service_integration mqtt_service_lib GTest::gtest_main ${FRAMEWORK_LIB} ${MOSQUITTO_LIBRARIES} pthread)

# ============================================================
# CR-004 IPC tests
# ============================================================

# Protocol/golden (CR-004 §10)
add_executable(test_mqtt_ipc_protocol
    unit/test_mqtt_ipc_protocol.cpp
)
target_include_directories(test_mqtt_ipc_protocol PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_ipc_protocol GTest::gtest_main nlohmann_json::nlohmann_json pthread)

# Dispatcher (CR-004 §10)
add_executable(test_mqtt_ipc_dispatcher
    unit/test_mqtt_ipc_dispatcher.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_ipc_dispatcher.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_mqtt_ipc_dispatcher PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_ipc_dispatcher GTest::gtest_main ${FRAMEWORK_LIB} nlohmann_json::nlohmann_json pthread)

# Retry policy (CR-004 §10)
add_executable(test_mqtt_retry_policy
    unit/test_mqtt_retry_policy.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_retry_policy.cpp
)
target_include_directories(test_mqtt_retry_policy PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_retry_policy GTest::gtest_main pthread)

# Dispatch queue / backpressure (CR-004 §10)
add_executable(test_mqtt_dispatch_queue
    unit/test_mqtt_dispatch_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_dispatch_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_mqtt_dispatch_queue PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_dispatch_queue GTest::gtest_main ${FRAMEWORK_LIB} pthread)

# Client SDK (CR-004 §10): 真实 framework-ipc Server/Client 起socket
add_executable(test_mqtt_client
    unit/test_mqtt_client.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_client/src/mqtt_client.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_ipc_dispatcher.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_retry_policy.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_mqtt_client PRIVATE ${TEST_COMMON_INCLUDES} ${CMAKE_SOURCE_DIR}/src/mqtt_client/src)
target_link_libraries(test_mqtt_client GTest::gtest_main ${FRAMEWORK_LIB} nlohmann_json::nlohmann_json pthread)

# Integration tests
add_executable(test_end_to_end
    integration/test_end_to_end.cpp
)
target_include_directories(test_end_to_end PRIVATE ${TEST_COMMON_INCLUDES})
target_link_directories(test_end_to_end PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_end_to_end mqtt_service_lib GTest::gtest_main ${FRAMEWORK_LIB} ${MOSQUITTO_LIBRARIES} pthread)

# IPC backpressure (CR-004 §10)
add_executable(test_mqtt_ipc_backpressure
    integration/test_mqtt_ipc_backpressure.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_client/src/mqtt_client.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_retry_policy.cpp
)
target_include_directories(test_mqtt_ipc_backpressure PRIVATE ${TEST_COMMON_INCLUDES} ${CMAKE_SOURCE_DIR}/src/mqtt_client/src)
target_link_libraries(test_mqtt_ipc_backpressure mqtt_service_lib tbox_mqtt_client GTest::gtest_main ${FRAMEWORK_LIB} nlohmann_json::nlohmann_json pthread)

# IPC fault injection (CR-004 §10)
add_executable(test_mqtt_ipc_fault_injection
    integration/test_mqtt_ipc_fault_injection.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_client/src/mqtt_client.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_retry_policy.cpp
)
target_include_directories(test_mqtt_ipc_fault_injection PRIVATE ${TEST_COMMON_INCLUDES} ${CMAKE_SOURCE_DIR}/src/mqtt_client/src)
target_link_libraries(test_mqtt_ipc_fault_injection mqtt_service_lib tbox_mqtt_client GTest::gtest_main ${FRAMEWORK_LIB} nlohmann_json::nlohmann_json pthread)

# Add tests
add_test(NAME ConnectionStateMachineTest COMMAND test_connection_state_machine)

# CR-006: ProvisionIdentityManager tests
add_executable(test_provision_identity_manager
    unit/test_provision_identity_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/provision_identity_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_provision_identity_manager PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_provision_identity_manager GTest::gtest_main ${FRAMEWORK_LIB} ${PROV_CLIENT_LIB} ${PROV_DAEMON_LIB} pthread)

add_test(NAME ProvisionIdentityManagerTest COMMAND test_provision_identity_manager)
add_test(NAME CredentialManagerTest COMMAND test_credential_manager)
add_test(NAME SecTlsKeyAdapterTest COMMAND test_sec_tls_key_adapter)
add_test(NAME CredentialLifecycleTest COMMAND test_credential_lifecycle)
add_test(NAME MqttClientManagerTest COMMAND test_mqtt_client_manager)
add_test(NAME OfflineCacheQueueTest COMMAND test_offline_cache_queue)
add_test(NAME RouteBridgeTest COMMAND test_route_bridge)
add_test(NAME ConfigManagerTest COMMAND test_config_manager)
add_test(NAME MqttServiceIntegrationTest COMMAND test_mqtt_service_integration)

# CR-006: MqttService PROV integration tests
add_executable(test_mqtt_service_provision
    unit/test_mqtt_service_provision.cpp
)
target_include_directories(test_mqtt_service_provision PRIVATE ${TEST_COMMON_INCLUDES})
target_link_directories(test_mqtt_service_provision PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_mqtt_service_provision mqtt_service_lib GTest::gtest_main ${FRAMEWORK_LIB} ${MOSQUITTO_LIBRARIES} pthread)

add_test(NAME MqttServiceProvisionTest COMMAND test_mqtt_service_provision)
add_test(NAME EndToEndTest COMMAND test_end_to_end)
add_test(NAME MqttIpcProtocolTest COMMAND test_mqtt_ipc_protocol)
add_test(NAME MqttIpcDispatcherTest COMMAND test_mqtt_ipc_dispatcher)
add_test(NAME MqttRetryPolicyTest COMMAND test_mqtt_retry_policy)
add_test(NAME MqttDispatchQueueTest COMMAND test_mqtt_dispatch_queue)
add_test(NAME MqttClientTest COMMAND test_mqtt_client)
add_test(NAME MqttIpcBackpressureTest COMMAND test_mqtt_ipc_backpressure)
add_test(NAME MqttIpcFaultInjectionTest COMMAND test_mqtt_ipc_fault_injection)

# ============================================================
# CR-007 Subscription snapshot integration test
# ============================================================
add_executable(test_mqtt_subscription_snapshot
    integration/test_mqtt_subscription_snapshot.cpp
)
target_include_directories(test_mqtt_subscription_snapshot PRIVATE ${TEST_COMMON_INCLUDES})
target_link_directories(test_mqtt_subscription_snapshot PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_mqtt_subscription_snapshot mqtt_service_lib GTest::gtest_main ${FRAMEWORK_LIB} ${MOSQUITTO_LIBRARIES} pthread)
add_test(NAME MqttSubscriptionSnapshotTest COMMAND test_mqtt_subscription_snapshot)

# ============================================================
# CR-007 Subscription snapshot & Cloud Readiness unit tests
# ============================================================

add_executable(test_owner_snapshot_registry
    unit/test_owner_snapshot_registry.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/owner_snapshot_registry.cpp
)
target_include_directories(test_owner_snapshot_registry PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_owner_snapshot_registry GTest::gtest_main pthread)

add_executable(test_effective_subscription_table
    unit/test_effective_subscription_table.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/effective_subscription_table.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/owner_snapshot_registry.cpp
)
target_include_directories(test_effective_subscription_table PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_effective_subscription_table GTest::gtest_main pthread)

add_executable(test_startup_subscription_gate
    unit/test_startup_subscription_gate.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/startup_subscription_gate.cpp
)
target_include_directories(test_startup_subscription_gate PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_startup_subscription_gate GTest::gtest_main pthread)

add_executable(test_applied_subscription_manifest
    unit/test_applied_subscription_manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/applied_subscription_manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_applied_subscription_manifest PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_applied_subscription_manifest GTest::gtest_main ${FRAMEWORK_LIB} nlohmann_json::nlohmann_json pthread)

add_executable(test_cloud_readiness_gate
    unit/test_cloud_readiness_gate.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/cloud_readiness_gate.cpp
)
target_include_directories(test_cloud_readiness_gate PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_cloud_readiness_gate GTest::gtest_main pthread)

add_executable(test_broker_subscription_reconciler
    unit/test_broker_subscription_reconciler.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/broker_subscription_reconciler.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/applied_subscription_manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_broker_subscription_reconciler PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_broker_subscription_reconciler GTest::gtest_main ${FRAMEWORK_LIB} nlohmann_json::nlohmann_json pthread)

add_test(NAME OwnerSnapshotRegistryTest COMMAND test_owner_snapshot_registry)
add_test(NAME EffectiveSubscriptionTableTest COMMAND test_effective_subscription_table)
add_test(NAME StartupSubscriptionGateTest COMMAND test_startup_subscription_gate)
add_test(NAME AppliedSubscriptionManifestTest COMMAND test_applied_subscription_manifest)
add_test(NAME CloudReadinessGateTest COMMAND test_cloud_readiness_gate)
add_test(NAME BrokerSubscriptionReconcilerTest COMMAND test_broker_subscription_reconciler)

# ============================================================
# Log tests (CR-003 §7)
# ============================================================

set(LOG_TEST_SOURCES
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)

add_executable(test_mqtt_log_adapter
    unit/test_mqtt_log_adapter.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_adapter PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_log_adapter GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_events
    unit/test_mqtt_log_events.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_events PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_log_events GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_context
    unit/test_mqtt_log_context.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_context PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_log_context GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_redaction
    unit/test_mqtt_log_redaction.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_redaction PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_log_redaction GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_security
    unit/test_mqtt_log_security.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_security PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_log_security GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_fault
    unit/test_mqtt_log_fault.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_fault PRIVATE ${TEST_COMMON_INCLUDES})
target_link_libraries(test_mqtt_log_fault GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_test(NAME MqttLogAdapterTest COMMAND test_mqtt_log_adapter)
add_test(NAME MqttLogEventsTest COMMAND test_mqtt_log_events)
add_test(NAME MqttLogContextTest COMMAND test_mqtt_log_context)
add_test(NAME MqttLogRedactionTest COMMAND test_mqtt_log_redaction)
add_test(NAME MqttLogSecurityTest COMMAND test_mqtt_log_security)
add_test(NAME MqttLogFaultTest COMMAND test_mqtt_log_fault)
