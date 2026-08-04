# TBox MQTT - 车端统一通信代理服务
#
# TBOX-MQTT-DSN-CR-011: 接入 TBOX-BUILD 统一 Orin aarch64 构建。
#   - 不设置 CMAKE_C_COMPILER / CMAKE_CXX_COMPILER / CMAKE_SYSROOT（由 BUILD
#     toolchain/preset 注入）
#   - framework / PROV / SEC 均通过已安装 package 消费（find_package CONFIG）
#   - GNUInstallDirs + 两个显式 install component: mqtt-sdk / mqtt-runtime
#   - 无硬编码 RPATH / 宿主路径 / build-tree library

cmake_minimum_required(VERSION 3.16)
project(tbox_mqtt VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# ===========================================================================
# 依赖（BUILD: 受控 staging/sysroot 注入；HOST: tbox-env.sh + CMAKE_PREFIX_PATH）
# ===========================================================================

# --- MQTT client library（libmosquitto；BUILD: TBOX_DEP_STAGING；HOST: 系统） ---
find_package(PkgConfig REQUIRED)
pkg_check_modules(MOSQUITTO REQUIRED IMPORTED_TARGET libmosquitto)

# --- OpenSSL（Orin sysroot 自带 1.1.1；HOST: 系统） ---
find_package(OpenSSL REQUIRED)

# --- nlohmann_json（daemon / client SDK 内部使用；BUILD: TBOX_DEP_STAGING） ---
find_package(nlohmann_json REQUIRED)

# --- TBox Framework（自包含 yaml-cpp / spdlog；CR-011: 组件 target 消费） ---
if(NOT TARGET TBoxFramework::config)
    find_package(TboxFramework CONFIG REQUIRED)
endif()
set(FRAMEWORK_LIBS
    TBoxFramework::config
    TBoxFramework::log
    TBoxFramework::ipc
    TBoxFramework::store
    TBoxFramework::application
)

# --- PROV client SDK（CR-006/CR-011: 只通过 tbox::prov_client） ---
if(NOT TARGET tbox::prov_client)
    find_package(TboxProvClient CONFIG REQUIRED)
endif()

# --- SEC client SDK（CR-005/CR-011: 只通过 tbox::sec_client） ---
if(NOT TARGET tbox::sec_client)
    find_package(TboxSecClient CONFIG REQUIRED)
endif()

# ===========================================================================
# Daemon library (mqtt_service_lib) — 内部库，不安装
# ===========================================================================
set(MQTT_SERVICE_SOURCES
    src/mqtt_service/src/mqtt_service.cpp
    src/mqtt_service/src/mqtt_log_adapter.cpp
    src/mqtt_service/src/mqtt_context.cpp
    src/mqtt_service/src/connection_state_machine.cpp
    src/mqtt_service/src/mqtt_client_manager.cpp
    src/mqtt_service/src/offline_cache_queue.cpp
    src/mqtt_service/src/route_bridge.cpp
    src/mqtt_service/src/config_manager.cpp
    src/mqtt_service/src/credential_manager.cpp
    src/mqtt_service/src/provision_identity_manager.cpp
    src/mqtt_service/src/sec_tls_key_adapter.cpp
    src/mqtt_service/src/mqtt_ipc_dispatcher.cpp
    src/mqtt_service/src/mqtt_retry_policy.cpp
    src/mqtt_service/src/mqtt_dispatch_queue.cpp
    # CR-007 §2 订阅快照与 Cloud Readiness 组件
    src/mqtt_service/src/owner_snapshot_registry.cpp
    src/mqtt_service/src/effective_subscription_table.cpp
    src/mqtt_service/src/startup_subscription_gate.cpp
    src/mqtt_service/src/applied_subscription_manifest.cpp
    src/mqtt_service/src/broker_subscription_reconciler.cpp
    src/mqtt_service/src/cloud_readiness_gate.cpp
    # CR-008: Application 组合根
    src/mqtt_service/src/mqtt_application.cpp
    # CR-009: Topic 模板校验/解析
    src/mqtt_service/src/topic_template_validator.cpp
    src/mqtt_service/src/topic_template_resolver.cpp
    # CR-009: 路由投影
    src/mqtt_service/src/effective_route_table.cpp
    src/mqtt_service/src/topic_projection_builder.cpp
    # CR-009: route-based 发布
    src/mqtt_service/src/route_publisher.cpp
    # CR-009: 下行路由分发
    src/mqtt_service/src/downlink_route_dispatcher.cpp
    # CR-009: Legacy 兼容守卫
    src/mqtt_service/src/legacy_topic_guard.cpp
)

add_library(mqtt_service_lib ${MQTT_SERVICE_SOURCES})
target_include_directories(mqtt_service_lib PUBLIC
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src/mqtt_service/include
    ${PROJECT_SOURCE_DIR}/src/mqtt_service/types
)
# 框架组件 PUBLIC（内部库不导出；测试/daemon 经传递依赖获得头与符号）。
# SEC/PROV client 同 PUBlic：daemon 与测试都需要其符号与类型头。
target_link_libraries(mqtt_service_lib
    PUBLIC
        ${FRAMEWORK_LIBS}
        tbox::prov_client
        tbox::sec_client
    PRIVATE
        PkgConfig::MOSQUITTO
        OpenSSL::SSL
        OpenSSL::Crypto
        nlohmann_json::nlohmann_json
)

# ===========================================================================
# Client SDK library (tbox_mqtt_client / tbox::mqtt_client, CR-004 §7.2)
# MQTT daemon 不链接 mqtt_client；双方共享 protocol contract/DTO。
# CR-011 §6.3: 公开接口不泄漏 framework-ipc/Broker/OpenSSL/PROV/SEC 私有类型。
# ===========================================================================
add_library(tbox_mqtt_client STATIC
    src/mqtt_client/src/mqtt_client.cpp
    src/mqtt_service/src/mqtt_retry_policy.cpp
)
add_library(tbox::mqtt_client ALIAS tbox_mqtt_client)
set_target_properties(tbox_mqtt_client PROPERTIES EXPORT_NAME mqtt_client)

# PUBLIC: only business facade/DTO headers
target_include_directories(tbox_mqtt_client
    PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
# PRIVATE: framework-ipc（client.h 公开头引用其 Subscription 句柄，消费方经
# TboxMqttClientConfig find_dependency(TboxFramework) 解析）与内部实现头
target_link_libraries(tbox_mqtt_client
    PRIVATE
        TBoxFramework::ipc
        nlohmann_json::nlohmann_json
)
target_include_directories(tbox_mqtt_client PRIVATE
    ${PROJECT_SOURCE_DIR}/src/mqtt_service/include
    ${PROJECT_SOURCE_DIR}/src/mqtt_client/src
)

# ===========================================================================
# Main executable (daemon)
# ===========================================================================
# CR-008 §9.1: 构建期二选一。ON 编译 MqttApplication 最小入口；
# OFF 编译旧式手写生命周期（回滚验证）。同一二进制只能存在一套生命周期。
option(MQTT_USE_FRAMEWORK_APPLICATION "Use hwyz::Application lifecycle" ON)
if(MQTT_USE_FRAMEWORK_APPLICATION)
    add_executable(tbox_mqtt_service src/main.cpp)
else()
    add_executable(tbox_mqtt_service src/main_legacy.cpp)
endif()
set_target_properties(tbox_mqtt_service PROPERTIES OUTPUT_NAME tbox_mqtt)
target_link_libraries(tbox_mqtt_service PRIVATE mqtt_service_lib)

# ===========================================================================
# CLI client tool (CR-004: HOST 测试用 IPC client；不安装)
# ===========================================================================
add_executable(mqtt_cli src/mqtt_cli.cpp)
# client.h 公开头引用 framework-ipc 的 Subscription 句柄，编译期需要其头目录
target_link_libraries(mqtt_cli PRIVATE tbox_mqtt_client TBoxFramework::ipc)

# ===========================================================================
# Tests (HOST only; BUILD 目标构建仅构建 declared targets，不编译测试)
# ===========================================================================
# 测试（HOST/隔离 test profile 使用，不进入量产 package；BUILD Orin 构建默认关闭，
# 避免 FetchContent googletest 的跨机器 .cache 缓存冲突）
option(BUILD_TESTS "Build tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ===========================================================================
# Install rules — 两个显式 component (CR-011 §9)
#
#   mqtt-sdk     -> staging: sdk      (公开头文件 / 静态库 / CMake package)
#   mqtt-runtime -> staging: rootfs   (daemon / systemd unit / 默认非秘密配置)
# ===========================================================================

# --- mqtt-runtime: daemon binary ---
install(TARGETS tbox_mqtt_service
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    COMPONENT mqtt-runtime
)

# --- mqtt-runtime: systemd unit ---
install(FILES packaging/systemd/tbox-mqtt.service
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system
    COMPONENT mqtt-runtime
)

# --- mqtt-runtime: default config（生产安全：无生产 Broker 地址/凭据/身份实例值） ---
install(FILES config/mqtt.default.yaml
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/tbox/conf.d
    RENAME mqtt.yaml
    COMPONENT mqtt-runtime
)

# --- mqtt-sdk: public headers ---
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/tbox/mqtt/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tbox/mqtt
    COMPONENT mqtt-sdk
    FILES_MATCHING PATTERN "*.h"
)

# --- mqtt-sdk: client static library + export ---
install(TARGETS tbox_mqtt_client
    EXPORT TboxMqttClientTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    COMPONENT mqtt-sdk
)

install(EXPORT TboxMqttClientTargets
    FILE TboxMqttClientTargets.cmake
    NAMESPACE tbox::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TboxMqttClient
    COMPONENT mqtt-sdk
)

# --- mqtt-sdk: CMake package config（可重定位）+ version ---
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TboxMqttClientConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/TboxMqttClientConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TboxMqttClient
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/TboxMqttClientConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/TboxMqttClientConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/TboxMqttClientConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TboxMqttClient
    COMPONENT mqtt-sdk
)
