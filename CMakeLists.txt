cmake_minimum_required(VERSION 3.16)
project(tbox-mqtt-service VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

# ============================================================
# Find dependencies
# ============================================================
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)
find_package(nlohmann_json REQUIRED)

# MQTT library (mosquitto)
find_package(PkgConfig REQUIRED)
pkg_check_modules(MOSQUITTO REQUIRED libmosquitto)

# yaml-cpp / spdlog 由 TBoxFramework 自包含提供，不再单独 find_package，
# 避免命中构建机上的 brew / conda 副本造成版本漂移。

# SQLite for offline cache
find_package(SQLite3 REQUIRED)

# TBox Framework：统一通过 find_package 消费（自包含 yaml-cpp / spdlog）
# 查找前缀由 scripts/build.sh 从 iov-vehicle-tbox-framework/scripts/tbox-env.sh
# 取得的 TBOX_PREFIX 决定，切换本地/生产环境无需改动本文件。
if(NOT TARGET TBoxFramework::tbox-framework)
    find_package(TBoxFramework REQUIRED)
endif()
set(FRAMEWORK_LIB TBoxFramework::tbox-framework)
set(FRAMEWORK_INCLUDE_DIR ${TBoxFramework_INCLUDE_DIRS})

# SEC client SDK (CR-004 §1: MQTT 获取证书/私钥句柄只使用 tbox::sec_client)
set(SEC_DIR ${PROJECT_SOURCE_DIR}/../iov-vehicle-tbox-sec)
if(EXISTS ${SEC_DIR}/include)
    set(SEC_INCLUDE_DIR ${SEC_DIR}/include)
else()
    message(FATAL_ERROR "SEC project not found at ${SEC_DIR}")
endif()
find_library(SEC_CLIENT_LIB tbox_sec_client PATHS ${SEC_DIR}/build NO_DEFAULT_PATH)
if(NOT SEC_CLIENT_LIB)
    message(FATAL_ERROR "SEC client library not found in ${SEC_DIR}/build. "
                        "Please build SEC first: cmake -B ${SEC_DIR}/build && cmake --build ${SEC_DIR}/build")
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${SEC_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)

# ============================================================
# Daemon library (mqtt_service_lib)
# ============================================================
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
    src/mqtt_service/src/sec_tls_key_adapter.cpp
    src/mqtt_service/src/mqtt_ipc_dispatcher.cpp
    src/mqtt_service/src/mqtt_retry_policy.cpp
    src/mqtt_service/src/mqtt_dispatch_queue.cpp
)

add_library(mqtt_service_lib ${MQTT_SERVICE_SOURCES})
target_link_directories(mqtt_service_lib PUBLIC ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(mqtt_service_lib
    PUBLIC
    OpenSSL::SSL
    OpenSSL::Crypto
    ${MOSQUITTO_LIBRARIES}
    SQLite::SQLite3
    protobuf::libprotobuf
    nlohmann_json::nlohmann_json
    ${FRAMEWORK_LIB}
    ${SEC_CLIENT_LIB}
)

# ============================================================
# Client SDK library (tbox_mqtt_client / tbox::mqtt_client, CR-004 §7.2)
# MQTT daemon 不链接 mqtt_client；双方共享 protocol contract/DTO。
# ============================================================
add_library(tbox_mqtt_client STATIC
    src/mqtt_client/src/mqtt_client.cpp
    src/mqtt_service/src/mqtt_retry_policy.cpp
)
add_library(tbox::mqtt_client ALIAS tbox_mqtt_client)

# PUBLIC: only business facade/DTO headers
target_include_directories(tbox_mqtt_client
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
# PRIVATE: framework-ipc link and internal headers
target_link_libraries(tbox_mqtt_client
    PUBLIC nlohmann_json::nlohmann_json
    PRIVATE ${FRAMEWORK_LIB}
)
target_include_directories(tbox_mqtt_client PRIVATE
    ${FRAMEWORK_INCLUDE_DIR}
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
)

# Install public SDK headers
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/tbox/mqtt/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tbox/mqtt
    FILES_MATCHING PATTERN "*.h"
)

# Install client library
install(TARGETS tbox_mqtt_client
    EXPORT TboxMqttClientTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Export CMake config
install(EXPORT TboxMqttClientTargets
    FILE TboxMqttClientTargets.cmake
    NAMESPACE tbox::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TboxMqttClient
)

# CMake config file
include(CMakePackageConfigHelpers)
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TboxMqttClientConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/TboxMqttClientConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TboxMqttClient
)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/TboxMqttClientConfig.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TboxMqttClient
)

# ============================================================
# Main executable
# ============================================================
add_executable(tbox_mqtt_service src/main.cpp)
set_target_properties(tbox_mqtt_service PROPERTIES OUTPUT_NAME tbox_mqtt)
target_link_libraries(tbox_mqtt_service mqtt_service_lib)

# ============================================================
# CLI client tool (CR-004: 测试用 IPC client)
# ============================================================
add_executable(mqtt_cli src/mqtt_cli.cpp)
target_link_libraries(mqtt_cli PRIVATE tbox_mqtt_client)

# Testing
enable_testing()
add_subdirectory(tests)

# Install daemon
install(TARGETS tbox_mqtt_service DESTINATION bin)
install(TARGETS mqtt_service_lib DESTINATION lib)
install(DIRECTORY src/mqtt_service/include/ DESTINATION include/tbox/mqtt_service)
