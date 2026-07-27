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
pkg_check_modules(MOSQUITTO REQUIRED libmosquitto)

# Framework library (framework-config + framework-store)
set(FRAMEWORK_DIR ${CMAKE_SOURCE_DIR}/../iov-vehicle-tbox-framework)
if(EXISTS ${FRAMEWORK_DIR}/include)
    set(FRAMEWORK_INCLUDE_DIR ${FRAMEWORK_DIR}/include)
endif()
find_library(FRAMEWORK_LIB tbox-framework PATHS ${FRAMEWORK_DIR}/build NO_DEFAULT_PATH)
if(NOT FRAMEWORK_LIB)
    message(FATAL_ERROR "Framework library not found in ${FRAMEWORK_DIR}/build")
endif()

# Find yaml-cpp (required by framework-config headers transitively)
find_package(yaml-cpp QUIET)
if(YAML_CPP_INCLUDE_DIRS)
    set(YAML_CPP_INC ${YAML_CPP_INCLUDE_DIRS})
elseif(EXISTS "/opt/homebrew/include/yaml-cpp/yaml.h")
    set(YAML_CPP_INC /opt/homebrew/include)
endif()

# Unit tests
add_executable(test_connection_state_machine
    unit/test_connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_connection_state_machine PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_libraries(test_connection_state_machine GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_sec_cert_interface
    unit/test_sec_cert_interface.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_cert_interface.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_sec_cert_interface PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_libraries(test_sec_cert_interface GTest::gtest_main ${FRAMEWORK_LIB} OpenSSL::SSL OpenSSL::Crypto pthread)

add_executable(test_mqtt_client_manager
    unit/test_mqtt_client_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_client_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_cert_interface.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_mqtt_client_manager PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_directories(test_mqtt_client_manager PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_mqtt_client_manager GTest::gtest_main ${FRAMEWORK_LIB} ${MOSQUITTO_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto pthread)

add_executable(test_offline_cache_queue
    unit/test_offline_cache_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/offline_cache_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_offline_cache_queue PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${YAML_CPP_INC}
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_libraries(test_offline_cache_queue GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_route_bridge
    unit/test_route_bridge.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/route_bridge.cpp
)
target_include_directories(test_route_bridge PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_libraries(test_route_bridge GTest::gtest_main pthread)

add_executable(test_config_manager
    unit/test_config_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/config_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)
target_include_directories(test_config_manager PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${YAML_CPP_INC}
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_libraries(test_config_manager GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_internal_bus_interface
    unit/test_internal_bus_interface.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/internal_bus_interface.cpp
)
target_include_directories(test_internal_bus_interface PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_libraries(test_internal_bus_interface GTest::gtest_main pthread)

add_executable(test_mqtt_service_integration
    unit/test_mqtt_service_integration.cpp
)
target_include_directories(test_mqtt_service_integration PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_directories(test_mqtt_service_integration PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_mqtt_service_integration mqtt_service_lib GTest::gtest_main ${FRAMEWORK_LIB} ${MOSQUITTO_LIBRARIES} pthread)

# Integration tests
add_executable(test_end_to_end
    integration/test_end_to_end.cpp
)
target_include_directories(test_end_to_end PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_directories(test_end_to_end PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_end_to_end mqtt_service_lib GTest::gtest_main ${FRAMEWORK_LIB} ${MOSQUITTO_LIBRARIES} pthread)

# Add tests
add_test(NAME ConnectionStateMachineTest COMMAND test_connection_state_machine)
add_test(NAME SecCertInterfaceTest COMMAND test_sec_cert_interface)
add_test(NAME MqttClientManagerTest COMMAND test_mqtt_client_manager)
add_test(NAME OfflineCacheQueueTest COMMAND test_offline_cache_queue)
add_test(NAME RouteBridgeTest COMMAND test_route_bridge)
add_test(NAME ConfigManagerTest COMMAND test_config_manager)
add_test(NAME InternalBusInterfaceTest COMMAND test_internal_bus_interface)
add_test(NAME MqttServiceIntegrationTest COMMAND test_mqtt_service_integration)
add_test(NAME EndToEndTest COMMAND test_end_to_end)

# ============================================================
# Log tests (CR-003 §7)
# ============================================================

# Common log test sources
set(LOG_TEST_SOURCES
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_log_adapter.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_context.cpp
)

add_executable(test_mqtt_log_adapter
    unit/test_mqtt_log_adapter.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_adapter PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
)
target_link_libraries(test_mqtt_log_adapter GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_events
    unit/test_mqtt_log_events.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_events PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
)
target_link_libraries(test_mqtt_log_events GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_context
    unit/test_mqtt_log_context.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_context PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
)
target_link_libraries(test_mqtt_log_context GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_redaction
    unit/test_mqtt_log_redaction.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_redaction PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
)
target_link_libraries(test_mqtt_log_redaction GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_security
    unit/test_mqtt_log_security.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_security PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
)
target_link_libraries(test_mqtt_log_security GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_executable(test_mqtt_log_fault
    unit/test_mqtt_log_fault.cpp
    ${LOG_TEST_SOURCES}
)
target_include_directories(test_mqtt_log_fault PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
)
target_link_libraries(test_mqtt_log_fault GTest::gtest_main ${FRAMEWORK_LIB} pthread)

add_test(NAME MqttLogAdapterTest COMMAND test_mqtt_log_adapter)
add_test(NAME MqttLogEventsTest COMMAND test_mqtt_log_events)
add_test(NAME MqttLogContextTest COMMAND test_mqtt_log_context)
add_test(NAME MqttLogRedactionTest COMMAND test_mqtt_log_redaction)
add_test(NAME MqttLogSecurityTest COMMAND test_mqtt_log_security)
add_test(NAME MqttLogFaultTest COMMAND test_mqtt_log_fault)
