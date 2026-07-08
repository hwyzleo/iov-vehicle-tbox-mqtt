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
find_library(FRAMEWORK_LIB hwyz PATHS ${FRAMEWORK_DIR}/build NO_DEFAULT_PATH)
if(NOT FRAMEWORK_LIB)
    message(FATAL_ERROR "Framework library not found in ${FRAMEWORK_DIR}/build")
endif()

# Unit tests
add_executable(test_connection_state_machine
    unit/test_connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/connection_state_machine.cpp
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
)
target_include_directories(test_sec_cert_interface PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_libraries(test_sec_cert_interface GTest::gtest_main pthread)

add_executable(test_mqtt_client_manager
    unit/test_mqtt_client_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_client_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_cert_interface.cpp
)
target_include_directories(test_mqtt_client_manager PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${MOSQUITTO_INCLUDE_DIRS}
)
target_link_directories(test_mqtt_client_manager PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(test_mqtt_client_manager GTest::gtest_main ${MOSQUITTO_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto pthread)

add_executable(test_offline_cache_queue
    unit/test_offline_cache_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/offline_cache_queue.cpp
)
target_include_directories(test_offline_cache_queue PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
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
)
target_include_directories(test_config_manager PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
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
