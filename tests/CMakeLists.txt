cmake_minimum_required(VERSION 3.16)

# Fetch GoogleTest
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Find mosquitto for tests
find_package(PkgConfig REQUIRED)
pkg_check_modules(MOSQUITTO REQUIRED libmosquitto)

# Unit tests
add_executable(unit_tests
    unit/test_connection_state_machine.cpp
    unit/test_sec_cert_interface.cpp
    unit/test_mqtt_client_manager.cpp
    unit/test_offline_cache_queue.cpp
    unit/test_route_bridge.cpp
    unit/test_config_manager.cpp
    unit/test_internal_bus_interface.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_cert_interface.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/mqtt_client_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/offline_cache_queue.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/route_bridge.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/config_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/internal_bus_interface.cpp
)

target_include_directories(unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${MOSQUITTO_INCLUDE_DIRS}
)

target_link_directories(unit_tests PRIVATE ${MOSQUITTO_LIBRARY_DIRS})

target_link_libraries(unit_tests
    GTest::gtest_main
    ${MOSQUITTO_LIBRARIES}
)

include(GoogleTest)
gtest_discover_tests(unit_tests)
