cmake_minimum_required(VERSION 3.16)

# Fetch GoogleTest
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Unit tests
add_executable(unit_tests
    unit/test_connection_state_machine.cpp
    unit/test_sec_cert_interface.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/connection_state_machine.cpp
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/src/sec_cert_interface.cpp
)

target_include_directories(unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
)

target_link_libraries(unit_tests
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(unit_tests)
