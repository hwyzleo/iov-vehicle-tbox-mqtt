cmake_minimum_required(VERSION 3.16)
project(tbox-mqtt-service VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find dependencies
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)

# MQTT library (mosquitto)
find_package(PkgConfig REQUIRED)
pkg_check_modules(MOSQUITTO REQUIRED libmosquitto)

# SQLite for offline cache
find_package(SQLite3 REQUIRED)

# Framework library (framework-config + framework-store)
set(FRAMEWORK_DIR ${PROJECT_SOURCE_DIR}/../iov-vehicle-tbox-framework)
if(EXISTS ${FRAMEWORK_DIR}/include)
    set(FRAMEWORK_INCLUDE_DIR ${FRAMEWORK_DIR}/include)
endif()
find_library(FRAMEWORK_LIB tbox-framework PATHS ${FRAMEWORK_DIR}/build NO_DEFAULT_PATH)
if(NOT FRAMEWORK_LIB)
    message(FATAL_ERROR "Framework library not found in ${FRAMEWORK_DIR}/build. "
                        "Please build framework first: cmake -B ${FRAMEWORK_DIR}/build && cmake --build ${FRAMEWORK_DIR}/build")
endif()

# Find yaml-cpp (required by framework-config headers transitively)
find_package(yaml-cpp QUIET)
if(YAML_CPP_INCLUDE_DIRS)
    include_directories(${YAML_CPP_INCLUDE_DIRS})
elseif(EXISTS "/opt/homebrew/include/yaml-cpp/yaml.h")
    include_directories(SYSTEM /opt/homebrew/include)
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
    ${FRAMEWORK_INCLUDE_DIR}
    ${MOSQUITTO_INCLUDE_DIRS}
)

# Source files
set(MQTT_SERVICE_SOURCES
    src/mqtt_service/src/mqtt_service.cpp
    src/mqtt_service/src/mqtt_log_adapter.cpp
    src/mqtt_service/src/mqtt_context.cpp
    src/mqtt_service/src/connection_state_machine.cpp
    src/mqtt_service/src/mqtt_client_manager.cpp
    src/mqtt_service/src/offline_cache_queue.cpp
    src/mqtt_service/src/route_bridge.cpp
    src/mqtt_service/src/config_manager.cpp
    src/mqtt_service/src/sec_cert_interface.cpp
    src/mqtt_service/src/internal_bus_interface.cpp
)

# Main library
add_library(mqtt_service_lib ${MQTT_SERVICE_SOURCES})
target_link_directories(mqtt_service_lib PUBLIC ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(mqtt_service_lib
    PUBLIC
    OpenSSL::SSL
    OpenSSL::Crypto
    ${MOSQUITTO_LIBRARIES}
    SQLite::SQLite3
    protobuf::libprotobuf
    ${FRAMEWORK_LIB}
)

# Main executable
add_executable(tbox_mqtt_service src/main.cpp)
set_target_properties(tbox_mqtt_service PROPERTIES OUTPUT_NAME tbox_mqtt)
target_link_libraries(tbox_mqtt_service mqtt_service_lib)

# Testing
enable_testing()
add_subdirectory(tests)

# Install
install(TARGETS tbox_mqtt_service DESTINATION bin)
install(TARGETS mqtt_service_lib DESTINATION lib)
install(DIRECTORY src/mqtt_service/include/ DESTINATION include)
