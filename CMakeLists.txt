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

# JSON library (for config)
find_package(nlohmann_json REQUIRED)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/include
    ${CMAKE_SOURCE_DIR}/src/mqtt_service/types
)

# Source files
set(MQTT_SERVICE_SOURCES
    src/mqtt_service/src/mqtt_service.cpp
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
target_link_libraries(mqtt_service_lib
    OpenSSL::SSL
    OpenSSL::Crypto
    ${MOSQUITTO_LIBRARIES}
    SQLite::SQLite3
    nlohmann_json::nlohmann_json
    protobuf::libprotobuf
)

# Main executable
add_executable(tbox_mqtt_service src/main.cpp)
target_link_directories(tbox_mqtt_service PRIVATE ${MOSQUITTO_LIBRARY_DIRS})
target_link_libraries(tbox_mqtt_service mqtt_service_lib ${MOSQUITTO_LIBRARIES})

# Testing
enable_testing()
add_subdirectory(tests)

# Install
install(TARGETS tbox_mqtt_service DESTINATION bin)
install(TARGETS mqtt_service_lib DESTINATION lib)
install(DIRECTORY src/mqtt_service/include/ DESTINATION include)
