# TBOX-MQTT-DSN-CR-011 §13.2: 独立 installed-package consumer。
#
# 验证安装后的 TboxMqttClient package（而非 build tree / 源码）可被外部消费方
# find_package + 链接，且 tbox::mqtt_client 公开接口不泄漏 framework IPC 私有类型。
#
# 用法（HOST 开发验证）：
#   cmake -S tests/consumer -B build-consumer \
#       -DCMAKE_PREFIX_PATH=<mqtt-sdk install prefix>;<framework/prov/sec/deps prefix>
#   cmake --build build-consumer
cmake_minimum_required(VERSION 3.16)
project(tbox_mqtt_consumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(TboxMqttClient CONFIG REQUIRED)
# client.h 公开头引用 framework-ipc 的 Subscription 句柄；消费方必须链接 ipc 以解析头文件
find_package(TboxFramework CONFIG REQUIRED)

add_executable(mqtt_consumer main.cpp)
target_link_libraries(mqtt_consumer PRIVATE tbox::mqtt_client TBoxFramework::ipc)
