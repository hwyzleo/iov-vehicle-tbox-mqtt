//
// TBOX-MQTT-DSN-CR-011 §13.2: installed-package consumer。
// 仅做编译期/链接期验证：构造 Client、检查 facade API 可解析、
// 验证错误码与 DTO 类型来自安装后的公共头文件（tbox/mqtt/...）。
// 不连接真实 daemon、不执行任何 MQTT 操作。
//

#include <tbox/mqtt/client.h>
#include <tbox/mqtt/errors.h>
#include <tbox/mqtt/types.h>

#include <cstdio>
#include <string>
#include <vector>

int main() {
    // 构造（默认 socket 路径）——验证公开构造可用
    tbox::mqtt::Client client("/tmp/tbox-mqtt-consumer.sock");

    // 公开类型可解析（ErrorCode 为强类型枚举，无 OK 哨兵；此处仅验证可引用）
    tbox::mqtt::ErrorCode ec = tbox::mqtt::ErrorCode::MQTT_1001;
    std::string ec_str = tbox::mqtt::errorCodeToString(ec);
    if (ec_str.empty()) {
        return 1;
    }

    // DTO 类型可实例化（QoS / Priority / ConnectionState）
    tbox::mqtt::QoS qos = tbox::mqtt::QoS::AT_LEAST_ONCE;
    tbox::mqtt::Priority pri = tbox::mqtt::Priority::NORMAL;
    (void)qos;
    (void)pri;

    // facade 方法签名可解析（仅编译期；不连接 daemon）
    client.disconnect();

    // publishRoute 全参数签名可解析（CR-009 §6.1）
    tbox::mqtt::PublishResult r =
        client.publishRoute("tsp", "up/fota", "msg-1", std::vector<std::uint8_t>{0x01},
                            tbox::mqtt::QoS::AT_LEAST_ONCE, tbox::mqtt::Priority::NORMAL);
    (void)r;

    std::printf("OK: tbox::mqtt_client installed-package consumer compiles/links\n");
    return 0;
}
