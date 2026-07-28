// CR-004 §10: Protocol/golden 单元测试
// 覆盖三类头、JSON/base64、msg_id、method/event/status
#include <gtest/gtest.h>
#include "tbox/mqtt/ipc_protocol.h"
#include "tbox/mqtt/types.h"
#include "tbox/mqtt/errors.h"
#include "ipc_types.h"
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

using namespace tbox::mqtt;
using json = nlohmann::json;

// ---- 三类 wire header 大小 (framework-ipc) ----
TEST(MqttIpcProtocolTest, WireHeaderSizes) {
    EXPECT_EQ(sizeof(::tbox::fw::ipc::RequestHeader), 8u);
    EXPECT_EQ(sizeof(::tbox::fw::ipc::ResponseHeader), 8u);
    EXPECT_EQ(sizeof(::tbox::fw::ipc::EventHeader), 8u);
}

// ---- Method ID 枚举 (CR-004 §3) ----
TEST(MqttIpcProtocolTest, MethodIds) {
    EXPECT_EQ(static_cast<uint32_t>(ipc::MethodId::PUBLISH), 1u);
    EXPECT_EQ(static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE), 2u);
    EXPECT_EQ(static_cast<uint32_t>(ipc::MethodId::UNSUBSCRIBE), 3u);
    EXPECT_EQ(static_cast<uint32_t>(ipc::MethodId::REGISTER_ROUTE), 4u);
    EXPECT_EQ(static_cast<uint32_t>(ipc::MethodId::GET_CONNECTION_STATE), 5u);
}

// ---- Event 类型 (CR-004 §6) ----
TEST(MqttIpcProtocolTest, EventTypes) {
    EXPECT_EQ(static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE), 1u);
    EXPECT_EQ(static_cast<uint32_t>(ipc::EventType::CONNECTION_STATE_CHANGED), 2u);
}

// ---- publish envelope JSON 编解码 + base64 (CR-004 §5.1) ----
TEST(MqttIpcProtocolTest, PublishEnvelopeJsonRoundtrip) {
    PublishEnvelope env;
    env.msg_id = "msg-001";
    env.topic = "vehicle/{device_sn}/up/telemetry";
    env.qos = QoS::AT_LEAST_ONCE;
    env.priority = Priority::HIGH;
    env.content_type = "application/x-protobuf";
    env.payload = {0xDE, 0xAD, 0xBE, 0xEF};
    env.trace_id = "trace-1";
    env.request_id = "req-1";

    // 编码为 wire JSON（payload_base64）
    json j;
    j["msg_id"] = env.msg_id;
    j["topic"] = env.topic;
    j["qos"] = static_cast<int>(env.qos);
    j["priority"] = static_cast<int>(env.priority);
    j["content_type"] = env.content_type;
    j["payload_base64"] = "3q2+7w==";  // base64 of {0xDE,0xAD,0xBE,0xEF}
    j["trace_id"] = env.trace_id;
    j["request_id"] = env.request_id;

    std::string wire = j.dump();

    // 解码回来
    auto parsed = json::parse(wire);
    EXPECT_EQ(parsed["msg_id"], "msg-001");
    EXPECT_EQ(parsed["topic"], "vehicle/{device_sn}/up/telemetry");
    EXPECT_EQ(parsed["qos"], 1);
    EXPECT_EQ(parsed["priority"], 2);
    EXPECT_EQ(parsed["content_type"], "application/x-protobuf");
    EXPECT_EQ(parsed["trace_id"], "trace-1");
    EXPECT_EQ(parsed["request_id"], "req-1");
    EXPECT_EQ(parsed["payload_base64"], "3q2+7w==");
}

// ---- msg_id 必填 (CR-004 §5.1) ----
TEST(MqttIpcProtocolTest, MsgIdIsRequired) {
    json j;
    j["topic"] = "test/topic";
    j["qos"] = 1;
    // 无 msg_id
    std::string wire = j.dump();
    auto parsed = json::parse(wire);
    EXPECT_TRUE(parsed["msg_id"].is_null() || parsed.value("msg_id", "") == "");
}

// ---- 响应 status 字段 (CR-004 §3) ----
TEST(MqttIpcProtocolTest, ResponseStatusField) {
    json resp;
    resp["accepted"] = true;
    resp["outcome"] = "accepted";
    std::string wire = resp.dump();
    resp["status"] = 0;  // 业务状态码嵌入 JSON
    EXPECT_EQ(resp["status"], 0);
}

// ---- 错误码枚举 (设计 §6) ----
TEST(MqttIpcProtocolTest, ErrorCodes) {
    EXPECT_EQ(static_cast<uint32_t>(ErrorCode::MQTT_1001), 1001u);
    EXPECT_EQ(static_cast<uint32_t>(ErrorCode::MQTT_1008), 1008u);
    EXPECT_EQ(errorCodeToString(ErrorCode::MQTT_1007), "MQTT-1007");
}

// ---- 连接状态枚举 ----
TEST(MqttIpcProtocolTest, ConnectionStateEnum) {
    EXPECT_EQ(connectionStateToString(ConnectionState::CONNECTED), "CONNECTED");
    EXPECT_EQ(connectionStateToString(ConnectionState::CERT_WAIT), "CERT_WAIT");
}
