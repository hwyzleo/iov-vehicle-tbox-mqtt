#include "mqtt_ipc_dispatcher.h"
#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "utils.h"
#include <nlohmann/json.hpp>
#include <chrono>

namespace tbox {
namespace mqtt {

namespace {

// FW-0305: JSON/base64 serialization failed
constexpr int32_t FW_SERIALIZATION_FAILED = 305;
// FW-0306: unknown method or request handler failed
constexpr int32_t FW_HANDLER_FAILED = 306;

std::string make_json_response(int32_t status, const nlohmann::json& payload) {
    nlohmann::json j = payload;
    j["status"] = status;
    return j.dump();
}

/// base64 编码 vector<uint8_t> -> string
std::string b64_encode(const std::vector<uint8_t>& data) {
    return ::hwyz::Utils::base64_encode(
        std::string(reinterpret_cast<const char*>(data.data()), data.size()));
}

/// base64 解码 string -> vector<uint8_t>
std::vector<uint8_t> b64_decode(const std::string& encoded) {
    std::string decoded = ::hwyz::Utils::base64_decode(encoded);
    return std::vector<uint8_t>(decoded.begin(), decoded.end());
}

QoS parse_qos(int v) {
    if (v == 0) return QoS::AT_MOST_ONCE;
    if (v == 2) return QoS::EXACTLY_ONCE;
    return QoS::AT_LEAST_ONCE;
}

Priority parse_priority(int v) {
    if (v <= 0) return Priority::LOW;
    if (v == 1) return Priority::NORMAL;
    if (v == 2) return Priority::HIGH;
    return Priority::CRITICAL;
}

} // anonymous namespace

MqttIpcDispatcher::MqttIpcDispatcher(ServiceHandler* service)
    : service_(service) {
}

std::string MqttIpcDispatcher::dispatch(uint32_t method_id,
                                        std::string_view params_json,
                                        int client_fd) {
    auto start = std::chrono::steady_clock::now();

    // IPC 日志只记录 method_id 和 payload_bytes，不记录原始 JSON (CR-004 §8)
    MqttLogAdapter::ipc().debug(
        "mqtt.ipc.dispatch",
        "Dispatching request",
        {tbox::fw::log::Field("method_id", tbox::fw::log::FieldValue::makeInt(static_cast<int>(method_id))),
         tbox::fw::log::Field("payload_bytes", tbox::fw::log::FieldValue::makeInt(static_cast<int>(params_json.size()))),
         tbox::fw::log::Field("peer", tbox::fw::log::FieldValue::makeInt(client_fd))}
    );

    std::pair<int32_t, std::string> result{FW_HANDLER_FAILED, "{}"};

    try {
        switch (static_cast<ipc::MethodId>(method_id)) {
            case ipc::MethodId::PUBLISH:
                result = handlePublish(params_json);
                break;
            case ipc::MethodId::SUBSCRIBE:
                result = handleSubscribe(params_json, client_fd);
                break;
            case ipc::MethodId::UNSUBSCRIBE:
                result = handleUnsubscribe(client_fd);
                break;
            case ipc::MethodId::REGISTER_ROUTE:
                result = handleRegisterRoute(params_json);
                break;
            case ipc::MethodId::GET_CONNECTION_STATE:
                result = handleGetConnectionState();
                break;
            // CR-007 §4
            case ipc::MethodId::REPLACE_SUBSCRIPTION_SNAPSHOT:
                result = handleReplaceSubscriptionSnapshot(params_json);
                break;
            case ipc::MethodId::GET_SUBSCRIPTION_SNAPSHOT_STATUS:
                result = handleGetSubscriptionSnapshotStatus(params_json);
                break;
            case ipc::MethodId::GET_SUBSCRIPTION_READINESS:
                result = handleGetSubscriptionReadiness();
                break;
            default:
                MqttLogAdapter::ipc().warn(
                    "mqtt.ipc.unknown_method",
                    "Unknown method ID",
                    {tbox::fw::log::Field("method_id", tbox::fw::log::FieldValue::makeInt(static_cast<int>(method_id)))}
                );
                result = {FW_HANDLER_FAILED, nlohmann::json({{"error", "Unknown method"}}).dump()};
                break;
        }
    } catch (const nlohmann::json::exception& e) {
        MqttLogAdapter::ipc().error(
            "mqtt.ipc.json_error",
            "JSON serialization error in dispatch",
            {tbox::fw::log::Field("method_id", tbox::fw::log::FieldValue::makeInt(static_cast<int>(method_id))),
             tbox::fw::log::Field("what", tbox::fw::log::FieldValue::makeString(e.what()))}
        );
        result = {FW_SERIALIZATION_FAILED, nlohmann::json({{"error", "JSON error"}}).dump()};
    } catch (const std::exception& e) {
        MqttLogAdapter::ipc().error(
            "mqtt.ipc.dispatch_exception",
            "Exception in dispatch",
            {tbox::fw::log::Field("method_id", tbox::fw::log::FieldValue::makeInt(static_cast<int>(method_id))),
             tbox::fw::log::Field("what", tbox::fw::log::FieldValue::makeString(e.what()))}
        );
        result = {FW_HANDLER_FAILED, nlohmann::json({{"error", "Internal error"}}).dump()};
    } catch (...) {
        MqttLogAdapter::ipc().error(
            "mqtt.ipc.dispatch_exception",
            "Unknown exception in dispatch",
            {tbox::fw::log::Field("method_id", tbox::fw::log::FieldValue::makeInt(static_cast<int>(method_id)))}
        );
        result = {FW_HANDLER_FAILED, nlohmann::json({{"error", "Unknown exception"}}).dump()};
    }

    auto end = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // IPC 日志只记录 method_id、status、duration，不记录响应内容 (CR-004 §8)
    MqttLogAdapter::ipc().debug(
        "mqtt.ipc.dispatched",
        "Request dispatched",
        {tbox::fw::log::Field("method_id", tbox::fw::log::FieldValue::makeInt(static_cast<int>(method_id))),
         tbox::fw::log::Field("status", tbox::fw::log::FieldValue::makeInt(result.first)),
         tbox::fw::log::Field("duration_ms", tbox::fw::log::FieldValue::makeInt(duration_ms)),
         tbox::fw::log::Field("peer", tbox::fw::log::FieldValue::makeInt(client_fd))}
    );

    try {
        auto payload = nlohmann::json::parse(result.second);
        return make_json_response(result.first, payload);
    } catch (...) {
        return make_json_response(result.first, nlohmann::json({{"error", "Invalid response"}}));
    }
}

void MqttIpcDispatcher::onClientDisconnected(int client_fd) {
    std::lock_guard<std::mutex> lock(fd_mutex_);
    auto it = fd_subscriptions_.find(client_fd);
    if (it != fd_subscriptions_.end()) {
        MqttLogAdapter::ipc().debug(
            "mqtt.ipc.client_disconnected",
            "Client disconnected, cleaning per-fd subscriptions",
            {tbox::fw::log::Field("peer", tbox::fw::log::FieldValue::makeInt(client_fd)),
             tbox::fw::log::Field("subscription_count",
                 tbox::fw::log::FieldValue::makeInt(static_cast<int>(it->second.size())))}
        );
        fd_subscriptions_.erase(it);
    }
}

bool MqttIpcDispatcher::isAccepted(const std::string& msg_id) const {
    std::lock_guard<std::mutex> lock(dedup_mutex_);
    return accepted_msg_ids_.count(msg_id) > 0;
}

std::pair<int32_t, std::string> MqttIpcDispatcher::handlePublish(std::string_view params) {
    auto j = nlohmann::json::parse(params);

    PublishEnvelope env;
    env.msg_id = j.value("msg_id", "");
    env.topic = j.value("topic", "");
    env.qos = parse_qos(j.value("qos", 1));
    env.priority = parse_priority(j.value("priority", 1));
    env.content_type = j.value("content_type", "");
    env.trace_id = j.value("trace_id", "");
    env.request_id = j.value("request_id", "");

    std::string payload_b64 = j.value("payload_base64", "");
    if (!payload_b64.empty()) {
        env.payload = b64_decode(payload_b64);
    }

    // msg_id 必填 (CR-004 §5.1)
    if (env.msg_id.empty()) {
        MqttLogAdapter::publish().warn(
            "mqtt.publish.rejected",
            "msg_id 为空，拒绝发布",
            {{"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(env.topic))},
             {"qos", tbox::fw::log::FieldValue::makeString(qosToString(env.qos))},
             {"payload_bytes", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(env.payload.size()))},
             {"validation_reason", tbox::fw::log::FieldValue::makeString("empty_msg_id")},
             {"error_code", tbox::fw::log::FieldValue::makeString("MQTT-1004")}}
        );
        nlohmann::json resp;
        resp["accepted"] = false;
        resp["outcome"] = "rejected";
        return {static_cast<int32_t>(ErrorCode::MQTT_1004), resp.dump()};
    }

    // 去重：相同 msg_id 已受理则返回已有受理状态 (CR-004 §5.2)
    {
        std::lock_guard<std::mutex> lock(dedup_mutex_);
        if (accepted_msg_ids_.count(env.msg_id) > 0) {
            MqttLogAdapter::publish().debug(
                "mqtt.publish.deduplicated",
                "重复 msg_id，返回已有受理状态",
                {{"msg_id_hash", tbox::fw::log::FieldValue::makeString(msgIdHash(env.msg_id))},
                 {"topic_template", tbox::fw::log::FieldValue::makeString(topicTemplate(env.topic))}}
            );
            nlohmann::json resp;
            resp["accepted"] = true;
            resp["outcome"] = "accepted";
            resp["deduplicated"] = true;
            return {0, resp.dump()};
        }
    }

    // 调用业务 handler (accepted 边界：在线入发送队列 / 离线入离线队列)
    auto [status, accepted] = service_->onPublish(env);

    if (accepted) {
        std::lock_guard<std::mutex> lock(dedup_mutex_);
        accepted_msg_ids_.insert(env.msg_id);
    }

    nlohmann::json resp;
    resp["accepted"] = accepted;
    resp["outcome"] = accepted ? "accepted" : "rejected";
    return {status, resp.dump()};
}

std::pair<int32_t, std::string> MqttIpcDispatcher::handleSubscribe(std::string_view params, int client_fd) {
    // framework-ipc Client::subscribe 发送空 params（纯事件通道注册, CR-004 §6）
    if (params.empty()) {
        auto [status, ok] = service_->onSubscribeEventChannel(client_fd);
        nlohmann::json resp;
        resp["ok"] = ok;
        return {status, resp.dump()};
    }

    auto j = nlohmann::json::parse(params);

    Route route;
    route.topic = j.value("topic", "");
    route.qos = parse_qos(j.value("qos", 1));

    if (route.topic.empty()) {
        nlohmann::json resp;
        resp["ok"] = false;
        resp["error"] = "Missing topic";
        return {static_cast<int32_t>(ErrorCode::MQTT_1004), resp.dump()};
    }

    auto [status, ok] = service_->onSubscribe(route, client_fd);

    if (ok) {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        fd_subscriptions_[client_fd].insert(route.topic);
    }

    nlohmann::json resp;
    resp["ok"] = ok;
    return {status, resp.dump()};
}

std::pair<int32_t, std::string> MqttIpcDispatcher::handleUnsubscribe(int client_fd) {
    auto [status, ok] = service_->onUnsubscribe(client_fd);

    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        fd_subscriptions_.erase(client_fd);
    }

    nlohmann::json resp;
    resp["ok"] = ok;
    return {status, resp.dump()};
}

std::pair<int32_t, std::string> MqttIpcDispatcher::handleRegisterRoute(std::string_view params) {
    auto j = nlohmann::json::parse(params);

    RouteEntry entry;
    entry.internal_addr = j.value("internal_addr", "");
    entry.mqtt_topic = j.value("mqtt_topic", "");
    entry.qos = parse_qos(j.value("qos", 1));
    std::string dir = j.value("direction", "UP");
    entry.direction = (dir == "DOWN") ? RouteEntry::Direction::DOWN : RouteEntry::Direction::UP;

    if (entry.internal_addr.empty() || entry.mqtt_topic.empty()) {
        nlohmann::json resp;
        resp["ok"] = false;
        resp["error"] = "Missing internal_addr or mqtt_topic";
        return {static_cast<int32_t>(ErrorCode::MQTT_1004), resp.dump()};
    }

    auto [status, ok] = service_->onRegisterRoute(entry);

    nlohmann::json resp;
    resp["ok"] = ok;
    return {status, resp.dump()};
}

std::pair<int32_t, std::string> MqttIpcDispatcher::handleGetConnectionState() {
    std::string state_json = service_->onGetConnectionState();
    try {
        auto payload = nlohmann::json::parse(state_json);
        return {0, payload.dump()};
    } catch (...) {
        nlohmann::json resp;
        resp["state"] = "UNKNOWN";
        return {0, resp.dump()};
    }
}

// ============================================================
// CR-007 §4: 订阅快照与 Cloud Readiness handler
// ============================================================

std::pair<int32_t, std::string> MqttIpcDispatcher::handleReplaceSubscriptionSnapshot(
        std::string_view params) {
    auto j = nlohmann::json::parse(params);

    OwnerSubscriptionSnapshot snapshot;
    snapshot.owner = j.value("owner", "");
    snapshot.generation = j.value("generation", 0ULL);
    snapshot.content_digest = j.value("content_digest", "");
    snapshot.registration_complete = j.value("registration_complete", false);
    if (j.contains("items") && j["items"].is_array()) {
        for (const auto& it : j["items"]) {
            SubscriptionItem item;
            item.route_id = it.value("route_id", "");
            item.topic_template = it.value("topic_template", "");
            std::string dir = it.value("direction", "UP");
            snapshotDirectionFromString(dir, item.direction);
            item.qos = static_cast<uint8_t>(it.value("qos", 1));
            item.target = it.value("target", "");
            item.mandatory = it.value("mandatory", false);
            snapshot.items.push_back(std::move(item));
        }
    }

    auto result = service_->onReplaceSubscriptionSnapshot(snapshot);

    // 业务状态码：REJECTED -> MQTT-1010，其余 0
    int32_t status = (result.status == SnapshotAcceptStatus::REJECTED)
        ? static_cast<int32_t>(ErrorCode::MQTT_1010) : 0;

    nlohmann::json resp;
    resp["status"] = snapshotAcceptStatusToString(result.status);
    resp["accepted_generation"] = result.accepted_generation;
    resp["rejected_route_ids"] = result.rejected_route_ids;
    resp["reason_code"] = result.reason_code;

    MqttLogAdapter::subscribe().info(
        "mqtt.subscription.snapshot.dispatched",
        "快照提交处理完成",
        {{"owner", tbox::fw::log::FieldValue::makeString(snapshot.owner)},
         {"generation", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(snapshot.generation))},
         {"status", tbox::fw::log::FieldValue::makeString(snapshotAcceptStatusToString(result.status))},
         {"item_count", tbox::fw::log::FieldValue::makeInt(static_cast<int64_t>(snapshot.items.size()))}}
    );

    return {status, resp.dump()};
}

std::pair<int32_t, std::string> MqttIpcDispatcher::handleGetSubscriptionSnapshotStatus(
        std::string_view params) {
    auto j = nlohmann::json::parse(params);
    std::string owner = j.value("owner", "");
    uint64_t generation = j.value("generation", 0ULL);

    auto result = service_->onGetSubscriptionSnapshotStatus(owner, generation);

    nlohmann::json resp;
    resp["status"] = snapshotAcceptStatusToString(result.status);
    resp["generation"] = result.generation;
    resp["content_digest"] = result.content_digest;
    resp["registration_complete"] = result.registration_complete;
    return {0, resp.dump()};
}

std::pair<int32_t, std::string> MqttIpcDispatcher::handleGetSubscriptionReadiness() {
    auto r = service_->onGetSubscriptionReadiness();

    nlohmann::json resp;
    resp["cloud_ready"] = r.cloud_ready;
    resp["subscriptions_ready"] = r.subscriptions_ready;
    resp["connected"] = r.connected;
    resp["connection_generation"] = r.connection_generation;
    nlohmann::json gens = nlohmann::json::object();
    for (const auto& [owner, gen] : r.owner_generations) {
        gens[owner] = gen;
    }
    resp["owner_generations"] = gens;
    resp["mandatory_count"] = r.mandatory_count;
    resp["mandatory_active"] = r.mandatory_active;
    resp["reason_code"] = r.reason_code;
    return {0, resp.dump()};
}

} // namespace mqtt
} // namespace tbox
