#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_retry_policy.h"
#include "utils.h"
#include <nlohmann/json.hpp>
#include <chrono>
#include <cstring>

namespace tbox {
namespace mqtt {

// ============================================================
// Client::Impl
// ============================================================
class Client::Impl {
public:
    explicit Impl(const std::string& socket_path)
        : socket_path_(socket_path),
          fw_client_(std::make_unique<::tbox::fw::ipc::Client>(socket_path_)) {
    }

    ~Impl() {
        disconnect();
    }

    bool connect() {
        return fw_client_->connect();
    }

    void disconnect() {
        fw_client_->disconnect();
    }

    bool is_connected() const {
        return fw_client_->is_connected();
    }

    /// Send request and receive response (CR-004 §5.2, §7.1)
    /// @return (transport_ok, biz_status_code, response_json)
    std::tuple<bool, int32_t, std::string>
    send_request(uint32_t method_id, const std::string& params_json) {
        bool retry_allowed = MqttRetryPolicy::should_retry(method_id);

        std::pair<int32_t, std::string> result;
        if (retry_allowed) {
            result = fw_client_->call(method_id, params_json);
        } else {
            // callOnce: no auto-retry, transport failure = unknown outcome (CR-004 §7.1)
            result = fw_client_->callOnce(method_id, params_json);
        }

        auto [fw_status, response_json] = result;

        if (fw_status < 0) {
            // Client transport error: unknown outcome (server may have executed)
            return {false, 0, ""};
        }
        if (fw_status > 0) {
            // Server transport/handler error (FW-03xx)
            return {false, 0, ""};
        }

        // fw_status == 0: transport success, extract business status from JSON
        int32_t biz_status = 0;
        try {
            auto j = nlohmann::json::parse(response_json);
            biz_status = j.value("status", 0);
        } catch (...) {
            // JSON parse failure, keep biz_status = 0
        }
        return {true, biz_status, response_json};
    }

    std::string b64_encode(const std::vector<uint8_t>& data) {
        return ::hwyz::Utils::base64_encode(
            std::string(reinterpret_cast<const char*>(data.data()), data.size()));
    }

    std::vector<uint8_t> b64_decode(const std::string& encoded) {
        std::string decoded = ::hwyz::Utils::base64_decode(encoded);
        return std::vector<uint8_t>(decoded.begin(), decoded.end());
    }

    ::tbox::fw::ipc::Subscription subscribe_event(uint32_t event_type,
                                                   MessageCallback callback) {
        // 每个 subscribe 建立独立连接 (CR-004 §6)
        return fw_client_->subscribe(
            static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE),
            event_type,
            [cb = std::move(callback)](uint32_t /*ev_type*/, std::string_view payload_json) {
                // 解包下行事件 payload: {topic, payload_base64}
                try {
                    auto j = nlohmann::json::parse(payload_json);
                    std::string topic = j.value("topic", "");
                    std::string b64 = j.value("payload_base64", "");
                    std::vector<uint8_t> payload;
                    if (!b64.empty()) {
                        std::string decoded = ::hwyz::Utils::base64_decode(b64);
                        payload.assign(decoded.begin(), decoded.end());
                    }
                    if (cb) {
                        cb(topic, payload);
                    }
                } catch (const std::exception&) {
                    // 忽略格式错误的事件
                }
            });
    }

private:
    std::string socket_path_;
    std::unique_ptr<::tbox::fw::ipc::Client> fw_client_;
};

// ============================================================
// Client facade (CR-004 §7.1)
// ============================================================

Client::Client(const std::string& socket_path)
    : impl_(std::make_unique<Impl>(socket_path)) {
}

Client::~Client() = default;

bool Client::connect() {
    return impl_->connect();
}

void Client::disconnect() {
    impl_->disconnect();
}

bool Client::is_connected() const {
    return impl_->is_connected();
}

PublishResult Client::publish(const PublishEnvelope& message) {
    PublishResult result;

    // 构建 envelope JSON (CR-004 §5.1)
    nlohmann::json params;
    params["msg_id"] = message.msg_id;
    params["topic"] = message.topic;
    params["qos"] = static_cast<int>(message.qos);
    params["priority"] = static_cast<int>(message.priority);
    params["content_type"] = message.content_type;
    if (!message.payload.empty()) {
        params["payload_base64"] = impl_->b64_encode(message.payload);
    }
    params["trace_id"] = message.trace_id;
    params["request_id"] = message.request_id;

    // publish: 相同 msg_id 可去重时允许一次重试 (CR-004 §7.1)
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH), params.dump());

    if (!ok) {
        // 传输失败：unknown outcome (CR-004 §5.2)
        // 只能用相同 msg_id 查询/重试，Server 按去重返回已有受理状态
        result.accepted = false;
        result.outcome = PublishOutcome::UNKNOWN;
        return result;
    }

    // 解析业务响应
    try {
        auto j = nlohmann::json::parse(response_json);
        result.accepted = j.value("accepted", false);
        std::string outcome = j.value("outcome", "rejected");
        result.outcome = (outcome == "accepted") ? PublishOutcome::ACCEPTED
                                                  : PublishOutcome::UNKNOWN;
        if (status != 0) {
            result.error = static_cast<ErrorCode>(status);
        }
    } catch (const std::exception&) {
        result.accepted = false;
        result.outcome = PublishOutcome::UNKNOWN;
    }
    return result;
}

::tbox::fw::ipc::Subscription Client::subscribe(const Route& route, MessageCallback callback) {
    nlohmann::json params;
    params["topic"] = route.topic;
    params["qos"] = static_cast<int>(route.qos);

    // subscribe 为一次性，禁止自动重放 (CR-004 §7.1)
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE), params.dump());

    if (!ok || status != 0) {
        // 订阅失败：返回非活跃句柄
        return ::tbox::fw::ipc::Subscription{};
    }

    // 建立 framework Subscription（独立连接，event-only loop, CR-004 §6）
    // framework 发送空 params，server 调 onSubscribeEventChannel 登记 add_subscription。
    // 下行事件 payload 含 topic，client 按 route.topic 过滤。
    std::string filter_topic = route.topic;
    return impl_->subscribe_event(
        static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE),
        [cb = std::move(callback), filter_topic](const std::string& topic, const std::vector<uint8_t>& payload) {
            if (topic == filter_topic || filter_topic.empty()) {
                if (cb) cb(topic, payload);
            }
        });
}

Result<void> Client::registerRoute(const RouteEntry& route) {
    nlohmann::json params;
    params["internal_addr"] = route.internal_addr;
    params["mqtt_topic"] = route.mqtt_topic;
    params["qos"] = static_cast<int>(route.qos);
    params["direction"] = (route.direction == RouteEntry::Direction::DOWN) ? "DOWN" : "UP";

    // registerRoute 同 key 覆盖，允许一次传输重试 (CR-004 §7.1)
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::REGISTER_ROUTE), params.dump());

    if (!ok) {
        return Result<void>::failure(ErrorCode::MQTT_1002);
    }
    if (status != 0) {
        return Result<void>::failure(static_cast<ErrorCode>(status));
    }
    return Result<void>::success();
}

ConnectionState Client::getConnectionState() {
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::GET_CONNECTION_STATE), "{}");

    if (!ok) {
        return ConnectionState::OFFLINE;
    }

    try {
        auto j = nlohmann::json::parse(response_json);
        std::string state_str = j.value("state", "OFFLINE");
        if (state_str == "INIT") return ConnectionState::INIT;
        if (state_str == "CERT_WAIT") return ConnectionState::CERT_WAIT;
        if (state_str == "CONNECTING") return ConnectionState::CONNECTING;
        if (state_str == "CONNECTED") return ConnectionState::CONNECTED;
        if (state_str == "RECONNECTING") return ConnectionState::RECONNECTING;
        return ConnectionState::OFFLINE;
    } catch (const std::exception&) {
        return ConnectionState::OFFLINE;
    }
}

// ============================================================
// CR-007 §4: 订阅快照与 Cloud Readiness
// ============================================================

ReplaceSnapshotResult Client::replaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot& snapshot) {
    nlohmann::json params;
    params["owner"] = snapshot.owner;
    params["generation"] = snapshot.generation;
    params["content_digest"] = snapshot.content_digest;
    params["registration_complete"] = snapshot.registration_complete;
    nlohmann::json items = nlohmann::json::array();
    for (const auto& it : snapshot.items) {
        nlohmann::json item;
        item["route_id"] = it.route_id;
        item["topic_template"] = it.topic_template;
        item["direction"] = snapshotDirectionToString(it.direction);
        item["qos"] = static_cast<int>(it.qos);
        item["target"] = it.target;
        item["mandatory"] = it.mandatory;
        items.push_back(item);
    }
    params["items"] = items;

    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::REPLACE_SUBSCRIPTION_SNAPSHOT), params.dump());

    ReplaceSnapshotResult result;
    if (!ok) {
        result.status = SnapshotAcceptStatus::UNKNOWN;
        result.reason_code = "transport_failed";
        return result;
    }
    try {
        auto j = nlohmann::json::parse(response_json);
        std::string s = j.value("status", "UNKNOWN");
        if (s == "ACCEPTED") result.status = SnapshotAcceptStatus::ACCEPTED;
        else if (s == "REJECTED") result.status = SnapshotAcceptStatus::REJECTED;
        else result.status = SnapshotAcceptStatus::UNKNOWN;
        result.accepted_generation = j.value("accepted_generation", 0ULL);
        result.reason_code = j.value("reason_code", "");
        if (j.contains("rejected_route_ids") && j["rejected_route_ids"].is_array()) {
            for (const auto& rid : j["rejected_route_ids"]) {
                result.rejected_route_ids.push_back(rid.get<std::string>());
            }
        }
    } catch (...) {
        result.status = SnapshotAcceptStatus::UNKNOWN;
    }
    return result;
}

SnapshotStatusResult Client::getSubscriptionSnapshotStatus(
        const std::string& owner, uint64_t generation) {
    nlohmann::json params;
    params["owner"] = owner;
    params["generation"] = generation;

    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::GET_SUBSCRIPTION_SNAPSHOT_STATUS), params.dump());

    SnapshotStatusResult r;
    if (!ok) { r.status = SnapshotAcceptStatus::UNKNOWN; return r; }
    try {
        auto j = nlohmann::json::parse(response_json);
        std::string s = j.value("status", "UNKNOWN");
        if (s == "ACCEPTED") r.status = SnapshotAcceptStatus::ACCEPTED;
        else if (s == "REJECTED") r.status = SnapshotAcceptStatus::REJECTED;
        else r.status = SnapshotAcceptStatus::UNKNOWN;
        r.generation = j.value("generation", 0ULL);
        r.content_digest = j.value("content_digest", "");
        r.registration_complete = j.value("registration_complete", false);
    } catch (...) {
        r.status = SnapshotAcceptStatus::UNKNOWN;
    }
    return r;
}

SubscriptionReadiness Client::getSubscriptionReadiness() {
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::GET_SUBSCRIPTION_READINESS), "{}");

    SubscriptionReadiness r;
    if (!ok) return r;
    try {
        auto j = nlohmann::json::parse(response_json);
        r.cloud_ready = j.value("cloud_ready", false);
        r.subscriptions_ready = j.value("subscriptions_ready", false);
        r.connected = j.value("connected", false);
        r.connection_generation = j.value("connection_generation", 0ULL);
        r.mandatory_count = j.value("mandatory_count", 0);
        r.mandatory_active = j.value("mandatory_active", 0);
        r.reason_code = j.value("reason_code", "");
        if (j.contains("owner_generations") && j["owner_generations"].is_object()) {
            for (auto it = j["owner_generations"].begin(); it != j["owner_generations"].end(); ++it) {
                r.owner_generations[it.key()] = it.value().get<uint64_t>();
            }
        }
    } catch (...) {}
    return r;
}

} // namespace mqtt
} // namespace tbox
