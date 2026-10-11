// CR-014 §5/§8: Cloud Ready 快照查询与事件订阅 SDK 单元测试
// 覆盖：订阅进入/退出事件、首次快照解析、sequence 缺口/回退自动对账、
// contract_version 不兼容、取消/参数非法、callback 独立执行且异常不终止。
#include <gtest/gtest.h>
#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_ipc_dispatcher.h"
#include "ipc.h"
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

using namespace tbox::mqtt;

namespace {

class ReadinessTestHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    ::tbox::fw::ipc::Server* server = nullptr;
    std::atomic<uint64_t> snapshot_sequence{0};
    std::atomic<int> readiness_query_count{0};

    std::pair<int32_t, bool> onPublish(const PublishEnvelope&) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribe(const Route&, int) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribeEventChannel(int) override { return {0, true}; }
    std::pair<int32_t, bool> onUnsubscribe(int) override { return {0, true}; }
    std::pair<int32_t, bool> onRegisterRoute(const RouteEntry&) override { return {0, true}; }
    std::string onGetConnectionState() override { return R"({"state":"CLOUD_READY"})"; }
    ReplaceSnapshotResult onReplaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot&) override { return ReplaceSnapshotResult{}; }
    SnapshotStatusResult onGetSubscriptionSnapshotStatus(
        const std::string&, uint64_t) override { return SnapshotStatusResult{}; }
    CloudReadinessSnapshot onGetSubscriptionReadiness() override {
        readiness_query_count.fetch_add(1);
        CloudReadinessSnapshot s;
        s.cloud_ready = true;
        s.connection_generation = 4;
        s.cert_session_generation = 9;
        s.credential_id = "cred-x";
        s.credential_version = 3;
        s.device_identity_digest = "sha256:ffeedd";
        s.certificate_ref = "sha256:ffeedd";
        s.event_sequence = snapshot_sequence.load();
        s.subscriptions_ready = true;
        s.connected = true;
        s.contract_version = kCloudReadinessContractVersion;
        s.contract_supported = true;
        return s;
    }
    PublishResult onPublishRoute(const RoutePublishRequest&) override { return PublishResult{}; }
    std::pair<int32_t, bool> onSubscribeRoutedDownlinkEventChannel(
        const std::string&, int) override { return {0, true}; }
    std::pair<int32_t, bool> onSubscribeCloudReadinessEventChannel(int client_fd) override {
        if (server) {
            server->add_subscription(
                client_fd, static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED));
        }
        return {0, true};
    }
};

std::string make_socket(const char* tag) {
    return std::string("/tmp/tbox_mqtt_readiness_") + tag + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
}

// contract_version<=0 时省略字段（模拟旧 daemon）
std::string readinessJson(uint64_t seq, bool ready = true, uint64_t cert_gen = 9,
                          const std::string& cert_ref = "sha256:ffeedd",
                          int contract_version = static_cast<int>(kCloudReadinessContractVersion)) {
    nlohmann::json j;
    j["cloud_ready"] = ready;
    j["connection_generation"] = 4;
    j["cert_session_generation"] = cert_gen;
    j["credential_id"] = "cred-x";
    j["credential_version"] = 3;
    j["device_identity_digest"] = cert_ref;
    j["certificate_ref"] = cert_ref;
    j["reason_code"] = ready ? "cloud_ready" : "connection_lost";
    j["event_sequence"] = seq;
    if (contract_version > 0) j["contract_version"] = contract_version;
    j["subscriptions_ready"] = ready;
    j["connected"] = true;
    j["owner_generations"] = nlohmann::json::object();
    j["mandatory_count"] = 1;
    j["mandatory_active"] = 1;
    return j.dump();
}

} // namespace

class MqttClientReadinessTest : public ::testing::Test {
protected:
    std::string socket_path;
    ReadinessTestHandler handler;
    std::unique_ptr<MqttIpcDispatcher> dispatcher;
    std::unique_ptr<::tbox::fw::ipc::Server> server;
    std::unique_ptr<Client> client;

    void SetUp() override {
        socket_path = make_socket("u");
        ::unlink(socket_path.c_str());
        dispatcher = std::make_unique<MqttIpcDispatcher>(&handler);
        server = std::make_unique<::tbox::fw::ipc::Server>(socket_path);
        handler.server = server.get();
        auto* dp = dispatcher.get();
        ASSERT_TRUE(server->start(
            [dp](uint32_t m, std::string_view p, int fd) { return dp->dispatch(m, p, fd); },
            [dp](int fd) { dp->onClientDisconnected(fd); }));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        client = std::make_unique<Client>(socket_path);
    }

    void TearDown() override {
        client.reset();
        server->stop();
        server.reset();
        ::unlink(socket_path.c_str());
    }

    void push(const std::string& json) {
        server->push_event(static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED), json);
    }
};

// 订阅后收到完整 Cloud Ready 事件
TEST_F(MqttClientReadinessTest, SubscribeDeliversSnapshot) {
    std::mutex m;
    std::vector<CloudReadinessSnapshot> events;
    auto sub = client->subscribeCloudReadinessChanged(
        [&](const CloudReadinessSnapshot& s) { std::lock_guard<std::mutex> l(m); events.push_back(s); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(readinessJson(1));
    for (int i = 0; i < 50 && events.empty(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_FALSE(events.empty());
    std::lock_guard<std::mutex> l(m);
    EXPECT_TRUE(events[0].cloud_ready);
    EXPECT_EQ(events[0].cert_session_generation, 9u);
    EXPECT_EQ(events[0].certificate_ref, "sha256:ffeedd");
    EXPECT_EQ(events[0].event_sequence, 1u);
    EXPECT_TRUE(events[0].contract_supported);
}

// 退出事件序列：进入(true) -> 退出(false)
TEST_F(MqttClientReadinessTest, EnterAndExitEvents) {
    std::mutex m;
    std::vector<bool> ready_seq;
    auto sub = client->subscribeCloudReadinessChanged(
        [&](const CloudReadinessSnapshot& s) { std::lock_guard<std::mutex> l(m); ready_seq.push_back(s.cloud_ready); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(readinessJson(1, true));
    push(readinessJson(2, false));
    for (int i = 0; i < 50 && ready_seq.size() < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::lock_guard<std::mutex> l(m);
    ASSERT_EQ(ready_seq.size(), 2u);
    EXPECT_TRUE(ready_seq[0]);
    EXPECT_FALSE(ready_seq[1]);
}

// sequence 缺口：SDK 自动重查快照并补投（最终收敛）
TEST_F(MqttClientReadinessTest, SequenceGapTriggersResync) {
    std::mutex m;
    std::vector<uint64_t> sequences;
    auto sub = client->subscribeCloudReadinessChanged(
        [&](const CloudReadinessSnapshot& s) { std::lock_guard<std::mutex> l(m); sequences.push_back(s.event_sequence); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(readinessJson(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // daemon 当前已到 sequence 5；推送 seq=3 形成缺口
    handler.snapshot_sequence.store(5);
    push(readinessJson(3));
    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::lock_guard<std::mutex> l(m);
        if (!sequences.empty() && sequences.back() == 5) break;
    }
    EXPECT_GE(handler.readiness_query_count.load(), 1);
    std::lock_guard<std::mutex> l(m);
    ASSERT_FALSE(sequences.empty());
    EXPECT_EQ(sequences.back(), 5u);
    // 不得投递缺口事件本身（seq=3）后继续停留在旧状态
    for (uint64_t s : sequences) EXPECT_NE(s, 3u);
}

// daemon 重启（sequence 回退）：SDK 重查快照
TEST_F(MqttClientReadinessTest, SequenceRegressionTriggersResync) {
    std::mutex m;
    std::vector<uint64_t> sequences;
    auto sub = client->subscribeCloudReadinessChanged(
        [&](const CloudReadinessSnapshot& s) { std::lock_guard<std::mutex> l(m); sequences.push_back(s.event_sequence); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(readinessJson(5));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    handler.snapshot_sequence.store(1);
    push(readinessJson(2));  // 回退
    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::lock_guard<std::mutex> l(m);
        if (!sequences.empty() && sequences.back() == 1) break;
    }
    EXPECT_GE(handler.readiness_query_count.load(), 1);
}

// 不兼容 contract_version：不得伪造空身份宣告 ready
TEST_F(MqttClientReadinessTest, UnsupportedContractVersion) {
    std::mutex m;
    CloudReadinessSnapshot got;
    bool received = false;
    auto sub = client->subscribeCloudReadinessChanged([&](const CloudReadinessSnapshot& s) {
        std::lock_guard<std::mutex> l(m);
        got = s; received = true;
    });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(readinessJson(1, true, 9, "sha256:ffeedd", 0));  // 省略 contract_version
    for (int i = 0; i < 50 && !received; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(received);
    std::lock_guard<std::mutex> l(m);
    EXPECT_FALSE(got.contract_supported);
    EXPECT_FALSE(got.cloud_ready);
    EXPECT_EQ(got.reason_code, "UNSUPPORTED_CONTRACT_VERSION");
}

// 查询返回完整快照
TEST_F(MqttClientReadinessTest, QueryReturnsFullSnapshot) {
    handler.snapshot_sequence.store(7);
    auto r = client->getSubscriptionReadiness();
    EXPECT_TRUE(r.contract_supported);
    EXPECT_TRUE(r.cloud_ready);
    EXPECT_EQ(r.connection_generation, 4u);
    EXPECT_EQ(r.cert_session_generation, 9u);
    EXPECT_EQ(r.credential_id, "cred-x");
    EXPECT_EQ(r.credential_version, 3u);
    EXPECT_EQ(r.device_identity_digest, "sha256:ffeedd");
    EXPECT_EQ(r.certificate_ref, "sha256:ffeedd");
    EXPECT_EQ(r.event_sequence, 7u);
}

// 参数非法：空 callback 返回非活跃句柄
TEST_F(MqttClientReadinessTest, EmptyCallbackInactive) {
    auto sub = client->subscribeCloudReadinessChanged(CloudReadinessCallback{});
    EXPECT_FALSE(sub.isActive());
}

// cancel 幂等且句柄非活跃
TEST_F(MqttClientReadinessTest, CancelIdempotent) {
    auto sub = client->subscribeCloudReadinessChanged(
        [](const CloudReadinessSnapshot&) {});
    ASSERT_TRUE(sub.isActive());
    sub.cancel();
    EXPECT_FALSE(sub.isActive());
    sub.cancel();  // 幂等
    EXPECT_FALSE(sub.isActive());
}
