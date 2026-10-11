// CR-015 §18.4: Capability Readiness SDK 单元测试
// 覆盖：能力查询解析、事件订阅、sequence 缺口对账、contract 不兼容、
// capability 不匹配、取消幂等与参数非法。
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

class CapabilityTestHandler : public MqttIpcDispatcher::ServiceHandler {
public:
    ::tbox::fw::ipc::Server* server = nullptr;
    std::atomic<uint64_t> snapshot_sequence{0};
    std::atomic<int> query_count{0};

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
        return CloudReadinessSnapshot{};
    }
    PublishResult onPublishRoute(const RoutePublishRequest&) override { return PublishResult{}; }

    CapabilityReadiness onGetCapabilityReadiness(const std::string& capability) override {
        query_count.fetch_add(1);
        CapabilityReadiness r;
        r.capability = capability;
        r.enabled = true;
        r.contract_supported = true;
        r.snapshot_accepted = true;
        r.uplink_ready = true;
        r.downlink_suback_ready = true;
        r.ready = true;
        r.owner_generation = 3;
        r.connection_generation = 4;
        r.cert_session_generation = 9;
        r.reason_code = "READY";
        r.event_sequence = snapshot_sequence.load();
        return r;
    }

    std::pair<int32_t, bool> onSubscribeCapabilityReadinessEventChannel(
        const std::string&, int client_fd) override {
        if (server) {
            server->add_subscription(
                client_fd, static_cast<uint32_t>(ipc::EventType::CAPABILITY_READINESS_CHANGED));
        }
        return {0, true};
    }
};

std::string make_socket(const char* tag) {
    return std::string("/tmp/tbox_mqtt_capability_") + tag + "_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
}

std::string capabilityJson(uint64_t seq, bool ready = true,
                           const std::string& capability = kCapabilitySecurityKeyprov,
                           int contract_version =
                               static_cast<int>(kCapabilityReadinessContractVersion)) {
    nlohmann::json j;
    j["capability"] = capability;
    j["enabled"] = true;
    j["contract_supported"] = true;
    j["snapshot_accepted"] = true;
    j["uplink_ready"] = true;
    j["downlink_suback_ready"] = ready;
    j["ready"] = ready;
    j["owner_generation"] = 3;
    j["connection_generation"] = 4;
    j["cert_session_generation"] = 9;
    j["reason_code"] = ready ? "READY" : capability_reason::kKeyprovSubscribePending;
    j["event_sequence"] = seq;
    if (contract_version > 0) j["contract_version"] = contract_version;
    return j.dump();
}

} // namespace

class MqttClientCapabilityTest : public ::testing::Test {
protected:
    std::string socket_path;
    CapabilityTestHandler handler;
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
        server->push_event(
            static_cast<uint32_t>(ipc::EventType::CAPABILITY_READINESS_CHANGED), json);
    }
};

TEST_F(MqttClientCapabilityTest, QueryReturnsReadiness) {
    handler.snapshot_sequence.store(7);
    auto r = client->getCapabilityReadiness(kCapabilitySecurityKeyprov);
    EXPECT_TRUE(r.contract_supported);
    EXPECT_TRUE(r.ready);
    EXPECT_EQ(r.capability, kCapabilitySecurityKeyprov);
    EXPECT_EQ(r.owner_generation, 3u);
    EXPECT_EQ(r.event_sequence, 7u);
}

TEST_F(MqttClientCapabilityTest, QueryEmptyCapabilityUnsupported) {
    auto r = client->getCapabilityReadiness("");
    EXPECT_FALSE(r.contract_supported);
    EXPECT_FALSE(r.ready);
}

TEST_F(MqttClientCapabilityTest, SubscribeDeliversReadiness) {
    std::mutex m;
    std::vector<CapabilityReadiness> events;
    auto sub = client->subscribeCapabilityReadinessChanged(
        kCapabilitySecurityKeyprov,
        [&](const CapabilityReadiness& r) { std::lock_guard<std::mutex> l(m); events.push_back(r); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(capabilityJson(1));
    for (int i = 0; i < 50 && events.empty(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_FALSE(events.empty());
    std::lock_guard<std::mutex> l(m);
    EXPECT_TRUE(events[0].ready);
    EXPECT_EQ(events[0].capability, kCapabilitySecurityKeyprov);
    EXPECT_EQ(events[0].event_sequence, 1u);
    EXPECT_TRUE(events[0].contract_supported);
}

TEST_F(MqttClientCapabilityTest, SequenceGapTriggersResync) {
    std::mutex m;
    std::vector<uint64_t> sequences;
    auto sub = client->subscribeCapabilityReadinessChanged(
        kCapabilitySecurityKeyprov,
        [&](const CapabilityReadiness& r) { std::lock_guard<std::mutex> l(m); sequences.push_back(r.event_sequence); });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(capabilityJson(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    handler.snapshot_sequence.store(5);
    push(capabilityJson(3));  // 缺口
    for (int i = 0; i < 50; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::lock_guard<std::mutex> l(m);
        if (!sequences.empty() && sequences.back() == 5) break;
    }
    EXPECT_GE(handler.query_count.load(), 1);
    std::lock_guard<std::mutex> l(m);
    ASSERT_FALSE(sequences.empty());
    EXPECT_EQ(sequences.back(), 5u);
    for (uint64_t s : sequences) EXPECT_NE(s, 3u);
}

TEST_F(MqttClientCapabilityTest, UnsupportedContractVersion) {
    std::mutex m;
    CapabilityReadiness got;
    bool received = false;
    auto sub = client->subscribeCapabilityReadinessChanged(
        kCapabilitySecurityKeyprov,
        [&](const CapabilityReadiness& r) { std::lock_guard<std::mutex> l(m); got = r; received = true; });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(capabilityJson(1, true, kCapabilitySecurityKeyprov, 0));  // 省略 contract_version
    for (int i = 0; i < 50 && !received; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(received);
    std::lock_guard<std::mutex> l(m);
    EXPECT_FALSE(got.contract_supported);
    EXPECT_FALSE(got.ready);
}

TEST_F(MqttClientCapabilityTest, CapabilityMismatchRejected) {
    std::mutex m;
    CapabilityReadiness got;
    bool received = false;
    auto sub = client->subscribeCapabilityReadinessChanged(
        kCapabilitySecurityKeyprov,
        [&](const CapabilityReadiness& r) { std::lock_guard<std::mutex> l(m); got = r; received = true; });
    ASSERT_TRUE(sub.isActive());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    push(capabilityJson(1, true, "security.other"));
    for (int i = 0; i < 50 && !received; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(received);
    std::lock_guard<std::mutex> l(m);
    EXPECT_FALSE(got.contract_supported);
    EXPECT_FALSE(got.ready);
}

TEST_F(MqttClientCapabilityTest, EmptyCallbackInactive) {
    auto sub = client->subscribeCapabilityReadinessChanged(
        kCapabilitySecurityKeyprov, CapabilityReadinessCallback{});
    EXPECT_FALSE(sub.isActive());
}

TEST_F(MqttClientCapabilityTest, EmptyCapabilityInactive) {
    auto sub = client->subscribeCapabilityReadinessChanged(
        "", [](const CapabilityReadiness&) {});
    EXPECT_FALSE(sub.isActive());
}

TEST_F(MqttClientCapabilityTest, CancelIdempotent) {
    auto sub = client->subscribeCapabilityReadinessChanged(
        kCapabilitySecurityKeyprov, [](const CapabilityReadiness&) {});
    ASSERT_TRUE(sub.isActive());
    sub.cancel();
    EXPECT_FALSE(sub.isActive());
    sub.cancel();  // 幂等
    EXPECT_FALSE(sub.isActive());
}
