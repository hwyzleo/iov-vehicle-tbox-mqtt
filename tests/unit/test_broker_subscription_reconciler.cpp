// CR-007 §13: BrokerSubscriptionReconciler 单元测试
#include <gtest/gtest.h>
#include "broker_subscription_reconciler.h"
#include "applied_subscription_manifest.h"
#include "store.h"
#include <filesystem>
#include <memory>

using namespace tbox::mqtt;

namespace {
struct FakeBrokerOps : public BrokerOperations {
    std::vector<std::pair<std::string, uint8_t>> subscribes;
    std::vector<std::string> unsubscribes;
    bool brokerSubscribe(const std::string& topic, uint8_t qos) override {
        subscribes.push_back({topic, qos});
        return true;
    }
    bool brokerUnsubscribe(const std::string& topic) override {
        unsubscribes.push_back(topic);
        return true;
    }
};

std::string mkStore() {
    auto d = std::filesystem::temp_directory_path() / "tbox_mqtt_reconcile_test";
    std::filesystem::create_directories(d);
    return d.string();
}

std::shared_ptr<const std::map<std::string, EffectiveSubscription>>
makeDesired(const std::string& topic, uint8_t qos, bool mandatory) {
    auto m = std::make_shared<std::map<std::string, EffectiveSubscription>>();
    EffectiveSubscription eff;
    eff.expanded_topic = topic;
    eff.topic_template = topic;
    eff.effective_qos = qos;
    eff.mandatory = mandatory;
    eff.owners.insert("tsp");
    eff.targets.push_back({"tsp", "tsp.x", "r1"});
    (*m)[topic] = eff;
    return m;
}
}

TEST(BrokerSubscriptionReconcilerTest, SessionNotPresentFullSubscribe) {
    FakeBrokerOps ops;
    BrokerSubscriptionReconciler rec;
    rec.setBrokerOperations(&ops);
    auto desired = makeDesired("vehicle/x", 1, true);
    auto r = rec.reconcile(desired, false, "c_abc", 1);
    EXPECT_FALSE(r.session_present);
    EXPECT_EQ(r.subscribe_count, 1);
    EXPECT_EQ(ops.subscribes.size(), 1u);
    EXPECT_FALSE(rec.isConverged());
    rec.onSubAck("vehicle/x", true);
    EXPECT_TRUE(rec.isConverged());
    EXPECT_EQ(rec.mandatoryActive(), 1u);
}

TEST(BrokerSubscriptionReconcilerTest, SessionPresentManifestMatchNoResubscribe) {
    FakeBrokerOps ops;
    auto dir = mkStore();
    auto store = std::make_shared<hwyz::store::Store>(hwyz::store::Store::open("mqtt", dir));
    AppliedSubscriptionManifestStore ms;
    ms.setStore(store);
    AppliedSubscriptionManifest m;
    m.client_id_hash = "c_abc";
    m.identity_generation = 1;
    m.active_topics = {{"vehicle/x", 1}};
    ms.save(m);

    BrokerSubscriptionReconciler rec;
    rec.setBrokerOperations(&ops);
    rec.setManifestStore(&ms);
    auto desired = makeDesired("vehicle/x", 1, true);
    auto r = rec.reconcile(desired, true, "c_abc", 1);
    EXPECT_TRUE(r.manifest_valid);
    EXPECT_EQ(r.subscribe_count, 0);
    EXPECT_TRUE(rec.isConverged());
    std::filesystem::remove_all(dir);
}

TEST(BrokerSubscriptionReconcilerTest, SessionPresentIdentityMismatchFullSubscribe) {
    FakeBrokerOps ops;
    auto dir = mkStore();
    auto store = std::make_shared<hwyz::store::Store>(hwyz::store::Store::open("mqtt", dir));
    AppliedSubscriptionManifestStore ms;
    ms.setStore(store);
    AppliedSubscriptionManifest m;
    m.client_id_hash = "c_old";
    m.identity_generation = 1;
    m.active_topics = {{"vehicle/x", 1}};
    ms.save(m);

    BrokerSubscriptionReconciler rec;
    rec.setBrokerOperations(&ops);
    rec.setManifestStore(&ms);
    auto desired = makeDesired("vehicle/x", 1, true);
    auto r = rec.reconcile(desired, true, "c_new", 2);
    EXPECT_FALSE(r.manifest_valid);
    EXPECT_EQ(r.subscribe_count, 1);
    std::filesystem::remove_all(dir);
}

TEST(BrokerSubscriptionReconcilerTest, SubackFailureRetriesUpToMax) {
    FakeBrokerOps ops;
    BrokerSubscriptionReconciler rec;
    rec.setBrokerOperations(&ops);
    rec.setSubackRetryMax(2);
    auto desired = makeDesired("vehicle/x", 1, true);
    rec.reconcile(desired, false, "c_abc", 1);
    EXPECT_EQ(ops.subscribes.size(), 1u);
    rec.onSubAck("vehicle/x", false);
    EXPECT_EQ(ops.subscribes.size(), 2u);
    rec.onSubAck("vehicle/x", false);
    EXPECT_EQ(ops.subscribes.size(), 3u);
    rec.onSubAck("vehicle/x", false);
    EXPECT_EQ(ops.subscribes.size(), 3u);  // 超过 max 不再重试
    EXPECT_FALSE(rec.isConverged());
}

TEST(BrokerSubscriptionReconcilerTest, UnsubscribeWhenTopicRemoved) {
    FakeBrokerOps ops;
    BrokerSubscriptionReconciler rec;
    rec.setBrokerOperations(&ops);
    auto desired1 = std::make_shared<std::map<std::string, EffectiveSubscription>>();
    EffectiveSubscription effX;
    effX.expanded_topic = "vehicle/x"; effX.topic_template = "vehicle/x";
    effX.effective_qos = 1; effX.mandatory = false; effX.owners.insert("tsp");
    EffectiveSubscription effY;
    effY.expanded_topic = "vehicle/y"; effY.topic_template = "vehicle/y";
    effY.effective_qos = 1; effY.mandatory = true; effY.owners.insert("tsp");
    (*desired1)["vehicle/x"] = effX;
    (*desired1)["vehicle/y"] = effY;
    rec.reconcile(desired1, false, "c", 1);
    rec.onSubAck("vehicle/x", true);
    rec.onSubAck("vehicle/y", true);

    auto desired2 = std::make_shared<std::map<std::string, EffectiveSubscription>>();
    (*desired2)["vehicle/y"] = effY;
    auto r = rec.reconcile(desired2, true, "c", 1);
    EXPECT_EQ(r.unsubscribe_count, 1);
    EXPECT_FALSE(ops.unsubscribes.empty());
}

TEST(BrokerSubscriptionReconcilerTest, QosChangeTriggersResubscribe) {
    FakeBrokerOps ops;
    BrokerSubscriptionReconciler rec;
    rec.setBrokerOperations(&ops);
    auto desired1 = makeDesired("vehicle/x", 1, true);
    rec.reconcile(desired1, false, "c", 1);
    rec.onSubAck("vehicle/x", true);

    auto desired2 = makeDesired("vehicle/x", 2, true);  // QoS 升级
    auto r = rec.reconcile(desired2, true, "c", 1);
    EXPECT_EQ(r.subscribe_count, 1);
}

TEST(BrokerSubscriptionReconcilerTest, ActiveTopicsForManifest) {
    FakeBrokerOps ops;
    BrokerSubscriptionReconciler rec;
    rec.setBrokerOperations(&ops);
    auto desired = makeDesired("vehicle/x", 1, true);
    rec.reconcile(desired, false, "c", 1);
    rec.onSubAck("vehicle/x", true);
    auto active = rec.activeTopics();
    EXPECT_EQ(active.size(), 1u);
    EXPECT_EQ(active["vehicle/x"], 1u);
}
