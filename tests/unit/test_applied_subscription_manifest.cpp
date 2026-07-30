// CR-007 §13: AppliedSubscriptionManifest 单元测试
#include <gtest/gtest.h>
#include "applied_subscription_manifest.h"
#include "store.h"
#include <filesystem>

using namespace tbox::mqtt;

namespace {
std::string mkStore() {
    auto d = std::filesystem::temp_directory_path() / "tbox_mqtt_manifest_test";
    std::filesystem::create_directories(d);
    return d.string();
}
void rmStore(const std::string& p) { std::filesystem::remove_all(p); }
}

TEST(AppliedSubscriptionManifestTest, SaveLoadRoundTrip) {
    auto dir = mkStore();
    auto store = std::make_shared<hwyz::store::Store>(hwyz::store::Store::open("mqtt", dir));
    AppliedSubscriptionManifestStore ms;
    ms.setStore(store);
    AppliedSubscriptionManifest m;
    m.client_id_hash = "c_abc";
    m.identity_generation = 5;
    m.active_topics = {{"vehicle/x", 1}, {"vehicle/y", 2}};
    m.broker_manifest_version = 3;
    EXPECT_TRUE(ms.save(m));
    auto loaded = ms.load();
    EXPECT_EQ(loaded.client_id_hash, "c_abc");
    EXPECT_EQ(loaded.identity_generation, 5u);
    EXPECT_EQ(loaded.active_topics.size(), 2u);
    EXPECT_EQ(loaded.active_topics["vehicle/x"], 1u);
    EXPECT_EQ(loaded.broker_manifest_version, 3u);
    rmStore(dir);
}

TEST(AppliedSubscriptionManifestTest, IdentityMatch) {
    auto dir = mkStore();
    auto store = std::make_shared<hwyz::store::Store>(hwyz::store::Store::open("mqtt", dir));
    AppliedSubscriptionManifestStore ms;
    ms.setStore(store);
    AppliedSubscriptionManifest m;
    m.client_id_hash = "c_abc";
    m.identity_generation = 5;
    m.active_topics = {{"x", 1}};
    ms.save(m);
    EXPECT_TRUE(ms.isIdentityMatch("c_abc", 5));
    EXPECT_FALSE(ms.isIdentityMatch("c_other", 5));
    EXPECT_FALSE(ms.isIdentityMatch("c_abc", 6));
    rmStore(dir);
}

TEST(AppliedSubscriptionManifestTest, Clear) {
    auto dir = mkStore();
    auto store = std::make_shared<hwyz::store::Store>(hwyz::store::Store::open("mqtt", dir));
    AppliedSubscriptionManifestStore ms;
    ms.setStore(store);
    AppliedSubscriptionManifest m;
    m.client_id_hash = "c_abc";
    m.active_topics = {{"x", 1}};
    ms.save(m);
    ms.clear();
    auto loaded = ms.load();
    EXPECT_TRUE(loaded.active_topics.empty());
    EXPECT_FALSE(ms.isIdentityMatch("c_abc", 0));
    rmStore(dir);
}

TEST(AppliedSubscriptionManifestTest, CorruptReturnsEmpty) {
    auto dir = mkStore();
    auto store = std::make_shared<hwyz::store::Store>(hwyz::store::Store::open("mqtt", dir));
    store->save<std::string>(StoreKeys::kSubsManifest, "{not valid json");
    AppliedSubscriptionManifestStore ms;
    ms.setStore(store);
    auto loaded = ms.load();
    EXPECT_TRUE(loaded.active_topics.empty());
    rmStore(dir);
}

TEST(AppliedSubscriptionManifestTest, MissingReturnsEmpty) {
    auto dir = mkStore();
    auto store = std::make_shared<hwyz::store::Store>(hwyz::store::Store::open("mqtt", dir));
    AppliedSubscriptionManifestStore ms;
    ms.setStore(store);
    auto loaded = ms.load();
    EXPECT_TRUE(loaded.active_topics.empty());
    EXPECT_FALSE(ms.isIdentityMatch("c", 0));
    rmStore(dir);
}
