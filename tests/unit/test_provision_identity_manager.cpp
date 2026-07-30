#include <gtest/gtest.h>
#include "provision_identity_manager.h"
#include "mqtt_context.h"

using namespace tbox::mqtt;
using namespace tbox::prov;

// ============================================================
// CR-006 §5: PROV 绑定前置与校验
// ============================================================

TEST(ProvisionIdentityManagerTest, InitialStateNotValid) {
    ProvisionIdentityManager mgr;
    mgr.simulateUnbound();
    EXPECT_FALSE(mgr.isIdentityValid());
    EXPECT_FALSE(mgr.isBound());
}

TEST(ProvisionIdentityManagerTest, SimulateBindingValid) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "00000001AA00000001";
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    snapshot.locked = true;
    mgr.simulateBinding(snapshot);

    EXPECT_TRUE(mgr.isIdentityValid());
    EXPECT_TRUE(mgr.isBound());
}

TEST(ProvisionIdentityManagerTest, ReadBindingReturnsTrueWhenBound) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "00000001AA00000001";
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    EXPECT_TRUE(mgr.readBinding());
}

TEST(ProvisionIdentityManagerTest, ReadBindingReturnsFalseWhenNotBound) {
    ProvisionIdentityManager mgr;
    mgr.simulateUnbound();

    EXPECT_FALSE(mgr.readBinding());
}

TEST(ProvisionIdentityManagerTest, ReadBindingReturnsFalseWhenVinEmpty) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "";  // empty VIN
    snapshot.ecu_uid = "00000001AA00000001";
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    EXPECT_FALSE(mgr.readBinding());
}

TEST(ProvisionIdentityManagerTest, ReadBindingReturnsFalseWhenSnEmpty) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "00000001AA00000001";
    snapshot.sn = "";  // empty SN
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    EXPECT_FALSE(mgr.readBinding());
}

TEST(ProvisionIdentityManagerTest, ReadBindingReturnsFalseWhenEcuUidEmpty) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "";  // empty ECU UID
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    EXPECT_FALSE(mgr.readBinding());
}

TEST(ProvisionIdentityManagerTest, ReadBindingReturnsFalseWhenStateNotBound) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "00000001AA00000001";
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::VIN_WRITTEN;  // not BOUND
    mgr.simulateBinding(snapshot);

    EXPECT_FALSE(mgr.readBinding());
}

// ============================================================
// CR-006 §6.1: ClientIdBuilder
// ============================================================

TEST(ProvisionIdentityManagerTest, BuildClientIdFromVinAndSn) {
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}-{sn}");

    std::string client_id = mgr.buildClientId("HWYZTEST900000001", "TBOX00000001");
    EXPECT_EQ(client_id, "HWYZTEST900000001-TBOX00000001");
}

TEST(ProvisionIdentityManagerTest, BuildClientIdReturnsEmptyWhenVinEmpty) {
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}-{sn}");

    EXPECT_TRUE(mgr.buildClientId("", "TBOX00000001").empty());
}

TEST(ProvisionIdentityManagerTest, BuildClientIdReturnsEmptyWhenSnEmpty) {
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}-{sn}");

    EXPECT_TRUE(mgr.buildClientId("HWYZTEST900000001", "").empty());
}

TEST(ProvisionIdentityManagerTest, BuildClientIdDoesNotUseEcuUid) {
    // CR-006 §6.1: 不得引用 ecu_uid
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}-{sn}");

    std::string client_id = mgr.buildClientId("VIN123", "SN456");
    EXPECT_EQ(client_id, "VIN123-SN456");
    // client_id should NOT contain any ecu_uid
    EXPECT_EQ(client_id.find("ecu"), std::string::npos);
}

TEST(ProvisionIdentityManagerTest, BuildClientIdWithCustomFormat) {
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}_{sn}_mqtt");

    std::string client_id = mgr.buildClientId("VIN123", "SN456");
    EXPECT_EQ(client_id, "VIN123_SN456_mqtt");
}

TEST(ProvisionIdentityManagerTest, BuildClientIdRejectsUnresolvedPlaceholders) {
    // CR-006 §6.1: format with unresolved placeholders should fail
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}-{sn}-{ecu_uid}");

    std::string client_id = mgr.buildClientId("VIN123", "SN456");
    EXPECT_TRUE(client_id.empty());
}

// ============================================================
// CR-006 §6.2: TopicResolver
// ============================================================

TEST(ProvisionIdentityManagerTest, ResolveTopicWithEcuUid) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "ECU123456789ABCD";
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    std::string topic = mgr.resolveTopic("vehicle/{ecu_uid}/up/fota");
    EXPECT_EQ(topic, "vehicle/ECU123456789ABCD/up/fota");
}

TEST(ProvisionIdentityManagerTest, ResolveTopicReturnsEmptyWhenNoEcuUid) {
    ProvisionIdentityManager mgr;
    mgr.simulateUnbound();

    EXPECT_TRUE(mgr.resolveTopic("vehicle/{ecu_uid}/up/fota").empty());
}

TEST(ProvisionIdentityManagerTest, ResolveTopicDoesNotUseVinOrSn) {
    // CR-006 §6.2: Topic device key must be ecu_uid, not VIN or SN
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "VIN12345678901234";
    snapshot.ecu_uid = "ECU123456789ABCD";
    snapshot.sn = "SN45678901234";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    std::string topic = mgr.resolveTopic("vehicle/{ecu_uid}/up/fota");
    EXPECT_NE(topic.find("ECU123456789ABCD"), std::string::npos);
    // Topic should NOT contain VIN or SN
    EXPECT_EQ(topic.find("VIN12345678901234"), std::string::npos);
    EXPECT_EQ(topic.find("SN45678901234"), std::string::npos);
}

// ============================================================
// CR-006 §3, §7: Generation 与身份变化
// ============================================================

TEST(ProvisionIdentityManagerTest, GenerationIncrementsOnBindingChange) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snap1;
    snap1.vin = "VIN001";
    snap1.ecu_uid = "ECU001";
    snap1.sn = "SN001";
    snap1.state = ProvisionState::BOUND;
    mgr.simulateBinding(snap1);
    uint64_t gen1 = mgr.getGeneration();

    ProvisionIdentitySnapshot snap2;
    snap2.vin = "VIN002";  // VIN changed
    snap2.ecu_uid = "ECU001";
    snap2.sn = "SN001";
    snap2.state = ProvisionState::BOUND;
    mgr.simulateBinding(snap2);
    uint64_t gen2 = mgr.getGeneration();

    EXPECT_GT(gen2, gen1);
}

TEST(ProvisionIdentityManagerTest, GenerationIncrementsOnEcuUidChange) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snap1;
    snap1.vin = "VIN001";
    snap1.ecu_uid = "ECU001";
    snap1.sn = "SN001";
    snap1.state = ProvisionState::BOUND;
    mgr.simulateBinding(snap1);
    uint64_t gen1 = mgr.getGeneration();

    ProvisionIdentitySnapshot snap2;
    snap2.vin = "VIN001";
    snap2.ecu_uid = "ECU002";  // ECU UID changed
    snap2.sn = "SN001";
    snap2.state = ProvisionState::BOUND;
    mgr.simulateBinding(snap2);
    uint64_t gen2 = mgr.getGeneration();

    EXPECT_GT(gen2, gen1);
}

TEST(ProvisionIdentityManagerTest, GenerationIncrementsOnStateChange) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snap1;
    snap1.vin = "VIN001";
    snap1.ecu_uid = "ECU001";
    snap1.sn = "SN001";
    snap1.state = ProvisionState::BOUND;
    mgr.simulateBinding(snap1);
    uint64_t gen1 = mgr.getGeneration();

    mgr.simulateUnbound();  // state changed to NONE
    uint64_t gen2 = mgr.getGeneration();

    EXPECT_GT(gen2, gen1);
}

TEST(ProvisionIdentityManagerTest, HasIdentityChangedSinceGeneration) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snap1;
    snap1.vin = "VIN001";
    snap1.ecu_uid = "ECU001";
    snap1.sn = "SN001";
    snap1.state = ProvisionState::BOUND;
    mgr.simulateBinding(snap1);
    uint64_t gen = mgr.getGeneration();

    EXPECT_FALSE(mgr.hasIdentityChanged(gen));

    ProvisionIdentitySnapshot snap2;
    snap2.vin = "VIN002";  // changed
    snap2.ecu_uid = "ECU001";
    snap2.sn = "SN001";
    snap2.state = ProvisionState::BOUND;
    mgr.simulateBinding(snap2);

    EXPECT_TRUE(mgr.hasIdentityChanged(gen));
}

// ============================================================
// CR-006 §3: Snapshot 访问
// ============================================================

TEST(ProvisionIdentityManagerTest, GetSnapshotReturnsCurrentIdentity) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "HWYZTEST900000001";
    snapshot.ecu_uid = "00000001AA00000001";
    snapshot.sn = "TBOX00000001";
    snapshot.state = ProvisionState::BOUND;
    snapshot.locked = true;
    mgr.simulateBinding(snapshot);

    auto current = mgr.getSnapshot();
    EXPECT_EQ(current.vin, "HWYZTEST900000001");
    EXPECT_EQ(current.ecu_uid, "00000001AA00000001");
    EXPECT_EQ(current.sn, "TBOX00000001");
    EXPECT_EQ(current.state, ProvisionState::BOUND);
    EXPECT_TRUE(current.locked);
    EXPECT_FALSE(current.client_id.empty());
    EXPECT_EQ(current.client_id, "HWYZTEST900000001-TBOX00000001");
}

TEST(ProvisionIdentityManagerTest, GetEcuUidReturnsCurrentEcuUid) {
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "VIN001";
    snapshot.ecu_uid = "ECU_ABC123";
    snapshot.sn = "SN001";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    EXPECT_EQ(mgr.getEcuUid(), "ECU_ABC123");
}

TEST(ProvisionIdentityManagerTest, SnapshotClientIdMatchesBuildClientId) {
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}_{sn}");

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "VIN123";
    snapshot.ecu_uid = "ECU123";
    snapshot.sn = "SN456";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    auto current = mgr.getSnapshot();
    EXPECT_EQ(current.client_id, "VIN123_SN456");
    EXPECT_EQ(current.client_id, mgr.buildClientId("VIN123", "SN456"));
}

// ============================================================
// CR-006 §6.3: 身份映射禁止互相兜底
// ============================================================

TEST(ProvisionIdentityManagerTest, ClientIdDoesNotFallbackToEcuUid) {
    // CR-006 §6.1: 不得以 ecu_uid 兜底缺失的 VIN 或 SN
    ProvisionIdentityManager mgr;
    mgr.setClientIdFormat("{vin}-{sn}");

    // Empty VIN -> empty client_id, no ecu_uid fallback
    EXPECT_TRUE(mgr.buildClientId("", "SN456").empty());
    // Empty SN -> empty client_id, no ecu_uid fallback
    EXPECT_TRUE(mgr.buildClientId("VIN123", "").empty());
}

TEST(ProvisionIdentityManagerTest, TopicDoesNotUseClientId) {
    // CR-006 §6.2: Topic device key must be ecu_uid, not client_id
    ProvisionIdentityManager mgr;

    ProvisionIdentitySnapshot snapshot;
    snapshot.vin = "VIN12345678901234";
    snapshot.ecu_uid = "ECU123456789ABCD";
    snapshot.sn = "SN45678901234";
    snapshot.state = ProvisionState::BOUND;
    mgr.simulateBinding(snapshot);

    std::string topic = mgr.resolveTopic("vehicle/{ecu_uid}/status");
    std::string client_id = mgr.getSnapshot().client_id;

    // Topic should contain ecu_uid, not client_id
    EXPECT_NE(topic.find("ECU123456789ABCD"), std::string::npos);
    EXPECT_EQ(topic.find(client_id), std::string::npos);
}
