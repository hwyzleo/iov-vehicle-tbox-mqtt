#include <gtest/gtest.h>
#include "credential_manager.h"

using namespace tbox::mqtt;
using namespace tbox::sec;

TEST(CredentialManagerTest, InitialStateNotReady) {
    CredentialManager mgr("/tmp/nonexistent-sec.sock", "mqtt");
    mgr.simulateNotReady();
    EXPECT_FALSE(mgr.isCredentialReady());
}

TEST(CredentialManagerTest, SimulateReady) {
    CredentialManager mgr;
    mgr.simulateReady();
    EXPECT_TRUE(mgr.isCredentialReady());
}

TEST(CredentialManagerTest, GetTlsCredentialWhenReady) {
    CredentialManager mgr;
    mgr.simulateReady();

    ActiveTlsCredential cred;
    EXPECT_TRUE(mgr.getTlsCredential(cred));
    EXPECT_TRUE(cred.ready);
    EXPECT_EQ(cred.status, TlsCredentialStatus::READY);
    EXPECT_FALSE(cred.credential_id.empty());
    EXPECT_FALSE(cred.root_ca_bundle.empty());
    EXPECT_FALSE(cred.client_cert_chain.empty());
    EXPECT_FALSE(cred.private_key_ref.empty());
    EXPECT_EQ(cred.key_algorithm, KeyAlgorithm::ECDSA_P256);
}

TEST(CredentialManagerTest, GetTlsCredentialWhenNotReady) {
    CredentialManager mgr;
    mgr.simulateNotReady();

    ActiveTlsCredential cred;
    EXPECT_FALSE(mgr.getTlsCredential(cred));
    EXPECT_FALSE(cred.ready);
}

TEST(CredentialManagerTest, GetTlsCredentialState) {
    CredentialManager mgr;
    mgr.simulateReady();

    TlsCredentialState state;
    EXPECT_TRUE(mgr.getTlsCredentialState(state));
    EXPECT_EQ(state.status, TlsCredentialStatus::READY);
    EXPECT_FALSE(state.credential_id.empty());
}

TEST(CredentialManagerTest, SignTlsSimulated) {
    CredentialManager mgr;
    mgr.simulateReady();

    TlsSignRequest req;
    req.algorithm = SignatureAlgorithm::ECDSA_SECP256R1_SHA256;
    req.digest = {0xAB, 0xCD, 0xEF};

    std::vector<uint8_t> signature;
    EXPECT_TRUE(mgr.signTls(req, signature));
    EXPECT_FALSE(signature.empty());
}

TEST(CredentialManagerTest, SignTlsWithoutReady) {
    CredentialManager mgr;
    // Not simulated, not connected to SEC
    TlsSignRequest req;
    req.algorithm = SignatureAlgorithm::ECDSA_SECP256R1_SHA256;
    req.digest = {0xAB, 0xCD};

    std::vector<uint8_t> signature;
    EXPECT_FALSE(mgr.signTls(req, signature));
    EXPECT_FALSE(mgr.getLastError().empty());
}

TEST(CredentialManagerTest, GenerationIncrementsOnReady) {
    CredentialManager mgr;
    mgr.simulateNotReady();
    uint64_t gen1 = mgr.getGeneration();

    mgr.simulateReady();
    uint64_t gen2 = mgr.getGeneration();
    EXPECT_GT(gen2, gen1);
}

TEST(CredentialManagerTest, GenerationIncrementsOnInvalidate) {
    CredentialManager mgr;
    mgr.simulateReady();
    uint64_t gen1 = mgr.getGeneration();

    mgr.invalidateCredential();
    uint64_t gen2 = mgr.getGeneration();
    EXPECT_GT(gen2, gen1);
}

TEST(CredentialManagerTest, CleanupCredentialClearsBuffers) {
    CredentialManager mgr;
    mgr.simulateReady();

    auto snapshot = mgr.getSnapshot();
    EXPECT_FALSE(snapshot.root_ca_bundle.empty());
    EXPECT_FALSE(snapshot.private_key_ref.empty());

    mgr.cleanupCredential();
    snapshot = mgr.getSnapshot();
    EXPECT_TRUE(snapshot.root_ca_bundle.empty());
    EXPECT_TRUE(snapshot.client_cert_chain.empty());
    EXPECT_TRUE(snapshot.private_key_ref.empty());
    EXPECT_FALSE(snapshot.ready);
}

TEST(CredentialManagerTest, SnapshotIsThreadSafe) {
    CredentialManager mgr;
    mgr.simulateReady();

    auto snap1 = mgr.getSnapshot();
    EXPECT_TRUE(snap1.ready);

    mgr.cleanupCredential();
    // snap1 should still have old data (it's a copy)
    EXPECT_TRUE(snap1.ready);

    auto snap2 = mgr.getSnapshot();
    EXPECT_FALSE(snap2.ready);
}

TEST(CredentialManagerTest, SubscribeCallbackSimulated) {
    CredentialManager mgr;
    mgr.simulateReady();

    bool callback_called = false;
    mgr.subscribeTlsCredentialChanged([&callback_called]() {
        callback_called = true;
    });

    mgr.simulateStateChanged();
    EXPECT_TRUE(callback_called);
}

TEST(CredentialManagerTest, ProfileAccessor) {
    CredentialManager mgr("/tmp/sec.sock", "mqtt");
    EXPECT_EQ(mgr.getProfile(), "mqtt");
}

TEST(CredentialManagerTest, SecConnectedInSimulatedMode) {
    CredentialManager mgr;
    mgr.simulateReady();
    EXPECT_TRUE(mgr.isSecConnected());
}

TEST(CredentialManagerTest, ErrorMappingSEC1010) {
    EXPECT_EQ(CredentialManager::secErrorToMqttEvent(tbox::sec::ErrorCode::TLS_CREDENTIAL_NOT_READY),
              "mqtt.certificate.waiting");
    EXPECT_EQ(CredentialManager::secErrorToReasonCode(tbox::sec::ErrorCode::TLS_CREDENTIAL_NOT_READY),
              "SEC-1010");
}

TEST(CredentialManagerTest, ErrorMappingSEC1011) {
    EXPECT_EQ(CredentialManager::secErrorToMqttEvent(tbox::sec::ErrorCode::TLS_CREDENTIAL_INVALID),
              "mqtt.credential.invalid");
    EXPECT_EQ(CredentialManager::secErrorToReasonCode(tbox::sec::ErrorCode::TLS_CREDENTIAL_INVALID),
              "SEC-1011");
}

TEST(CredentialManagerTest, ErrorMappingSEC1012) {
    EXPECT_EQ(CredentialManager::secErrorToMqttEvent(tbox::sec::ErrorCode::TLS_KEY_REF_INVALID),
              "mqtt.credential.reference_expired");
    EXPECT_EQ(CredentialManager::secErrorToReasonCode(tbox::sec::ErrorCode::TLS_KEY_REF_INVALID),
              "SEC-1012");
}

TEST(CredentialManagerTest, ErrorMappingSEC1013) {
    EXPECT_EQ(CredentialManager::secErrorToMqttEvent(tbox::sec::ErrorCode::TLS_SIGN_ALGORITHM_NOT_ALLOWED),
              "mqtt.tls.algorithm_rejected");
    EXPECT_EQ(CredentialManager::secErrorToReasonCode(tbox::sec::ErrorCode::TLS_SIGN_ALGORITHM_NOT_ALLOWED),
              "SEC-1013");
}

TEST(CredentialManagerTest, ErrorMappingSEC1014) {
    EXPECT_EQ(CredentialManager::secErrorToMqttEvent(tbox::sec::ErrorCode::TLS_HSM_SIGN_FAILED),
              "mqtt.tls.sign_failed");
    EXPECT_EQ(CredentialManager::secErrorToReasonCode(tbox::sec::ErrorCode::TLS_HSM_SIGN_FAILED),
              "SEC-1014");
}

TEST(CredentialManagerTest, SimulateReadyWithCustomCredential) {
    CredentialManager mgr;
    ActiveTlsCredential custom;
    custom.credential_id = "custom-cred";
    custom.version = 42;
    custom.status = TlsCredentialStatus::READY;
    custom.root_ca_bundle = {0x01, 0x02, 0x03};
    custom.client_cert_chain = {0x04, 0x05, 0x06};
    custom.private_key_ref.data = {0x07, 0x08};
    custom.key_algorithm = KeyAlgorithm::ECDSA_P256;
    custom.ready = true;

    mgr.simulateReady(custom);

    auto snap = mgr.getSnapshot();
    EXPECT_EQ(snap.credential_id, "custom-cred");
    EXPECT_EQ(snap.version, 42u);
    EXPECT_TRUE(snap.ready);
}
