#include <gtest/gtest.h>
#include "sec_cert_interface.h"

using namespace tbox::mqtt;

TEST(SecCertInterfaceTest, InitialStateNotReady) {
    SecCertInterface sec;
    EXPECT_FALSE(sec.isCertificateReady());
}

TEST(SecCertInterfaceTest, GetCertHandleWhenNotReady) {
    SecCertInterface sec;
    auto result = sec.getCertHandle();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.error, "Certificate not ready");
    EXPECT_TRUE(result.cert_chain.empty());
    EXPECT_EQ(result.key_handle, nullptr);
}

TEST(SecCertInterfaceTest, SimulateCertificateReady) {
    SecCertInterface sec;
    EXPECT_FALSE(sec.isCertificateReady());

    sec.simulateCertificateReady();
    EXPECT_TRUE(sec.isCertificateReady());
}

TEST(SecCertInterfaceTest, GetCertHandleWhenReady) {
    SecCertInterface sec;
    sec.simulateCertificateReady();

    auto result = sec.getCertHandle();
    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.error.empty());
    EXPECT_FALSE(result.cert_chain.empty());
    EXPECT_NE(result.key_handle, nullptr);
}

TEST(SecCertInterfaceTest, GetCertificateChain) {
    SecCertInterface sec;
    sec.simulateCertificateReady();

    auto cert_chain = sec.getCertificateChain();
    EXPECT_FALSE(cert_chain.empty());
    EXPECT_EQ(cert_chain.size(), 4u);
    EXPECT_EQ(cert_chain[0], 0x30);
}
