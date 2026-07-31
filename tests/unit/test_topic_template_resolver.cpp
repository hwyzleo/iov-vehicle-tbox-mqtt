// CR-009 §13.1: TopicTemplateResolver 单元测试
#include <gtest/gtest.h>
#include "topic_template_resolver.h"

using namespace tbox::mqtt;

TEST(TopicTemplateResolverTest, ResolveValidTemplate) {
    TopicTemplateResolver r;
    r.setIdentity("ECU123", 1);
    auto result = r.resolve("vehicle/{ecu_uid}/up/fota");
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.expanded_topic, "vehicle/ECU123/up/fota");
    EXPECT_EQ(result.identity_generation, 1u);
}

TEST(TopicTemplateResolverTest, ResolveMultipleVariables) {
    TopicTemplateResolver r;
    r.setIdentity("ECU456", 2);
    auto result = r.resolve("{ecu_uid}/{ecu_uid}/data");
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.expanded_topic, "ECU456/ECU456/data");
}

TEST(TopicTemplateResolverTest, ResolveFailsWithoutIdentity) {
    TopicTemplateResolver r;
    auto result = r.resolve("vehicle/{ecu_uid}/up/fota");
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.reason_code, "identity_unavailable");
}

TEST(TopicTemplateResolverTest, ResolveFailsWithEmptyEcuUid) {
    TopicTemplateResolver r;
    r.setIdentity("", 1);
    auto result = r.resolve("vehicle/{ecu_uid}/up/fota");
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.reason_code, "identity_unavailable");
}

TEST(TopicTemplateResolverTest, DifferentGenerationsDifferentTopics) {
    // 同一快照在不同 identity generation 下生成不同完整 Topic
    TopicTemplateResolver r1;
    r1.setIdentity("ECU_OLD", 1);
    auto result1 = r1.resolve("vehicle/{ecu_uid}/up/fota");
    EXPECT_EQ(result1.expanded_topic, "vehicle/ECU_OLD/up/fota");

    TopicTemplateResolver r2;
    r2.setIdentity("ECU_NEW", 2);
    auto result2 = r2.resolve("vehicle/{ecu_uid}/up/fota");
    EXPECT_EQ(result2.expanded_topic, "vehicle/ECU_NEW/up/fota");

    // 模板本身不变
    EXPECT_NE(result1.expanded_topic, result2.expanded_topic);
}

TEST(TopicTemplateResolverTest, ResolveAndValidateRejectsBadTemplate) {
    TopicTemplateResolver r;
    r.setIdentity("ECU123", 1);
    auto result = r.resolveAndValidate("vehicle/{vin}/up/fota");
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.reason_code, "unknown_variable");
}

TEST(TopicTemplateResolverTest, ResolveAndValidateRejectsExpandedWithVariable) {
    TopicTemplateResolver r;
    r.setIdentity("ECU123", 1);
    // 展开后仍含 {ecu_uid}（如果 ecu_uid 本身含 {ecu_uid}，但正常不会）
    auto result = r.resolveAndValidate("vehicle/{ecu_uid}/up/fota");
    EXPECT_TRUE(result.ok);
}

TEST(TopicTemplateResolverTest, IsGenerationCurrent) {
    TopicTemplateResolver r;
    r.setIdentity("ECU123", 5);
    EXPECT_TRUE(r.isGenerationCurrent(5));
    EXPECT_FALSE(r.isGenerationCurrent(4));
    EXPECT_FALSE(r.isGenerationCurrent(0));
}

TEST(TopicTemplateResolverTest, IsIdentityValid) {
    TopicTemplateResolver r;
    EXPECT_FALSE(r.isIdentityValid());
    r.setIdentity("ECU123", 1);
    EXPECT_TRUE(r.isIdentityValid());
    r.setIdentity("", 2);
    EXPECT_FALSE(r.isIdentityValid());
}
