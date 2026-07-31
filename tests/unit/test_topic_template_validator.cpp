// CR-009 §13.1: TopicTemplateValidator 单元测试
#include <gtest/gtest.h>
#include "topic_template_validator.h"

using namespace tbox::mqtt;

// ---- 合法模板 ----
TEST(TopicTemplateValidatorTest, ValidEcuUidTemplate) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid}/up/fota");
    EXPECT_TRUE(r.ok);
}

TEST(TopicTemplateValidatorTest, ValidDownlinkTemplate) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid}/down/fota");
    EXPECT_TRUE(r.ok);
}

TEST(TopicTemplateValidatorTest, ValidNoVariable) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/status/info");
    EXPECT_TRUE(r.ok);
}

TEST(TopicTemplateValidatorTest, ValidMultipleEcuUid) {
    TopicTemplateValidator v;
    auto r = v.validate("{ecu_uid}/{ecu_uid}/data");
    EXPECT_TRUE(r.ok);
}

// ---- 未知变量 ----
TEST(TopicTemplateValidatorTest, UnknownVariable) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{vin}/up/fota");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "unknown_variable");
}

TEST(TopicTemplateValidatorTest, UnknownVariableSn) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{sn}/data");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "unknown_variable");
}

// ---- 未闭合括号 ----
TEST(TopicTemplateValidatorTest, UnclosedBracket) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid/up/fota");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "unclosed_bracket");
}

// ---- 孤立闭合括号 ----
TEST(TopicTemplateValidatorTest, UnmatchedBracket) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/ecu_uid}/up");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "unmatched_bracket");
}

// ---- 空变量名 ----
TEST(TopicTemplateValidatorTest, EmptyVariableName) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{}/up/fota");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "empty_variable_name");
}

// ---- 嵌套变量 ----
TEST(TopicTemplateValidatorTest, NestedVariable) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{{ecu_uid}}/up");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "nested_variable");
}

// ---- 不支持的表达式语法 ----
TEST(TopicTemplateValidatorTest, DefaultValueSyntax) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid:default}/up");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "unsupported_expression");
}

TEST(TopicTemplateValidatorTest, FunctionSyntax) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid|upper}/up");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "unsupported_expression");
}

// ---- NUL 字节 ----
TEST(TopicTemplateValidatorTest, NulByte) {
    TopicTemplateValidator v;
    auto r = v.validate(std::string("vehicle/\0/up", 10));
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "nul_byte");
}

// ---- 空模板 ----
TEST(TopicTemplateValidatorTest, EmptyTemplate) {
    TopicTemplateValidator v;
    auto r = v.validate("");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "empty_template");
}

// ---- 空层级 ----
TEST(TopicTemplateValidatorTest, EmptyLevelLeadingSlash) {
    TopicTemplateValidator v;
    auto r = v.validate("/vehicle/{ecu_uid}/up");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "empty_level");
}

TEST(TopicTemplateValidatorTest, EmptyLevelTrailingSlash) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid}/up/");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "empty_level");
}

TEST(TopicTemplateValidatorTest, EmptyLevelDoubleSlash) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle//{ecu_uid}/up");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "empty_level");
}

// ---- 通配符约束 ----
TEST(TopicTemplateValidatorTest, WildcardInDeviceKeyLayer) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid}+/up");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "wildcard_in_device_key_layer");
}

TEST(TopicTemplateValidatorTest, ValidSingleLevelWildcard) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid}/+/fota");
    EXPECT_TRUE(r.ok);
}

TEST(TopicTemplateValidatorTest, ValidMultiLevelWildcardLast) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid}/#");
    EXPECT_TRUE(r.ok);
}

TEST(TopicTemplateValidatorTest, InvalidMultiLevelWildcardNotLast) {
    TopicTemplateValidator v;
    auto r = v.validate("vehicle/{ecu_uid}/#/extra");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "invalid_multi_level_wildcard");
}

// ---- 展开后校验 ----
TEST(TopicTemplateValidatorTest, ValidExpandedTopic) {
    TopicTemplateValidator v;
    auto r = v.validateExpanded("vehicle/ECU123/up/fota");
    EXPECT_TRUE(r.ok);
}

TEST(TopicTemplateValidatorTest, ExpandedWithUnresolvedVariable) {
    TopicTemplateValidator v;
    auto r = v.validateExpanded("vehicle/{ecu_uid}/up/fota");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "unresolved_variable");
}

TEST(TopicTemplateValidatorTest, ExpandedEmptyLevel) {
    TopicTemplateValidator v;
    auto r = v.validateExpanded("vehicle//up/fota");
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "empty_level");
}

// ---- 变量解析 ----
TEST(TopicTemplateValidatorTest, ParseVariables) {
    TopicTemplateValidator v;
    auto vars = v.parseVariables("vehicle/{ecu_uid}/up/{ecu_uid}");
    ASSERT_EQ(vars.size(), 1u);  // 去重
    EXPECT_EQ(vars[0].name, "ecu_uid");
}

TEST(TopicTemplateValidatorTest, ParseVariablesNoVariable) {
    TopicTemplateValidator v;
    auto vars = v.parseVariables("vehicle/status/info");
    EXPECT_TRUE(vars.empty());
}

// ---- 长度边界 ----
TEST(TopicTemplateValidatorTest, VeryLongTemplate) {
    TopicTemplateValidator v;
    std::string long_template(70000, 'a');
    auto r = v.validate(long_template);
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.reason_code, "template_too_long");
}
