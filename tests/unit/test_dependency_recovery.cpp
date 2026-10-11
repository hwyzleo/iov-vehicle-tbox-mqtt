//
// TBOX-MQTT-DSN-CR-016 §6: 依赖恢复退避与计数工具单元测试
//

#include <gtest/gtest.h>

#include "dependency_recovery.h"

#include <random>

using namespace tbox::mqtt;

TEST(DependencyRecoveryTest, NamesAreStable) {
    EXPECT_STREQ(dependencyName(Dependency::SEC), "sec");
    EXPECT_STREQ(dependencyName(Dependency::PROV), "prov");
    EXPECT_STREQ(ipcStateName(IpcState::DISCONNECTED), "DISCONNECTED");
    EXPECT_STREQ(ipcStateName(IpcState::CONNECTING), "CONNECTING");
    EXPECT_STREQ(ipcStateName(IpcState::CONNECTED), "CONNECTED");
}

TEST(DependencyRecoveryTest, RetryStateDefaults) {
    RetryState rs;
    EXPECT_EQ(rs.cycle_id, 0u);
    EXPECT_EQ(rs.retry_count, 0u);
    EXPECT_EQ(rs.total_attempts, 0u);
    EXPECT_EQ(rs.next_backoff.count(), 0);
    EXPECT_EQ(rs.client_generation, 0u);
    EXPECT_FALSE(rs.timer_armed);
}

TEST(DependencyRecoveryTest, BackoffGrowsExponentiallyWithJitter) {
    std::mt19937 rng(12345);
    Duration initial(1000);
    Duration max_backoff(60000);

    auto b0 = computeDependencyBackoff(0, initial, max_backoff, rng);
    EXPECT_GE(b0.count(), 900);
    EXPECT_LE(b0.count(), 1100);

    auto b1 = computeDependencyBackoff(1, initial, max_backoff, rng);
    EXPECT_GE(b1.count(), 1800);
    EXPECT_LE(b1.count(), 2200);

    auto b2 = computeDependencyBackoff(2, initial, max_backoff, rng);
    EXPECT_GE(b2.count(), 3600);
    EXPECT_LE(b2.count(), 4400);
}

TEST(DependencyRecoveryTest, BackoffIsCappedAfterMax) {
    std::mt19937 rng(7);
    Duration initial(1000);
    Duration max_backoff(60000);

    // 达到上限后 backoff 保持 60000（含 ±10% jitter），retry_count 仍继续增长
    auto b6 = computeDependencyBackoff(6, initial, max_backoff, rng);
    auto b20 = computeDependencyBackoff(20, initial, max_backoff, rng);

    EXPECT_GE(b6.count(), 54000);
    EXPECT_LE(b6.count(), 66000);
    EXPECT_GE(b20.count(), 54000);
    EXPECT_LE(b20.count(), 66000);
}

TEST(DependencyRecoveryTest, BackoffRespectsConfiguredMaxBelowDefault) {
    std::mt19937 rng(99);
    Duration initial(50);
    Duration max_backoff(200);

    auto b0 = computeDependencyBackoff(0, initial, max_backoff, rng);
    EXPECT_LE(b0.count(), 220);

    auto b10 = computeDependencyBackoff(10, initial, max_backoff, rng);
    EXPECT_GE(b10.count(), 180);
    EXPECT_LE(b10.count(), 220);
}
