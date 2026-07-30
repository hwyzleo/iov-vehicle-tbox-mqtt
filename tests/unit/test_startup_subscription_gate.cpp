// CR-007 §13: StartupSubscriptionGate 单元测试
#include <gtest/gtest.h>
#include "startup_subscription_gate.h"
#include <chrono>
#include <thread>

using namespace tbox::mqtt;

TEST(StartupSubscriptionGateTest, SatisfiedWhenAllRequiredComplete) {
    StartupSubscriptionGate gate;
    gate.configure({"tsp"}, Duration(5000));
    gate.startWait();
    EXPECT_FALSE(gate.isSatisfied());
    gate.onSnapshotAccepted("tsp", true);
    EXPECT_TRUE(gate.isSatisfied());
    EXPECT_TRUE(gate.missingOwners().empty());
}

TEST(StartupSubscriptionGateTest, NotSatisfiedWhenIncomplete) {
    StartupSubscriptionGate gate;
    gate.configure({"tsp"}, Duration(5000));
    gate.startWait();
    gate.onSnapshotAccepted("tsp", false);
    EXPECT_FALSE(gate.isSatisfied());
    EXPECT_EQ(gate.missingOwners().size(), 1u);
}

TEST(StartupSubscriptionGateTest, TimeoutWhenNotSatisfied) {
    StartupSubscriptionGate gate;
    gate.configure({"tsp"}, Duration(50));
    gate.startWait();
    std::this_thread::sleep_for(Duration(60));
    EXPECT_TRUE(gate.isTimedOut());
}

TEST(StartupSubscriptionGateTest, NoTimeoutWhenSatisfied) {
    StartupSubscriptionGate gate;
    gate.configure({"tsp"}, Duration(50));
    gate.startWait();
    gate.onSnapshotAccepted("tsp", true);
    std::this_thread::sleep_for(Duration(60));
    EXPECT_FALSE(gate.isTimedOut());
}

TEST(StartupSubscriptionGateTest, MultipleRequiredOwners) {
    StartupSubscriptionGate gate;
    gate.configure({"tsp", "diag"}, Duration(5000));
    gate.startWait();
    gate.onSnapshotAccepted("tsp", true);
    EXPECT_FALSE(gate.isSatisfied());
    gate.onSnapshotAccepted("diag", true);
    EXPECT_TRUE(gate.isSatisfied());
}

TEST(StartupSubscriptionGateTest, NonRequiredOwnerIgnored) {
    StartupSubscriptionGate gate;
    gate.configure({"tsp"}, Duration(5000));
    gate.startWait();
    gate.onSnapshotAccepted("other", true);
    EXPECT_FALSE(gate.isSatisfied());
}

TEST(StartupSubscriptionGateTest, LateSnapshotAfterTimeoutSatisfies) {
    StartupSubscriptionGate gate;
    gate.configure({"tsp"}, Duration(50));
    gate.startWait();
    std::this_thread::sleep_for(Duration(60));
    EXPECT_TRUE(gate.isTimedOut());
    // 迟到快照到达后满足
    gate.onSnapshotAccepted("tsp", true);
    EXPECT_TRUE(gate.isSatisfied());
    EXPECT_FALSE(gate.isTimedOut());
}
