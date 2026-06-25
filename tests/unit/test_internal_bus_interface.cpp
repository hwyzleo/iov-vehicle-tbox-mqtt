#include <gtest/gtest.h>
#include "internal_bus_interface.h"

using namespace tbox::mqtt;

TEST(InternalBusInterfaceTest, RegisterService) {
    InternalBusInterface bus;
    EXPECT_TRUE(bus.registerService("service_diag", "127.0.0.1:50051"));
}

TEST(InternalBusInterfaceTest, SendMessage) {
    InternalBusInterface bus;
    bus.registerService("service_diag", "127.0.0.1:50051");

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    EXPECT_TRUE(bus.sendMessage("service_diag", payload));
}

TEST(InternalBusInterfaceTest, SetMessageHandler) {
    InternalBusInterface bus;

    bool handler_called = false;
    bus.setMessageHandler("service_diag",
        [&handler_called](const std::string& service, const std::vector<uint8_t>& payload) {
            handler_called = true;
        });

    // Simulate incoming message
    std::vector<uint8_t> payload = {0x01, 0x02, 0x03};
    bus.simulateIncomingMessage("service_diag", payload);

    EXPECT_TRUE(handler_called);
}

TEST(InternalBusInterfaceTest, GetRegisteredServices) {
    InternalBusInterface bus;

    bus.registerService("service_diag", "127.0.0.1:50051");
    bus.registerService("service_control", "127.0.0.1:50052");

    auto services = bus.getRegisteredServices();
    EXPECT_EQ(services.size(), 2);
}

TEST(InternalBusInterfaceTest, UnregisterService) {
    InternalBusInterface bus;

    bus.registerService("service_diag", "127.0.0.1:50051");
    EXPECT_TRUE(bus.unregisterService("service_diag"));

    auto services = bus.getRegisteredServices();
    EXPECT_EQ(services.size(), 0);
}
