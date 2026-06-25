#pragma once

#include "mqtt_types.h"
#include <functional>
#include <chrono>
#include <mutex>
#include <random>

namespace tbox {
namespace mqtt {

class ConnectionStateMachine {
public:
    using StateChangeCallback = std::function<void(ConnectionState)>;

    ConnectionStateMachine();

    void setStateChangeCallback(StateChangeCallback callback);
    void start();
    ConnectionState getCurrentState() const;
    ConnectionStateInfo getStateInfo() const;
    void setMaxRetries(uint32_t max_retries);

    void onCertificateReady();
    void onConnectionSuccess();
    void onConnectionFailure(const std::string& error);
    void onRetryTimeout();
    void onNetworkRestored();
    void onDisconnect();

private:
    void transitionTo(ConnectionState new_state);
    void calculateNextRetry();

    mutable std::mutex mutex_;
    ConnectionStateInfo state_info_;
    StateChangeCallback callback_;
    uint32_t max_retries_ = 10;
    std::chrono::milliseconds base_retry_delay_{1000};
    std::chrono::milliseconds max_retry_delay_{30000};
    std::mt19937 rng_{std::random_device{}()};
};

} // namespace mqtt
} // namespace tbox
