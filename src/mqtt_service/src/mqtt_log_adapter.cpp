#include "mqtt_log_adapter.h"

namespace tbox {
namespace mqtt {

bool MqttLogAdapter::s_initialized = false;

tbox::fw::log::InitResult MqttLogAdapter::init(
    const std::string& service,
    const tbox::fw::log::LogConfig& config
) {
    auto result = tbox::fw::log::Logger::init(service, config);
    if (result.error == tbox::fw::log::LogError::kOk) {
        s_initialized = true;
    }
    return result;
}

tbox::fw::log::Logger MqttLogAdapter::connection() {
    return tbox::fw::log::Logger::get("connection");
}

tbox::fw::log::Logger MqttLogAdapter::tls() {
    return tbox::fw::log::Logger::get("tls");
}

tbox::fw::log::Logger MqttLogAdapter::publish() {
    return tbox::fw::log::Logger::get("publish");
}

tbox::fw::log::Logger MqttLogAdapter::subscribe() {
    return tbox::fw::log::Logger::get("subscribe");
}

tbox::fw::log::Logger MqttLogAdapter::offline_queue() {
    return tbox::fw::log::Logger::get("offline_queue");
}

tbox::fw::log::Logger MqttLogAdapter::router() {
    return tbox::fw::log::Logger::get("router");
}

tbox::fw::log::Logger MqttLogAdapter::ipc() {
    return tbox::fw::log::Logger::get("ipc");
}

tbox::fw::log::Logger MqttLogAdapter::provision() {
    return tbox::fw::log::Logger::get("provision");
}

tbox::fw::log::Logger MqttLogAdapter::service() {
    return tbox::fw::log::Logger::get("service");
}

} // namespace mqtt
} // namespace tbox
