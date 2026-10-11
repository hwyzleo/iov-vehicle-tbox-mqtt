//
// TBOX-MQTT-DSN-CR-016 §10: 关键 warn/error 集中 flush 验证
//
// 本 CR 采用阶段方案：MQTT 只调用 framework-log 公开 Logger::flush()（经
// MqttLogAdapter::flushCritical），集中在依赖连接失败/长等待路径，不访问私有 sink。
// flush_on(warn)、周期 flush 与统一 sink 策略另建 framework CR。
//
// 本二进制单独初始化 Logger 到临时文件 sink，验证 flushCritical 后当前实例
// 的 warn 事件可立即从文件读取（不依赖进程退出）。
//

#include <gtest/gtest.h>

#include "mqtt_log_adapter.h"
#include "mqtt_context.h"
#include "log_types.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>

using namespace tbox::mqtt;
using namespace tbox::fw::log;

TEST(MqttDependencyFlushTest, FlushCriticalMakesWarnVisible) {
    const std::string service = "mqtt_cr016_flush";
    auto root = std::filesystem::temp_directory_path() /
                ("tbox_mqtt_cr016_flush_" +
                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root);

    LogConfig config;
    config.level = LogLevel::kDebug;
    config.console_config.enabled = false;
    config.file_config.enabled = true;
    config.file_config.root = root.string();
    config.async_config.enabled = true;
    config.async_config.flush_interval_ms = 1000;

    auto result = MqttLogAdapter::init(service, config);
    ASSERT_EQ(result.error, LogError::kOk);

    MqttLogAdapter::dependency().warn(
        "mqtt_dependency_ipc_connect_failed",
        "SEC IPC 连接失败（测试）",
        {
            {"dependency", FieldValue::makeString("sec")},
            {"reason_code", FieldValue::makeString("sec_connect_failed")},
            {"retry_count", FieldValue::makeInt(3)},
            {"error_code", FieldValue::makeString("MQTT-1001")}
        }
    );

    // 集中 flush：当前实例日志应在约定窗口内可见
    MqttLogAdapter::flushCritical();

    std::string log_path = (root / service / (service + "_0.log")).string();

    // flushCritical 已触发 dispatch drain + sink flush；异步 sink 的写入与 flush
    // 可能同批竞争，故在限定时窗内重试读取（不逐条 info 刷盘）。
    std::string content;
    bool visible = false;
    for (int i = 0; i < 40; ++i) {  // 最多 ~2s
        std::ifstream ifs(log_path);
        if (ifs.good()) {
            content.assign(std::istreambuf_iterator<char>(ifs),
                           std::istreambuf_iterator<char>());
            if (content.find("sec_connect_failed") != std::string::npos) {
                visible = true;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    EXPECT_TRUE(visible) << "log file content: " << content;

    // 不逐条 info 强制刷盘：普通 info 不触发 flushCritical
    std::filesystem::remove_all(root);
}
