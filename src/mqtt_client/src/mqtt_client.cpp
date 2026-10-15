#include "tbox/mqtt/client.h"
#include "tbox/mqtt/ipc_protocol.h"
#include "mqtt_retry_policy.h"
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <thread>

namespace tbox {
namespace mqtt {

// ============================================================
// CR-010 §4: routed downlink 解析与校验助手
// ============================================================
namespace {

// 字段长度与 payload 上限 (CR-010 §4, 安全边界)
constexpr size_t kMaxOwnerLen     = 128;
constexpr size_t kMaxRouteIdLen   = 128;
constexpr size_t kMaxTargetLen    = 128;
constexpr size_t kMaxRequestIdLen = 128;
constexpr size_t kMaxTraceIdLen   = 256;
constexpr size_t kMaxPayloadBytes = 1 * 1024 * 1024;  // 1 MiB
// 每 owner 有界 dispatch queue 容量 (CR-010 §5)
constexpr size_t kRoutedDispatchQueueSize = 128;

/// owner 命名规则：非空、长度受限、仅 [A-Za-z0-9._-]
bool isValidOwnerName(const std::string& s) {
    if (s.empty() || s.size() > kMaxOwnerLen) return false;
    for (char c : s) {
        if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '_' || c == '-')) {
            return false;
        }
    }
    return true;
}

/// 通用非空可打印字符串校验
bool isValidMetaString(const std::string& s, size_t max_len) {
    if (s.empty() || s.size() > max_len) return false;
    for (char c : s) {
        unsigned char uc = static_cast<unsigned char>(c);
        if (uc < 0x20 || uc == 0x7f) return false;  // 拒绝控制字符
    }
    return true;
}

/// base64 字符 -> 6bit 值，非法返回 -1
static int b64CharVal(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

/// 标准 base64 编码（带填充，无换行）
/// 本地实现，替代旧式对 framework 遗留头文件 utils.h 的裸名依赖 (CR-011 §7)。
std::string b64Encode(const std::vector<std::uint8_t>& data) {
    static const char kAlphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((data.size() + 2) / 3) * 4);
    size_t i = 0;
    for (; i + 2 < data.size(); i += 3) {
        uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                     (static_cast<uint32_t>(data[i + 1]) << 8) |
                     static_cast<uint32_t>(data[i + 2]);
        out.push_back(kAlphabet[(n >> 18) & 0x3f]);
        out.push_back(kAlphabet[(n >> 12) & 0x3f]);
        out.push_back(kAlphabet[(n >> 6) & 0x3f]);
        out.push_back(kAlphabet[n & 0x3f]);
    }
    size_t rem = data.size() - i;
    if (rem == 1) {
        uint32_t n = static_cast<uint32_t>(data[i]) << 16;
        out.push_back(kAlphabet[(n >> 18) & 0x3f]);
        out.push_back(kAlphabet[(n >> 12) & 0x3f]);
        out.push_back('=');
        out.push_back('=');
    } else if (rem == 2) {
        uint32_t n = (static_cast<uint32_t>(data[i]) << 16) |
                     (static_cast<uint32_t>(data[i + 1]) << 8);
        out.push_back(kAlphabet[(n >> 18) & 0x3f]);
        out.push_back(kAlphabet[(n >> 12) & 0x3f]);
        out.push_back(kAlphabet[(n >> 6) & 0x3f]);
        out.push_back('=');
    }
    return out;
}

/// 宽松 base64 解码（与旧 framework Utils::base64_decode 行为一致：
/// 接受带/不带填充，忽略非法字符）。调用方已保证输入来自内部协议。
std::string b64DecodeLenient(const std::string& s) {
    std::vector<std::uint8_t> buf;
    buf.reserve(s.size());
    unsigned int bits = 0;
    unsigned int acc = 0;
    for (unsigned char c : s) {
        if (c == '=') break;
        int v = b64CharVal(c);
        if (v < 0) continue;  // 忽略非法字符（换行/空白/异常）
        acc = (acc << 6) | static_cast<unsigned int>(v);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            buf.push_back(static_cast<std::uint8_t>((acc >> bits) & 0xff));
        }
    }
    return std::string(reinterpret_cast<const char*>(buf.data()), buf.size());
}

/// 严格 base64 解码：校验字母表/填充/长度后解码，失败返回 false 且不产生部分输出 (CR-010 §4)
bool strictB64Decode(const std::string& s, std::vector<std::uint8_t>& out) {
    out.clear();
    if (s.empty()) return true;
    if (s.size() % 4 != 0) return false;

    // 校验尾部填充
    size_t pad = 0;
    if (s.size() >= 1 && s.back() == '=') pad++;
    if (s.size() >= 2 && s[s.size() - 2] == '=') pad++;
    for (size_t i = 0; i < s.size() - pad; ++i) {
        if (s[i] == '=') return false;  // 填充只能出现在末尾
    }
    for (size_t i = s.size() - pad; i < s.size(); ++i) {
        if (s[i] != '=') return false;
    }
    // 字母表校验（非填充位）
    for (size_t i = 0; i < s.size() - pad; ++i) {
        if (b64CharVal(static_cast<unsigned char>(s[i])) < 0) return false;
    }

    out.reserve((s.size() / 4) * 3);
    for (size_t i = 0; i < s.size(); i += 4) {
        int v0 = b64CharVal(static_cast<unsigned char>(s[i]));
        int v1 = b64CharVal(static_cast<unsigned char>(s[i + 1]));
        int v2 = (s[i + 2] != '=') ? b64CharVal(static_cast<unsigned char>(s[i + 2])) : 0;
        int v3 = (s[i + 3] != '=') ? b64CharVal(static_cast<unsigned char>(s[i + 3])) : 0;
        out.push_back(static_cast<std::uint8_t>((v0 << 2) | (v1 >> 4)));
        if (s[i + 2] != '=') out.push_back(static_cast<std::uint8_t>(((v1 & 0x0f) << 4) | (v2 >> 2)));
        if (s[i + 3] != '=') out.push_back(static_cast<std::uint8_t>(((v2 & 0x03) << 6) | v3));
    }
    if (out.size() > kMaxPayloadBytes) {
        out.clear();
        return false;
    }
    return true;
}

/// 解析并校验 routed downlink JSON -> typed event (CR-010 §4)
/// 任一步失败返回 false（不产生部分 event）；expected_owner 必须与事件 owner 完全一致。
bool parseRoutedEvent(std::string_view payload_json,
                     const std::string& expected_owner,
                     RoutedDownlinkEvent& out) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(payload_json);
    } catch (...) {
        return false;
    }
    if (!j.is_object()) return false;

    std::string owner = j.value("owner", "");
    std::string route_id = j.value("route_id", "");
    std::string target = j.value("target", "");
    std::string request_id = j.value("request_id", "");
    std::string trace_id = j.value("trace_id", "");
    std::string payload_b64 = j.value("payload_base64", "");

    // 必填非空 + 长度/格式校验
    if (!isValidOwnerName(owner)) return false;
    if (owner != expected_owner) return false;  // SDK 二次验证 owner (CR-010 §4 step 3)
    if (!isValidMetaString(route_id, kMaxRouteIdLen)) return false;
    if (!isValidMetaString(target, kMaxTargetLen)) return false;
    if (!isValidMetaString(request_id, kMaxRequestIdLen)) return false;
    if (!trace_id.empty() && !isValidMetaString(trace_id, kMaxTraceIdLen)) return false;

    // qos 只接受 0/1/2
    if (!j.contains("qos") || !j["qos"].is_number_integer()) return false;
    int qos = j["qos"].get<int>();
    if (qos < 0 || qos > 2) return false;

    // payload_base64 类型校验 + 严格解码
    if (!j["payload_base64"].is_string()) return false;
    std::vector<std::uint8_t> payload;
    if (!strictB64Decode(payload_b64, payload)) return false;

    out.owner = std::move(owner);
    out.route_id = std::move(route_id);
    out.target = std::move(target);
    out.qos = static_cast<std::uint8_t>(qos);
    out.payload = std::move(payload);
    out.request_id = std::move(request_id);
    out.trace_id = std::move(trace_id);
    return true;
}

/// 解析 Cloud Readiness JSON -> CloudReadinessSnapshot (CR-014 §3, §5, §7)
/// contract_version 缺失或与 kCloudReadinessContractVersion 不一致时标记
/// contract_supported=false，绝不伪造空证书身份后宣告可用。
CloudReadinessSnapshot parseCloudReadinessJson(std::string_view payload_json) {
    CloudReadinessSnapshot r;
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(payload_json);
    } catch (...) {
        r.contract_supported = false;
        r.reason_code = "UNSUPPORTED_CONTRACT_VERSION";
        return r;
    }
    if (!j.is_object()) {
        r.contract_supported = false;
        r.reason_code = "UNSUPPORTED_CONTRACT_VERSION";
        return r;
    }
    const uint32_t version = j.value("contract_version", 0u);
    r.contract_version = version;
    if (version != kCloudReadinessContractVersion) {
        r.contract_supported = false;
        r.cloud_ready = false;
        r.reason_code = "UNSUPPORTED_CONTRACT_VERSION";
        return r;
    }
    r.contract_supported = true;
    r.cloud_ready = j.value("cloud_ready", false);
    r.connection_generation = j.value("connection_generation", 0ULL);
    r.cert_session_generation = j.value("cert_session_generation", 0ULL);
    r.credential_id = j.value("credential_id", "");
    r.credential_version = j.value("credential_version", 0ULL);
    r.device_identity_digest = j.value("device_identity_digest", "");
    r.certificate_ref = j.value("certificate_ref", "");
    r.reason_code = j.value("reason_code", "");
    r.event_sequence = j.value("event_sequence", 0ULL);
    // CR-017 §4: 会话身份投影、可校验上下文与 publish 契约版本
    r.ecu_uid = j.value("ecu_uid", "");
    r.publish_contract_version = j.value("publish_contract_version", 0u);
    if (j.contains("session_context") && j["session_context"].is_object()) {
        const auto& sc = j["session_context"];
        r.session_context.daemon_epoch = sc.value("daemon_epoch", "");
        r.session_context.connection_generation = sc.value("connection_generation", 0ULL);
        r.session_context.cert_session_generation = sc.value("cert_session_generation", 0ULL);
    }
    r.subscriptions_ready = j.value("subscriptions_ready", false);
    r.connected = j.value("connected", false);
    r.mandatory_count = j.value("mandatory_count", 0u);
    r.mandatory_active = j.value("mandatory_active", 0u);
    if (j.contains("owner_generations") && j["owner_generations"].is_object()) {
        for (auto it = j["owner_generations"].begin(); it != j["owner_generations"].end(); ++it) {
            r.owner_generations[it.key()] = it.value().get<uint64_t>();
        }
    }
    return r;
}

/// 解析 CapabilityReadiness JSON -> CapabilityReadiness (CR-015 §18.4)
/// contract_version 缺失或不兼容时 contract_supported=false，不得伪造 ready。
CapabilityReadiness parseCapabilityReadinessJson(std::string_view payload_json,
                                                 const std::string& expected_capability) {
    CapabilityReadiness r;
    r.capability = expected_capability;
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(payload_json);
    } catch (...) {
        r.contract_supported = false;
        r.reason_code = "UNSUPPORTED_CONTRACT_VERSION";
        return r;
    }
    if (!j.is_object()) {
        r.contract_supported = false;
        r.reason_code = "UNSUPPORTED_CONTRACT_VERSION";
        return r;
    }
    const uint32_t version = j.value("contract_version", 0u);
    if (version != kCapabilityReadinessContractVersion) {
        r.contract_supported = false;
        r.ready = false;
        r.reason_code = "UNSUPPORTED_CONTRACT_VERSION";
        return r;
    }
    r.contract_supported = true;
    std::string cap = j.value("capability", expected_capability);
    if (cap != expected_capability) {
        // capability 不匹配：不得构造半成品，标记不支持
        r.contract_supported = false;
        r.ready = false;
        r.reason_code = "CAPABILITY_MISMATCH";
        return r;
    }
    r.capability = std::move(cap);
    r.enabled = j.value("enabled", false);
    r.snapshot_accepted = j.value("snapshot_accepted", false);
    r.uplink_ready = j.value("uplink_ready", false);
    r.downlink_suback_ready = j.value("downlink_suback_ready", false);
    r.ready = j.value("ready", false);
    r.owner_generation = j.value("owner_generation", 0ULL);
    r.connection_generation = j.value("connection_generation", 0ULL);
    r.cert_session_generation = j.value("cert_session_generation", 0ULL);
    r.reason_code = j.value("reason_code", "");
    r.event_sequence = j.value("event_sequence", 0ULL);
    return r;
}

} // anonymous namespace

// ============================================================
// CR-010 §2.2/§5/§6: RoutedSubscription::Impl
// 每个 routed 订阅独立连接 + 有界 dispatch queue（解耦 IPC 读取与业务 callback）。
// reader 线程 = framework Subscription 读取线程（解析/校验后入队）；
// dispatcher 线程 = 出队调用业务 callback。队列满按 disconnect 策略由 dispatcher
// 取消 framework 订阅（不能在 reader 线程内 cancel，避免 join 自身死锁）。
// ============================================================
class RoutedSubscription::Impl
    : public std::enable_shared_from_this<RoutedSubscription::Impl> {
public:
    Impl(std::string owner, RoutedDownlinkCallback callback,
         size_t queue_capacity)
        : owner_(std::move(owner))
        , callback_(std::move(callback))
        , queue_capacity_(queue_capacity) {}

    ~Impl() { cancel(); }

    // 在 Client::Impl 持有的 fw_client 上建立独立事件连接 (CR-010 §3.1)
    void start(::tbox::fw::ipc::Client* fw_client) {
        nlohmann::json params;
        params["owner"] = owner_;
        auto self = shared_from_this();
        fw_sub_ = fw_client->subscribe(
            static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE_ROUTED_DOWNLINK),
            static_cast<uint32_t>(ipc::EventType::ROUTED_DOWNLINK_MESSAGE),
            params.dump(),
            [self](uint32_t /*ev_type*/, std::string_view payload_json) {
                self->onEvent(payload_json);
            });
        dispatcher_ = std::thread(&Impl::dispatchLoop, this);
    }

    // 幂等取消：停 reader + dispatcher，等待在途 callback 完成 (CR-010 §6)
    void cancel() {
        bool expected = true;
        if (!active_.compare_exchange_strong(expected, false)) {
            return;  // 已取消
        }
        overflowed_.store(true);  // 唤醒 dispatcher
        queue_cv_.notify_all();
        if (dispatcher_.joinable()) dispatcher_.join();
        fw_sub_.cancel();  // 幂等：停 reader 线程并关闭连接
    }

    bool isActive() const {
        return active_.load() && !overflowed_.load() && fw_sub_.isActive();
    }

private:
    // reader 线程回调：解析/校验后入队；队列满按 disconnect 策略标记溢出 (CR-010 §5)
    void onEvent(std::string_view payload_json) {
        if (!active_.load() || overflowed_.load()) return;
        RoutedDownlinkEvent event;
        if (!parseRoutedEvent(payload_json, owner_, event)) {
            // MQTT-1013：缺失/非法/owner 不匹配/base64 失败，丢弃当前事件，继续读取 (CR-010 §4/§9)
            return;
        }
        bool overflow = false;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (queue_.size() >= queue_capacity_) {
                overflow = true;
            } else {
                queue_.push_back(std::move(event));
            }
        }
        if (overflow) {
            // disconnect 策略：标记溢出，由 dispatcher 取消 framework 订阅 (CR-010 §5)
            overflowed_.store(true);
            queue_cv_.notify_all();
        } else {
            queue_cv_.notify_one();
        }
    }

    // dispatcher 线程：串行出队调用业务 callback（锁外），异常不终止线程 (CR-010 §5)
    void dispatchLoop() {
        while (true) {
            RoutedDownlinkEvent event;
            bool have_event = false;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cv_.wait(lock, [this] {
                    return !active_.load() || overflowed_.load() || !queue_.empty();
                });
                if (!active_.load()) {
                    return;  // cancel：不再启动新 callback (CR-010 §6)
                }
                if (overflowed_.load()) {
                    break;  // 溢出：退出后取消 framework 订阅
                }
                if (!queue_.empty()) {
                    event = std::move(queue_.front());
                    queue_.pop_front();
                    have_event = true;
                }
            }
            if (have_event) {
                try {
                    if (callback_) callback_(event);
                } catch (...) {
                    // 捕获业务异常，继续读取 (CR-010 §5)
                }
            }
        }
        // 溢出 disconnect：由 dispatcher（非 reader 线程）取消 framework 订阅，关闭 fd
        fw_sub_.cancel();
    }

    std::string owner_;
    RoutedDownlinkCallback callback_;
    size_t queue_capacity_;

    std::deque<RoutedDownlinkEvent> queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    std::atomic<bool> active_{true};
    std::atomic<bool> overflowed_{false};

    ::tbox::fw::ipc::Subscription fw_sub_;
    std::thread dispatcher_;
};

// ============================================================
// CR-014 §5: SubscriptionHandle::Impl
// 独立 dispatcher 线程 + 有界队列解耦 IPC 读取与业务 callback；
// sequence 缺口/回退（daemon 重启）时自动重查快照补投，实现最终收敛。
// ============================================================
class SubscriptionHandle::Impl {
public:
    virtual ~Impl() = default;
    virtual void cancel() = 0;
    virtual bool isActive() const = 0;
};

class CloudReadinessHandleImpl
    : public SubscriptionHandle::Impl,
      public std::enable_shared_from_this<CloudReadinessHandleImpl> {
public:
    CloudReadinessHandleImpl(CloudReadinessCallback callback, size_t queue_capacity)
        : callback_(std::move(callback)), queue_capacity_(queue_capacity) {}

    ~CloudReadinessHandleImpl() override { cancel(); }

    void start(::tbox::fw::ipc::Client* fw_client,
               std::function<CloudReadinessSnapshot()> resync) {
        resync_ = std::move(resync);
        auto self = shared_from_this();
        fw_sub_ = fw_client->subscribe(
            static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE_CLOUD_READINESS),
            static_cast<uint32_t>(ipc::EventType::CLOUD_READY_CHANGED),
            [self](uint32_t /*ev_type*/, std::string_view payload_json) {
                self->onEvent(payload_json);
            });
        dispatcher_ = std::thread(&CloudReadinessHandleImpl::dispatchLoop, this);
    }

    void cancel() override {
        bool expected = true;
        if (!active_.compare_exchange_strong(expected, false)) return;
        queue_cv_.notify_all();
        if (dispatcher_.joinable()) dispatcher_.join();
        fw_sub_.cancel();
    }

    bool isActive() const override {
        return active_.load() && fw_sub_.isActive();
    }

private:
    void onEvent(std::string_view payload_json) {
        if (!active_.load()) return;
        CloudReadinessSnapshot snap = parseCloudReadinessJson(payload_json);
        if (!snap.contract_supported) {
            // 不兼容契约：投递一次 unsupported 快照，不更新 sequence
            enqueue(std::move(snap));
            return;
        }
        bool need_resync = false;
        {
            std::lock_guard<std::mutex> lock(seq_mutex_);
            if (have_sequence_ && snap.event_sequence <= last_sequence_) {
                need_resync = true;  // daemon 重启/sequence 回退：全量重读
            } else if (have_sequence_ && snap.event_sequence > last_sequence_ + 1) {
                need_resync = true;  // 事件缺口
            }
            if (!need_resync) {
                last_sequence_ = snap.event_sequence;
                have_sequence_ = true;
            }
        }
        if (need_resync && resync_) {
            // 事件丢失/重启：重查快照收敛，以较大 sequence 为准 (CR-014 §5)
            CloudReadinessSnapshot fresh = resync_();
            if (fresh.contract_supported) {
                std::lock_guard<std::mutex> lock(seq_mutex_);
                if (fresh.event_sequence > last_sequence_) {
                    last_sequence_ = fresh.event_sequence;
                }
                have_sequence_ = true;
            }
            enqueue(std::move(fresh));
        } else {
            enqueue(std::move(snap));
        }
    }

    void enqueue(CloudReadinessSnapshot snap) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (queue_.size() >= queue_capacity_) {
                return;  // 有界：丢弃最新；readiness 可经重查收敛
            }
            queue_.push_back(std::move(snap));
        }
        queue_cv_.notify_one();
    }

    void dispatchLoop() {
        while (true) {
            CloudReadinessSnapshot snap;
            bool have = false;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cv_.wait(lock, [this] {
                    return !active_.load() || !queue_.empty();
                });
                if (!active_.load()) return;
                if (!queue_.empty()) {
                    snap = std::move(queue_.front());
                    queue_.pop_front();
                    have = true;
                }
            }
            if (have && callback_) {
                try {
                    callback_(snap);
                } catch (...) {
                    // 捕获业务异常，继续投递 (CR-014 §5)
                }
            }
        }
    }

    CloudReadinessCallback callback_;
    std::function<CloudReadinessSnapshot()> resync_;
    size_t queue_capacity_;

    std::deque<CloudReadinessSnapshot> queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    std::mutex seq_mutex_;
    uint64_t last_sequence_ = 0;
    bool have_sequence_ = false;

    std::atomic<bool> active_{true};
    ::tbox::fw::ipc::Subscription fw_sub_;
    std::thread dispatcher_;
};

// ============================================================
// CR-015 §18.4: CapabilityHandleImpl
// 与 CloudReadinessHandleImpl 同构，但使用 capability 事件与 Per-capability 序列。
// ============================================================
class CapabilityHandleImpl
    : public SubscriptionHandle::Impl,
      public std::enable_shared_from_this<CapabilityHandleImpl> {
public:
    CapabilityHandleImpl(std::string capability,
                         CapabilityReadinessCallback callback,
                         size_t queue_capacity)
        : capability_(std::move(capability)),
          callback_(std::move(callback)),
          queue_capacity_(queue_capacity) {}

    ~CapabilityHandleImpl() override { cancel(); }

    void start(::tbox::fw::ipc::Client* fw_client,
               std::function<CapabilityReadiness()> resync) {
        resync_ = std::move(resync);
        nlohmann::json params;
        params["capability"] = capability_;
        auto self = shared_from_this();
        fw_sub_ = fw_client->subscribe(
            static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE_CAPABILITY_READINESS),
            static_cast<uint32_t>(ipc::EventType::CAPABILITY_READINESS_CHANGED),
            params.dump(),
            [self](uint32_t /*ev_type*/, std::string_view payload_json) {
                self->onEvent(payload_json);
            });
        dispatcher_ = std::thread(&CapabilityHandleImpl::dispatchLoop, this);
    }

    void cancel() override {
        bool expected = true;
        if (!active_.compare_exchange_strong(expected, false)) return;
        queue_cv_.notify_all();
        if (dispatcher_.joinable()) dispatcher_.join();
        fw_sub_.cancel();
    }

    bool isActive() const override {
        return active_.load() && fw_sub_.isActive();
    }

private:
    void onEvent(std::string_view payload_json) {
        if (!active_.load()) return;
        CapabilityReadiness snap =
            parseCapabilityReadinessJson(payload_json, capability_);
        if (!snap.contract_supported) {
            enqueue(std::move(snap));
            return;
        }
        bool need_resync = false;
        {
            std::lock_guard<std::mutex> lock(seq_mutex_);
            if (have_sequence_ && snap.event_sequence <= last_sequence_) {
                need_resync = true;
            } else if (have_sequence_ && snap.event_sequence > last_sequence_ + 1) {
                need_resync = true;
            }
            if (!need_resync) {
                last_sequence_ = snap.event_sequence;
                have_sequence_ = true;
            }
        }
        if (need_resync && resync_) {
            CapabilityReadiness fresh = resync_();
            if (fresh.contract_supported) {
                std::lock_guard<std::mutex> lock(seq_mutex_);
                if (fresh.event_sequence > last_sequence_) {
                    last_sequence_ = fresh.event_sequence;
                }
                have_sequence_ = true;
            }
            enqueue(std::move(fresh));
        } else {
            enqueue(std::move(snap));
        }
    }

    void enqueue(CapabilityReadiness snap) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (queue_.size() >= queue_capacity_) return;
            queue_.push_back(std::move(snap));
        }
        queue_cv_.notify_one();
    }

    void dispatchLoop() {
        while (true) {
            CapabilityReadiness snap;
            bool have = false;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cv_.wait(lock, [this] {
                    return !active_.load() || !queue_.empty();
                });
                if (!active_.load()) return;
                if (!queue_.empty()) {
                    snap = std::move(queue_.front());
                    queue_.pop_front();
                    have = true;
                }
            }
            if (have && callback_) {
                try {
                    callback_(snap);
                } catch (...) {
                    // 捕获业务异常，继续投递
                }
            }
        }
    }

    std::string capability_;
    CapabilityReadinessCallback callback_;
    std::function<CapabilityReadiness()> resync_;
    size_t queue_capacity_;

    std::deque<CapabilityReadiness> queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    std::mutex seq_mutex_;
    uint64_t last_sequence_ = 0;
    bool have_sequence_ = false;

    std::atomic<bool> active_{true};
    ::tbox::fw::ipc::Subscription fw_sub_;
    std::thread dispatcher_;
};

// ============================================================
// Client::Impl
// ============================================================
class Client::Impl {
public:
    explicit Impl(const std::string& socket_path)
        : socket_path_(socket_path),
          fw_client_(std::make_unique<::tbox::fw::ipc::Client>(socket_path_)) {
    }

    ~Impl() {
        // CR-010 §6 / CR-014 §5 / CR-015 §18.4: 先取消订阅句柄（join reader/dispatcher），再断 fw_client
        cancelAllReadiness();
        cancelAllCapability();
        cancelAllRouted();
        disconnect();
    }

    bool connect() {
        return fw_client_->connect();
    }

    void disconnect() {
        fw_client_->disconnect();
    }

    bool is_connected() const {
        return fw_client_->is_connected();
    }

    /// Send request and receive response (CR-004 §5.2, §7.1)
    /// @return (transport_ok, biz_status_code, response_json)
    std::tuple<bool, int32_t, std::string>
    send_request(uint32_t method_id, const std::string& params_json) {
        bool retry_allowed = MqttRetryPolicy::should_retry(method_id);

        std::pair<int32_t, std::string> result;
        if (retry_allowed) {
            result = fw_client_->call(method_id, params_json);
        } else {
            // callOnce: no auto-retry, transport failure = unknown outcome (CR-004 §7.1)
            result = fw_client_->callOnce(method_id, params_json);
        }

        auto [fw_status, response_json] = result;

        if (fw_status < 0) {
            // Client transport error: unknown outcome (server may have executed)
            return {false, 0, ""};
        }
        if (fw_status > 0) {
            // Server transport/handler error (FW-03xx)
            return {false, 0, ""};
        }

        // fw_status == 0: transport success, extract business status from JSON
        int32_t biz_status = 0;
        try {
            auto j = nlohmann::json::parse(response_json);
            biz_status = j.value("status", 0);
        } catch (...) {
            // JSON parse failure, keep biz_status = 0
        }
        return {true, biz_status, response_json};
    }

    // CR-014 §5: Cloud Ready 快照查询（与事件共享同一状态对象）
    CloudReadinessSnapshot fetchCloudReadiness() {
        auto [ok, status, response_json] = send_request(
            static_cast<uint32_t>(ipc::MethodId::GET_SUBSCRIPTION_READINESS), "{}");
        if (!ok) {
            CloudReadinessSnapshot r;
            r.contract_supported = false;
            r.reason_code = "transport_failed";
            return r;
        }
        CloudReadinessSnapshot r = parseCloudReadinessJson(response_json);
        // CR-017 §4: 缓存 daemon 公布的精 identity 绑定发布契约版本，
        // 供 publishRoute(expected_session) 协商（旧 daemon 无保护，不得发 keyprov）
        publish_contract_version_.store(r.contract_supported ? r.publish_contract_version : 0u);
        return r;
    }

    /// CR-017: daemon 公布的身份绑定发布契约版本（0=未知/不支持）
    std::atomic<uint32_t> publish_contract_version_{0};

    std::string b64_encode(const std::vector<uint8_t>& data) {
        return b64Encode(data);
    }

    std::vector<uint8_t> b64_decode(const std::string& encoded) {
        std::string decoded = b64DecodeLenient(encoded);
        return std::vector<uint8_t>(decoded.begin(), decoded.end());
    }

    ::tbox::fw::ipc::Subscription subscribe_event(uint32_t event_type,
                                                   MessageCallback callback) {
        // 每个 subscribe 建立独立连接 (CR-004 §6)
        return fw_client_->subscribe(
            static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE),
            event_type,
            [cb = std::move(callback)](uint32_t /*ev_type*/, std::string_view payload_json) {
                // 解包下行事件 payload: {topic, payload_base64}
                try {
                    auto j = nlohmann::json::parse(payload_json);
                    std::string topic = j.value("topic", "");
                    std::string b64 = j.value("payload_base64", "");
                    std::vector<uint8_t> payload;
                    if (!b64.empty()) {
                        std::string decoded = b64DecodeLenient(b64);
                        payload.assign(decoded.begin(), decoded.end());
                    }
                    if (cb) {
                        cb(topic, payload);
                    }
                } catch (const std::exception&) {
                    // 忽略格式错误的事件
                }
            });
    }

    // ---- CR-010 §2.2/§7: routed downlink 订阅 ----
    // 返回 shared_ptr<RoutedSubscription::Impl>；nullptr 表示 owner 互斥冲突或参数非法。
    std::shared_ptr<RoutedSubscription::Impl>
    subscribeRoutedDownlink(std::string_view owner, RoutedDownlinkCallback callback) {
        std::string owner_str(owner);
        if (!isValidOwnerName(owner_str) || !callback) {
            return nullptr;
        }

        std::lock_guard<std::mutex> lock(routed_mutex_);
        // 互斥注册表：同一 owner 不得重复 routed 订阅 (CR-010 §7)
        auto it = routed_registry_.find(owner_str);
        if (it != routed_registry_.end()) {
            if (auto sp = it->second.lock()) {
                if (sp->isActive()) {
                    // 冲突：在发出任何 IPC 请求前失败 (CR-010 §7)
                    return nullptr;
                }
            }
            routed_registry_.erase(it);
        }

        auto impl = std::make_shared<RoutedSubscription::Impl>(
            owner_str, std::move(callback), kRoutedDispatchQueueSize);
        impl->start(fw_client_.get());
        routed_registry_[owner_str] = impl;
        return impl;
    }

    void cancelAllRouted() {
        std::vector<std::shared_ptr<RoutedSubscription::Impl>> live;
        {
            std::lock_guard<std::mutex> lock(routed_mutex_);
            live.reserve(routed_registry_.size());
            for (auto& [owner, weak] : routed_registry_) {
                if (auto sp = weak.lock()) live.push_back(std::move(sp));
            }
            routed_registry_.clear();
        }
        for (auto& sp : live) sp->cancel();  // join reader/dispatcher
    }

    // CR-014 §5: Cloud Ready 事件订阅（独立执行器，不阻塞 IPC 读线程）
    std::shared_ptr<SubscriptionHandle::Impl>
    subscribeCloudReadiness(CloudReadinessCallback callback) {
        if (!callback) return nullptr;
        auto impl = std::make_shared<CloudReadinessHandleImpl>(
            std::move(callback), kRoutedDispatchQueueSize);
        impl->start(fw_client_.get(), [this]() { return fetchCloudReadiness(); });
        {
            std::lock_guard<std::mutex> lock(readiness_mutex_);
            readiness_registry_.push_back(impl);
        }
        return impl;
    }

    void cancelAllReadiness() {
        std::vector<std::shared_ptr<SubscriptionHandle::Impl>> live;
        {
            std::lock_guard<std::mutex> lock(readiness_mutex_);
            live.reserve(readiness_registry_.size());
            for (auto& weak : readiness_registry_) {
                if (auto sp = weak.lock()) live.push_back(std::move(sp));
            }
            readiness_registry_.clear();
        }
        for (auto& sp : live) sp->cancel();
    }

    // CR-015 §18.4: capability readiness 查询与事件订阅
    CapabilityReadiness fetchCapabilityReadiness(const std::string& capability) {
        nlohmann::json params;
        params["capability"] = capability;
        auto [ok, status, response_json] = send_request(
            static_cast<uint32_t>(ipc::MethodId::GET_CAPABILITY_READINESS),
            params.dump());
        if (!ok) {
            CapabilityReadiness r;
            r.capability = capability;
            r.contract_supported = false;
            r.reason_code = "transport_failed";
            return r;
        }
        return parseCapabilityReadinessJson(response_json, capability);
    }

    std::shared_ptr<SubscriptionHandle::Impl>
    subscribeCapabilityReadiness(const std::string& capability,
                                 CapabilityReadinessCallback callback) {
        if (capability.empty() || !callback) return nullptr;
        auto impl = std::make_shared<CapabilityHandleImpl>(
            capability, std::move(callback), kRoutedDispatchQueueSize);
        impl->start(fw_client_.get(), [this, capability]() {
            return fetchCapabilityReadiness(capability);
        });
        {
            std::lock_guard<std::mutex> lock(capability_handle_mutex_);
            capability_registry_.push_back(impl);
        }
        return impl;
    }

    void cancelAllCapability() {
        std::vector<std::shared_ptr<SubscriptionHandle::Impl>> live;
        {
            std::lock_guard<std::mutex> lock(capability_handle_mutex_);
            live.reserve(capability_registry_.size());
            for (auto& weak : capability_registry_) {
                if (auto sp = weak.lock()) live.push_back(std::move(sp));
            }
            capability_registry_.clear();
        }
        for (auto& sp : live) sp->cancel();
    }

private:
    std::string socket_path_;
    std::unique_ptr<::tbox::fw::ipc::Client> fw_client_;

    // CR-010 §7: owner -> routed 订阅注册表（互斥 + 析构批量取消）
    std::map<std::string, std::weak_ptr<RoutedSubscription::Impl>> routed_registry_;
    std::mutex routed_mutex_;

    // CR-014 §5: Cloud Ready 订阅句柄注册表（析构批量取消）
    std::vector<std::weak_ptr<SubscriptionHandle::Impl>> readiness_registry_;
    std::mutex readiness_mutex_;

    // CR-015 §18.4: capability 订阅句柄注册表（析构批量取消）
    std::vector<std::weak_ptr<SubscriptionHandle::Impl>> capability_registry_;
    std::mutex capability_handle_mutex_;
};

// ============================================================
// RoutedSubscription facade (CR-010 §2.2)
// ============================================================
RoutedSubscription::RoutedSubscription() = default;

RoutedSubscription::~RoutedSubscription() {
    if (impl_) impl_->cancel();
}

RoutedSubscription::RoutedSubscription(RoutedSubscription&&) noexcept = default;
RoutedSubscription& RoutedSubscription::operator=(RoutedSubscription&&) noexcept = default;

RoutedSubscription::RoutedSubscription(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

void RoutedSubscription::cancel() {
    if (impl_) impl_->cancel();
}

bool RoutedSubscription::isActive() const {
    return impl_ && impl_->isActive();
}

// ============================================================
// CR-014 §5: SubscriptionHandle facade
// ============================================================
SubscriptionHandle::SubscriptionHandle() = default;

SubscriptionHandle::~SubscriptionHandle() {
    if (impl_) impl_->cancel();
}

SubscriptionHandle::SubscriptionHandle(SubscriptionHandle&&) noexcept = default;
SubscriptionHandle& SubscriptionHandle::operator=(SubscriptionHandle&&) noexcept = default;

SubscriptionHandle::SubscriptionHandle(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

void SubscriptionHandle::cancel() {
    if (impl_) impl_->cancel();
}

bool SubscriptionHandle::isActive() const {
    return impl_ && impl_->isActive();
}

// ============================================================
// Client facade (CR-004 §7.1)
// ============================================================

Client::Client(const std::string& socket_path)
    : impl_(std::make_unique<Impl>(socket_path)) {
}

Client::~Client() = default;

bool Client::connect() {
    return impl_->connect();
}

void Client::disconnect() {
    impl_->disconnect();
}

bool Client::is_connected() const {
    return impl_->is_connected();
}

PublishResult Client::publish(const PublishEnvelope& message) {
    PublishResult result;

    // 构建 envelope JSON (CR-004 §5.1)
    nlohmann::json params;
    params["msg_id"] = message.msg_id;
    params["topic"] = message.topic;
    params["qos"] = static_cast<int>(message.qos);
    params["priority"] = static_cast<int>(message.priority);
    params["content_type"] = message.content_type;
    if (!message.payload.empty()) {
        params["payload_base64"] = impl_->b64_encode(message.payload);
    }
    params["trace_id"] = message.trace_id;
    params["request_id"] = message.request_id;

    // publish: 相同 msg_id 可去重时允许一次重试 (CR-004 §7.1)
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH), params.dump());

    if (!ok) {
        // 传输失败：unknown outcome (CR-004 §5.2)
        // 只能用相同 msg_id 查询/重试，Server 按去重返回已有受理状态
        result.accepted = false;
        result.outcome = PublishOutcome::UNKNOWN;
        return result;
    }

    // 解析业务响应
    try {
        auto j = nlohmann::json::parse(response_json);
        result.accepted = j.value("accepted", false);
        std::string outcome = j.value("outcome", "rejected");
        result.outcome = (outcome == "accepted") ? PublishOutcome::ACCEPTED
                                                  : PublishOutcome::UNKNOWN;
        if (status != 0) {
            result.error = static_cast<ErrorCode>(status);
        }
    } catch (const std::exception&) {
        result.accepted = false;
        result.outcome = PublishOutcome::UNKNOWN;
    }
    return result;
}

// ============================================================
// CR-009 §6.1: Route-based 发布
// ============================================================

PublishResult Client::publishRoute(const std::string& owner,
                                   const std::string& route_id,
                                   const std::string& msg_id,
                                   const std::vector<uint8_t>& payload,
                                   QoS qos,
                                   Priority priority,
                                   const std::string& content_type,
                                   const std::string& trace_id,
                                   const std::string& request_id) {
    // CR-017: 旧签名保留源兼容；委托到带控制元数据重载（不含 expected_session）。
    PublishOptions options;
    options.qos = qos;
    options.priority = priority;
    options.content_type = content_type;
    options.trace_id = trace_id;
    options.request_id = request_id;
    return publishRoute(owner, route_id, msg_id, payload, options);
}

PublishResult Client::publishRoute(const std::string& owner,
                                   const std::string& route_id,
                                   const std::string& msg_id,
                                   const std::vector<uint8_t>& payload,
                                   const PublishOptions& options) {
    PublishResult result;

    // CR-017 §19.6: 携带 expected_session 时显式协商发布契约。
    // 旧 daemon 不识别 expected_session 也不会执行 guard，
    // 故新 SDK 必须拒绝，不得降级为无保护发布。
    if (options.expected_session.has_value() &&
        impl_->publish_contract_version_.load() != kPublishSessionContractVersion) {
        result.accepted = false;
        result.error = ErrorCode::MQTT_1004;
        result.reason_code = session_reason::kPublishContextUnsupported;
        result.retryable = false;
        return result;
    }

    // 构建 route publish JSON (CR-009 §6.1; CR-017 §5 expected_session)
    nlohmann::json params;
    params["owner"] = owner;
    params["route_id"] = route_id;
    params["msg_id"] = msg_id;
    params["qos"] = static_cast<int>(options.qos);
    params["priority"] = static_cast<int>(options.priority);
    params["content_type"] = options.content_type;
    if (!payload.empty()) {
        params["payload_base64"] = impl_->b64_encode(payload);
    }
    params["trace_id"] = options.trace_id;
    params["request_id"] = options.request_id;
    if (options.expected_session.has_value()) {
        nlohmann::json sc = nlohmann::json::object();
        sc["daemon_epoch"] = options.expected_session->daemon_epoch;
        sc["connection_generation"] = options.expected_session->connection_generation;
        sc["cert_session_generation"] = options.expected_session->cert_session_generation;
        params["expected_session"] = sc;
    }

    // publishRoute: 相同 msg_id 可去重时允许一次重试 (同 publish 语义, CR-004 §7.1)
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::PUBLISH_ROUTE), params.dump());

    if (!ok) {
        result.accepted = false;
        result.outcome = PublishOutcome::UNKNOWN;
        return result;
    }

    try {
        auto j = nlohmann::json::parse(response_json);
        result.accepted = j.value("accepted", false);
        std::string outcome = j.value("outcome", "rejected");
        result.outcome = (outcome == "accepted") ? PublishOutcome::ACCEPTED
                                                  : PublishOutcome::UNKNOWN;
        // CR-017 §6: 调用方按 reason 判断处理方式（STALE 刷新快照；CONFLICT 不可原样重试）
        result.reason_code = j.value("reason_code", "");
        result.retryable = j.value("retryable", true);
        if (status != 0) {
            result.error = static_cast<ErrorCode>(status);
        }
    } catch (const std::exception&) {
        result.accepted = false;
        result.outcome = PublishOutcome::UNKNOWN;
    }
    return result;
}

::tbox::fw::ipc::Subscription Client::subscribe(const Route& route, MessageCallback callback) {
    nlohmann::json params;
    params["topic"] = route.topic;
    params["qos"] = static_cast<int>(route.qos);

    // subscribe 为一次性，禁止自动重放 (CR-004 §7.1)
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::SUBSCRIBE), params.dump());

    if (!ok || status != 0) {
        // 订阅失败：返回非活跃句柄
        return ::tbox::fw::ipc::Subscription{};
    }

    // 建立 framework Subscription（独立连接，event-only loop, CR-004 §6）
    // framework 发送空 params，server 调 onSubscribeEventChannel 登记 add_subscription。
    // 下行事件 payload 含 topic，client 按 route.topic 过滤。
    std::string filter_topic = route.topic;
    return impl_->subscribe_event(
        static_cast<uint32_t>(ipc::EventType::DOWNLINK_MESSAGE),
        [cb = std::move(callback), filter_topic](const std::string& topic, const std::vector<uint8_t>& payload) {
            if (topic == filter_topic || filter_topic.empty()) {
                if (cb) cb(topic, payload);
            }
        });
}

// ============================================================
// CR-010 §2.2: Routed downlink 订阅
// ============================================================
RoutedSubscription Client::subscribeRoutedDownlink(std::string_view owner,
                                                   RoutedDownlinkCallback callback) {
    auto impl = impl_->subscribeRoutedDownlink(owner, std::move(callback));
    if (!impl) {
        return RoutedSubscription{};  // 互斥冲突或参数非法：空句柄
    }
    return RoutedSubscription(std::move(impl));
}

Result<void> Client::registerRoute(const RouteEntry& route) {
    nlohmann::json params;
    params["internal_addr"] = route.internal_addr;
    params["mqtt_topic"] = route.mqtt_topic;
    params["qos"] = static_cast<int>(route.qos);
    params["direction"] = (route.direction == RouteEntry::Direction::DOWN) ? "DOWN" : "UP";

    // registerRoute 同 key 覆盖，允许一次传输重试 (CR-004 §7.1)
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::REGISTER_ROUTE), params.dump());

    if (!ok) {
        return Result<void>::failure(ErrorCode::MQTT_1002);
    }
    if (status != 0) {
        return Result<void>::failure(static_cast<ErrorCode>(status));
    }
    return Result<void>::success();
}

ConnectionState Client::getConnectionState() {
    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::GET_CONNECTION_STATE), "{}");

    if (!ok) {
        return ConnectionState::OFFLINE;
    }

    try {
        auto j = nlohmann::json::parse(response_json);
        std::string state_str = j.value("state", "OFFLINE");
        if (state_str == "INIT") return ConnectionState::INIT;
        if (state_str == "CERT_WAIT") return ConnectionState::CERT_WAIT;
        if (state_str == "CONNECTING") return ConnectionState::CONNECTING;
        if (state_str == "CONNECTED") return ConnectionState::CONNECTED;
        if (state_str == "RECONNECTING") return ConnectionState::RECONNECTING;
        return ConnectionState::OFFLINE;
    } catch (const std::exception&) {
        return ConnectionState::OFFLINE;
    }
}

// ============================================================
// CR-007 §4: 订阅快照与 Cloud Readiness
// ============================================================

ReplaceSnapshotResult Client::replaceSubscriptionSnapshot(
        const OwnerSubscriptionSnapshot& snapshot) {
    nlohmann::json params;
    params["owner"] = snapshot.owner;
    params["generation"] = snapshot.generation;
    params["content_digest"] = snapshot.content_digest;
    params["registration_complete"] = snapshot.registration_complete;
    // CR-015 (DSN-CR-015 §4/§18.2): contract_version 必须随快照下发，缺失时
    // daemon 归一化为 0 -> capability route 判定 KEYPROV_CONTRACT_UNSUPPORTED。
    params["contract_version"] = snapshot.contract_version;
    nlohmann::json items = nlohmann::json::array();
    for (const auto& it : snapshot.items) {
        nlohmann::json item;
        item["route_id"] = it.route_id;
        item["topic_template"] = it.topic_template;
        item["direction"] = snapshotDirectionToString(it.direction);
        item["qos"] = static_cast<int>(it.qos);
        item["target"] = it.target;
        item["mandatory"] = it.mandatory;
        // CR-015: enabled 仅在显式设置时序列化，保留 nullopt 的缺省语义
        // （daemon 侧：legacy route 缺省=true，capability route 缺省=false）。
        if (it.enabled.has_value()) {
            item["enabled"] = *it.enabled;
        }
        item["readiness_scope"] = it.readiness_scope;
        item["criticality"] = subscriptionCriticalityToString(it.criticality);
        items.push_back(item);
    }
    params["items"] = items;

    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::REPLACE_SUBSCRIPTION_SNAPSHOT), params.dump());

    ReplaceSnapshotResult result;
    if (!ok) {
        result.status = SnapshotAcceptStatus::UNKNOWN;
        result.reason_code = "transport_failed";
        return result;
    }
    try {
        auto j = nlohmann::json::parse(response_json);
        std::string s = j.value("status", "UNKNOWN");
        if (s == "ACCEPTED") result.status = SnapshotAcceptStatus::ACCEPTED;
        else if (s == "REJECTED") result.status = SnapshotAcceptStatus::REJECTED;
        else result.status = SnapshotAcceptStatus::UNKNOWN;
        result.accepted_generation = j.value("accepted_generation", 0ULL);
        result.reason_code = j.value("reason_code", "");
        if (j.contains("rejected_route_ids") && j["rejected_route_ids"].is_array()) {
            for (const auto& rid : j["rejected_route_ids"]) {
                result.rejected_route_ids.push_back(rid.get<std::string>());
            }
        }
    } catch (...) {
        result.status = SnapshotAcceptStatus::UNKNOWN;
    }
    return result;
}

SnapshotStatusResult Client::getSubscriptionSnapshotStatus(
        const std::string& owner, uint64_t generation) {
    nlohmann::json params;
    params["owner"] = owner;
    params["generation"] = generation;

    auto [ok, status, response_json] = impl_->send_request(
        static_cast<uint32_t>(ipc::MethodId::GET_SUBSCRIPTION_SNAPSHOT_STATUS), params.dump());

    SnapshotStatusResult r;
    if (!ok) { r.status = SnapshotAcceptStatus::UNKNOWN; return r; }
    try {
        auto j = nlohmann::json::parse(response_json);
        std::string s = j.value("status", "UNKNOWN");
        if (s == "ACCEPTED") r.status = SnapshotAcceptStatus::ACCEPTED;
        else if (s == "REJECTED") r.status = SnapshotAcceptStatus::REJECTED;
        else r.status = SnapshotAcceptStatus::UNKNOWN;
        r.generation = j.value("generation", 0ULL);
        r.content_digest = j.value("content_digest", "");
        r.registration_complete = j.value("registration_complete", false);
    } catch (...) {
        r.status = SnapshotAcceptStatus::UNKNOWN;
    }
    return r;
}

CloudReadinessSnapshot Client::getSubscriptionReadiness() {
    return impl_->fetchCloudReadiness();
}

SubscriptionHandle Client::subscribeCloudReadinessChanged(CloudReadinessCallback callback) {
    auto impl = impl_->subscribeCloudReadiness(std::move(callback));
    if (!impl) {
        return SubscriptionHandle{};  // 参数非法：空句柄
    }
    return SubscriptionHandle(std::move(impl));
}

// ============================================================
// CR-015 §18.4: Capability Readiness
// ============================================================
CapabilityReadiness Client::getCapabilityReadiness(std::string_view capability) {
    std::string cap(capability);
    if (cap.empty()) {
        CapabilityReadiness r;
        r.contract_supported = false;
        r.reason_code = "invalid_capability";
        return r;
    }
    return impl_->fetchCapabilityReadiness(cap);
}

SubscriptionHandle Client::subscribeCapabilityReadinessChanged(
        std::string_view capability, CapabilityReadinessCallback callback) {
    std::string cap(capability);
    auto impl = impl_->subscribeCapabilityReadiness(cap, std::move(callback));
    if (!impl) {
        return SubscriptionHandle{};  // 参数非法：空句柄
    }
    return SubscriptionHandle(std::move(impl));
}

} // namespace mqtt
} // namespace tbox
