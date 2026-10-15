// 进程级 daemon epoch 实现 (CR-017 §3)
#include "daemon_epoch.h"

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <random>

#if defined(__linux__)
#include <sys/random.h>
#endif

namespace tbox {
namespace mqtt {

namespace {

constexpr size_t kEpochBytes = 16;  // 128-bit

/// 优先使用系统安全随机源生成 128-bit；失败返回空串，绝不退化为固定值。
std::string generateEpoch() {
    std::array<unsigned char, kEpochBytes> buf{};
    bool ok = false;

#if defined(__linux__)
    size_t got = 0;
    while (got < buf.size()) {
        ssize_t n = ::getrandom(buf.data() + got, buf.size() - got, 0);
        if (n > 0) {
            got += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        break;
    }
    ok = (got == buf.size());
#endif

    if (!ok) {
        std::ifstream urandom("/dev/urandom", std::ios::binary);
        if (urandom) {
            urandom.read(reinterpret_cast<char*>(buf.data()),
                         static_cast<std::streamsize>(buf.size()));
            ok = urandom.gcount() == static_cast<std::streamsize>(buf.size());
        }
    }

    if (!ok) {
        try {
            std::random_device rd;
            for (size_t i = 0; i < buf.size(); i += sizeof(uint32_t)) {
                uint32_t v = rd();
                for (size_t j = 0; j < sizeof(uint32_t) && i + j < buf.size(); ++j) {
                    buf[i + j] = static_cast<unsigned char>((v >> (8 * j)) & 0xFF);
                }
            }
            ok = true;
        } catch (...) {
            ok = false;
        }
    }

    if (!ok) return {};

    // 全零视为随机源异常，拒绝作为 epoch
    bool all_zero = true;
    for (unsigned char c : buf) {
        if (c != 0) { all_zero = false; break; }
    }
    if (all_zero) return {};

    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(kEpochBytes * 2);
    for (unsigned char c : buf) {
        out.push_back(kHex[c >> 4]);
        out.push_back(kHex[c & 0x0F]);
    }
    return out;
}

} // namespace

const std::string& daemonEpoch() {
    // C++11 线程安全 static 初始化：进程内只生成一次。
    static const std::string epoch = generateEpoch();
    return epoch;
}

bool daemonEpochValid() {
    return !daemonEpoch().empty();
}

} // namespace mqtt
} // namespace tbox
