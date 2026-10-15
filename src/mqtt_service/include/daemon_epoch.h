#pragma once

// 进程级 daemon epoch (CR-017 §3, §19.2)
//
// daemon 启动时生成一次、进程内保持不变的 128-bit 随机标识，用于让调用方缓存的
// 会话上下文在 daemon 重启后失效（不能误复用）。编码为固定的 32 位小写 hex 字符串。
//
// 所有权设计：epoch 由本模块的进程级单例持有（C++11 线程安全 static 初始化），
// 不依赖「CloudReadinessGate 恰好只构造一次」。Gate/TLS context/Broker 重连重建
// 都不会改变 epoch；连接与证书变化由 connection/cert session generation 表达。
//
// epoch 不是秘密，也不是授权凭据；仅用于一致性校验。

#include <string>

namespace tbox {
namespace mqtt {

/// 返回进程级 daemon epoch；随机源失败时返回空串（绝不退化为固定值）。
const std::string& daemonEpoch();

/// epoch 是否可用（随机源成功生成）。
bool daemonEpochValid();

} // namespace mqtt
} // namespace tbox
