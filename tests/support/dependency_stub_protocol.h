#pragma once

//
// TBOX-MQTT-DSN-CR-016 §11（测试专用）
//
// SEC/PROV IPC method id 兼容头。
//
// 为什么单独成文件：SEC/PROV SDK 只安装公开 facade（client.h/types.h/errors.h），
// 其内部 ipc_protocol.h 未随 SDK 安装。集成测试需要构造最小 stub framework-ipc
// Server，必须与服务端/客户端共享的 wire method id。为避免 magic number 散落
// 测试代码，这里集中定义。
//
// 与当前 SDK wire contract 的对应版本：
//   - TBOX-PROV-SPEC  ipc::MethodId::READ_BINDING              = 3
//   - TBOX-SEC-SPEC   ipc::MethodId::GET_TLS_CREDENTIAL        = 14
//   - TBOX-SEC-SPEC   ipc::MethodId::GET_TLS_CREDENTIAL_STATE  = 15
//
// 这些 id 是 wire contract（服务端与 client 必须一致，不重编号）。
// 若上游 method id 变更，本文件与对应用例必须同步更新。

#include <cstdint>

namespace tbox {
namespace mqtt {
namespace test {

constexpr uint32_t kProvReadBinding = 3;
constexpr uint32_t kSecGetTlsCredential = 14;
constexpr uint32_t kSecGetTlsCredentialState = 15;

} // namespace test
} // namespace mqtt
} // namespace tbox
