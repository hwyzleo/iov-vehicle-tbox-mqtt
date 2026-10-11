# TBOX-MQTT-DSN-CR-016 实施记录（SEC/PROV IPC client 自动恢复）

- 需求：`TBOX-MQTT-REQ-CR-016`
- 设计：`TBOX-MQTT-DSN-CR-016`
- 范围：SEC/PROV IPC client 自动重连与重试计数修复；systemd notify 评估；日志 flush 阶段方案。

## 1. 根因

现场 `CERT_WAIT/PROV_WAIT` 退避循环只重复业务查询，不恢复失效的 IPC client：

- `ProvisionIdentityManager::initialize()` / `CredentialManager::initialize()` 启动时只建立一次
  `prov_client_`/`sec_client_`；首次 `connect()` 失败后仍返回成功启动。
- `readBinding()` / `getTlsCredential()` 首行 `if (!client_ || !client_->is_connected()) return false;`
  早退，永不重连，形成永久自锁，只能重启 `tbox-mqtt`。
- 日志中的 `retry_count` 取自 Broker 重连计数，CERT_WAIT/PROV_WAIT 的实际轮询计数从未输出，
  现场恒为 0。
- framework-ipc `Client::call()` 自带惰性连接与一次重连，但被管理器的 `is_connected()` 早退守卫短路。

## 2. 实现

| 设计节 | 实现 |
| --- | --- |
| §3 统一依赖恢复器 | 新增 `src/mqtt_service/include/dependency_recovery.h`（`Dependency`/`IpcState`/`RetryState`/有界退避） |
| §4 CERT_WAIT | `CredentialManager::ensureConnected()`：探测 → 失效旧 transport → `client_generation++` → 重连；`MqttService` 在 CERT_WAIT 先恢复再查询 |
| §5 PROV_WAIT | `ProvisionIdentityManager::ensureConnected()`：同算法；`readBinding` 前先恢复，并用 client 连接状态区分传输故障与 `NOT_BOUND` |
| §6 退避与计数 | 每次实际尝试递增 `retry_count`/`total_attempts`；成功离态 `markDependencyReady()` 重置 `retry_count`，封顶后继续增长 |
| §7 generation guard | 每次重建 transport 递增 `client_generation`；查询结果提交前核对，旧 generation 丢弃并记录 `mqtt_dependency_stale_callback_dropped`；IPC 调用移出锁（不再持锁调用可能回调的 IPC） |
| §8 日志 | 新增 `mqtt_dependency_ipc_connect_attempt / ipc_connect_failed / ipc_connected / query_failed / not_ready / ready / stale_callback_dropped` 与 `dependency` logger；`mqtt.provision.waiting`/`mqtt.certificate.waiting` 输出依赖 `retry_count/total_attempts/backoff_ms/client_generation/reason_code` |
| §9 systemd notify | 见 §3 评估结论 |
| §10 日志 flush | 见 §4 阶段方案 |

稳定 reason：`sec_not_connected`、`sec_connect_failed`、`sec_credential_not_ready`、
`prov_not_connected`、`prov_connect_failed`、`prov_binding_not_ready`、
`dependency_query_transport_failed`。外部错误码继续沿用 MQTT-1001/MQTT-1009，不静默改码。

## 3. systemd Type=notify 评估结论

**本期保留 `Type=simple`，不迁移 notify。**

- 现有 `hwyz::Application` 未提供“IPC server bind/listen 且事件循环运行”就绪回调，
  MQTT 缺少可靠接入点；在 `execute()` 盲目 `sd_notify("READY=1")` 会把进程启动等同于 IPC ready。
- 目标 rootfs 的 `libsystemd`/`sd_notify` 可用性与链接策略尚未验证；非 systemd 测试环境兼容也需评估。
- 设计 §9.3 明确允许“暂不迁移，保留 Type=simple，先交付 client 重连修复”。

保留 `After=tbox-prov.service tbox-sec.service`（`After=` 只约束进程启动顺序，不能证明对端 IPC
可接受连接，也不能替代应用层重连）。`Type=notify` + `sd_notify(READY=1)` 另建独立 CR。

## 4. 日志 flush 阶段方案（收敛）

原表述“framework-log 配置 warn/error 即时 flush”收敛为阶段方案：

1. **本 CR（MQTT 侧）**：仅调用 framework-log 公开 `Logger::flush()`，经
   `MqttLogAdapter::flushCritical()` 集中在关键路径落盘：
   `mqtt_dependency_ipc_connect_failed`、`mqtt_dependency_query_failed`、
   `mqtt_dependency_not_ready` 以及 Broker 连接失败。不逐条 info 刷盘，不访问 framework-log 私有 sink。
   graceful shutdown 的最终 flush 由 `hwyz::Application::run()` 统一完成。
2. **另建 framework REQ/DSN CR**：`flush_on(warn)`、周期 `flush_every`、统一 sink 策略与多 logger
   single-flusher 保证，由 framework-log 提供配置并对所有服务生效。

## 5. 测试

新增/扩展测试（`ctest` 全量 51 项通过）：

- `tests/unit/test_dependency_recovery.cpp`：退避倍率/封顶/`RetryState` 默认值。
- `tests/unit/test_credential_manager.cpp`、`test_provision_identity_manager.cpp`：失败尝试计数、
  成功重置、reason 区分、generation guard 原语、模拟模式健康。
- `tests/integration/test_mqtt_dependency_recovery.cpp`：最小 stub framework-ipc Server
  （method id 见 `tests/support/dependency_stub_protocol.h`，对应 PROV `READ_BINDING=3`、
  SEC `GET_TLS_CREDENTIAL=14`）覆盖 SEC/PROV 晚启动、首次连接失败、对端运行期重启、
  generation 防旧回调；无需重启 MQTT。
- `tests/integration/test_mqtt_dependency_service_recovery.cpp`：`MqttService` + 真实 PROV client +
  stub PROV Server，验证服务无需重启即离开 PROV_WAIT。
- `tests/unit/test_mqtt_dependency_flush.cpp`：warn 事件经 `flushCritical()` 后可在限定时窗内从文件读取。
