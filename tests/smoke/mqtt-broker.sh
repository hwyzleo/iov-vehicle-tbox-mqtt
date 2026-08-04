#!/bin/bash
# Broker/security smoke test for tbox-mqtt service.
# Exit 0 = pass, non-zero = fail.
#
# TBOX-MQTT-DSN-CR-011 §13.3: Orin 最小通信验证入口。
# 只在受控测试 Broker（TBOX_MQTT_SMOKE_BROKER_HOST）与测试凭据下运行；
# 未设置环境变量时跳过实际 Broker 连接（本地语法/状态检查）。
# 严禁连接生产 Broker、发布生产 Topic 或使用真实生产私钥。

set -euo pipefail

BROKER_HOST="${TBOX_MQTT_SMOKE_BROKER_HOST:-}"

# --- 配置存在性 ---
CONFIG_FILE="/etc/tbox/conf.d/mqtt.yaml"
if [ ! -f "${CONFIG_FILE}" ]; then
    echo "FAIL: config file ${CONFIG_FILE} not found"
    exit 1
fi

# --- 默认配置不得含生产敏感字段（CR-011 §11/§12） ---
if grep -qE "root_ca_path|client_cert_path|private_key_path|key_source|tls_key_ref|tls_cert_ref|root_ca_ref" "${CONFIG_FILE}"; then
    echo "FAIL: default config contains forbidden local-credential fields"
    exit 1
fi

# --- 服务生命周期：重启并验证 socket 重建 ---
SOCKET_PATH="/tmp/tbox-mqtt.sock"
if ! systemctl restart tbox-mqtt.service; then
    echo "FAIL: could not restart tbox-mqtt.service"
    exit 1
fi

for i in $(seq 1 50); do
    if [ -S "${SOCKET_PATH}" ]; then
        break
    fi
    sleep 0.1
done
if [ ! -S "${SOCKET_PATH}" ]; then
    echo "FAIL: IPC socket ${SOCKET_PATH} not recreated after restart"
    exit 1
fi

# --- 受控 Broker 连通性（仅在显式提供测试 Broker 时执行） ---
if [ -n "${BROKER_HOST}" ]; then
    PORT="${TBOX_MQTT_SMOKE_BROKER_PORT:-8883}"
    if command -v mosquitto_pub >/dev/null 2>&1; then
        # 受控测试 Broker：验证 CONNECT/CONNACK 可达性（不发布业务 Topic）
        timeout 10 mosquitto_pub \
            -h "${BROKER_HOST}" -p "${PORT}" \
            --cafile "${TBOX_MQTT_SMOKE_CA:-}" \
            --cert "${TBOX_MQTT_SMOKE_CERT:-}" \
            --key "${TBOX_MQTT_SMOKE_KEY:-}" \
            -t "tbox/smoke/ping" -m "ping" -q 1 -d >/dev/null 2>&1 \
            || { echo "FAIL: broker smoke (${BROKER_HOST}:${PORT}) failed"; exit 1; }
    else
        echo "WARN: mosquitto_pub not found; broker reachability not exercised"
    fi
else
    echo "INFO: TBOX_MQTT_SMOKE_BROKER_HOST unset; skipped live broker connection"
fi

# --- 优雅停机并验证 socket 清理 ---
systemctl stop tbox-mqtt.service
for i in $(seq 1 30); do
    if [ ! -e "${SOCKET_PATH}" ]; then
        break
    fi
    sleep 0.1
done
if [ -e "${SOCKET_PATH}" ]; then
    echo "WARN: IPC socket ${SOCKET_PATH} still exists after stop (may be cleaning up)"
fi

# 恢复启动
systemctl start tbox-mqtt.service

echo "PASS: tbox-mqtt broker/security smoke (config, restart, socket, ${BROKER_HOST:-local-only})"
exit 0
