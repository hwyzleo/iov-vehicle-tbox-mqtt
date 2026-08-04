#!/bin/bash
# Health check for tbox-mqtt service.
# Exit 0 = healthy, non-zero = unhealthy.
#
# TBOX-MQTT-DSN-CR-011 §13.3: MQTT 特有健康检查入口。
# 不连接生产 Broker、不发布生产 Topic、不读取任何凭据。

set -euo pipefail

# Check if the tbox_mqtt binary exists
if [ ! -x /usr/bin/tbox_mqtt ]; then
    echo "ERROR: tbox_mqtt binary not found at /usr/bin/tbox_mqtt"
    exit 1
fi

# Check if the systemd service is active
if ! systemctl is-active --quiet tbox-mqtt.service; then
    echo "ERROR: tbox-mqtt.service is not active"
    exit 1
fi

# Check if the IPC socket exists
SOCKET_PATH="/tmp/tbox-mqtt.sock"
if [ ! -S "${SOCKET_PATH}" ]; then
    echo "ERROR: IPC socket ${SOCKET_PATH} not found"
    exit 1
fi

echo "OK: tbox-mqtt service is healthy (binary, service, socket)"
exit 0
