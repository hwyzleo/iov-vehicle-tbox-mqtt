#!/bin/bash
# TBOX MQTT 服务 - 快速验证脚本
# 用法: ./scripts/quick.sh
# 快速构建并运行测试，适合日常开发

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

echo "=========================================="
echo "TBOX MQTT 服务 - 快速验证"
echo "=========================================="
echo ""

# 检查是否需要重新构建
NEED_BUILD=false

if [ ! -d "$BUILD_DIR" ]; then
    NEED_BUILD=true
elif [ ! -f "$BUILD_DIR/tbox_mqtt_service" ]; then
    NEED_BUILD=true
fi

# 检查源文件是否有更新
if [ "$NEED_BUILD" = false ]; then
    LATEST_SOURCE=$(find "$PROJECT_DIR/src" -name "*.cpp" -o -name "*.h" | xargs ls -t 2>/dev/null | head -1)
    LATEST_BUILD=$(find "$BUILD_DIR" -name "*.o" | xargs ls -t 2>/dev/null | head -1)
    
    if [ -n "$LATEST_SOURCE" ] && [ -n "$LATEST_BUILD" ]; then
        if [ "$LATEST_SOURCE" -nt "$LATEST_BUILD" ]; then
            NEED_BUILD=true
        fi
    fi
fi

# 构建
if [ "$NEED_BUILD" = true ]; then
    echo "检测到源文件更新，重新构建..."
    "$SCRIPT_DIR/build.sh"
    echo ""
else
    echo "构建文件已是最新，跳过构建。"
    echo ""
fi

# 运行测试
echo "运行测试..."
echo "------------------------------------------"
cd "$BUILD_DIR"
ctest --output-on-failure

echo ""
echo "=========================================="
echo "快速验证完成!"
echo "=========================================="
