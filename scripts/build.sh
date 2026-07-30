#!/bin/bash
# TBOX MQTT 服务 - 构建脚本
# 用法: ./scripts/build.sh [选项]
#   --clean    清理后重新构建
#   --debug    Debug 模式构建
#   --release  Release 模式构建

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

# 构建环境单一来源：由 framework 仓库提供 TBOX_PREFIX / TBoxFramework_DIR
# 切换本地开发与生产环境只需覆盖环境变量，例如：
#   TBOX_PREFIX=/opt/tbox ./scripts/build.sh
TBOX_ENV_FILE="$PROJECT_DIR/../iov-vehicle-tbox-framework/scripts/tbox-env.sh"
if [ ! -f "$TBOX_ENV_FILE" ]; then
    echo "[ERROR] 未找到 $TBOX_ENV_FILE" >&2
    echo "        请确认 iov-vehicle-tbox-framework 与本项目位于同级目录" >&2
    exit 1
fi
source "$TBOX_ENV_FILE"

# 默认选项
BUILD_TYPE="Release"
CLEAN_BUILD=false

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN_BUILD=true
            shift
            ;;
        --debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        --release)
            BUILD_TYPE="Release"
            shift
            ;;
        *)
            echo "未知选项: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "TBOX MQTT 服务 - 构建"
echo "=========================================="
echo "构建类型: $BUILD_TYPE"
echo "项目目录: $PROJECT_DIR"
echo "构建目录: $BUILD_DIR"
echo "=========================================="

# 清理构建目录
if [ "$CLEAN_BUILD" = true ]; then
    echo "[1/4] 清理构建目录..."
    rm -rf "$BUILD_DIR"
else
    echo "[1/4] 准备构建目录..."
fi

# 创建构建目录
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# 配置 CMake
echo "[2/4] 配置 CMake..."
cmake -DCMAKE_BUILD_TYPE="$BUILD_TYPE" -DTBoxFramework_DIR="$TBoxFramework_DIR" ..

# 编译
echo "[3/4] 编译中..."
make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# 完成
echo "[4/4] 构建完成!"
echo ""
echo "生成的文件:"
echo "  - 可执行文件: $BUILD_DIR/tbox_mqtt_service"
echo "  - 静态库: $BUILD_DIR/libmqtt_service_lib.a"
echo ""
echo "下一步:"
echo "  - 运行测试: ./scripts/test.sh"
echo "  - 完整验证: ./scripts/verify.sh"
