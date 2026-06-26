#!/bin/bash
# TBOX MQTT 服务 - 完整验证脚本
# 用法: ./scripts/verify.sh [选项]
#   --clean    清理后重新构建
#   --debug    Debug 模式构建
#   --skip-build   跳过构建步骤
#   --skip-test    跳过测试步骤

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# 默认选项
CLEAN_BUILD=false
BUILD_TYPE="Release"
SKIP_BUILD=false
SKIP_TEST=false

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
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-test)
            SKIP_TEST=true
            shift
            ;;
        *)
            echo "未知选项: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "TBOX MQTT 服务 - 完整验证"
echo "=========================================="
echo "构建类型: $BUILD_TYPE"
echo "=========================================="
echo ""

# 记录开始时间
START_TIME=$(date +%s)

# 步骤 1: 构建
if [ "$SKIP_BUILD" = false ]; then
    echo ">>> 步骤 1/2: 构建项目"
    echo "------------------------------------------"
    
    BUILD_ARGS=""
    if [ "$CLEAN_BUILD" = true ]; then
        BUILD_ARGS="$BUILD_ARGS --clean"
    fi
    BUILD_ARGS="$BUILD_ARGS --$(echo $BUILD_TYPE | tr '[:upper:]' '[:lower:]')"
    
    "$SCRIPT_DIR/build.sh" $BUILD_ARGS
    echo ""
else
    echo ">>> 步骤 1/2: 跳过构建"
    echo ""
fi

# 步骤 2: 测试
if [ "$SKIP_TEST" = false ]; then
    echo ">>> 步骤 2/2: 运行测试"
    echo "------------------------------------------"
    "$SCRIPT_DIR/test.sh" --verbose
    echo ""
else
    echo ">>> 步骤 2/2: 跳过测试"
    echo ""
fi

# 计算耗时
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "=========================================="
echo "验证完成!"
echo "=========================================="
echo "总耗时: ${DURATION}秒"
echo ""

# 显示结果摘要
if [ "$SKIP_TEST" = false ]; then
    echo "测试结果:"
    cd "$PROJECT_DIR/build"
    ctest --output-on-failure 2>&1 | tail -5
fi

echo ""
echo "下一步:"
echo "  - 运行服务: ./build/tbox_mqtt_service"
echo "  - 清理构建: ./scripts/clean.sh"
