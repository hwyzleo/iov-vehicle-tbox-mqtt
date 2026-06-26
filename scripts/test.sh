#!/bin/bash
# TBOX MQTT 服务 - 测试脚本
# 用法: ./scripts/test.sh [选项]
#   --unit         仅运行单元测试
#   --integration  仅运行集成测试
#   --verbose      显示详细输出
#   --filter PATTERN  运行匹配模式的测试

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

# 默认选项
RUN_UNIT=true
RUN_INTEGRATION=true
VERBOSE=false
FILTER=""

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --unit)
            RUN_INTEGRATION=false
            shift
            ;;
        --integration)
            RUN_UNIT=false
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --filter)
            FILTER="$2"
            shift 2
            ;;
        *)
            echo "未知选项: $1"
            exit 1
            ;;
    esac
done

# 检查构建目录
if [ ! -d "$BUILD_DIR" ]; then
    echo "错误: 构建目录不存在，请先运行 ./scripts/build.sh"
    exit 1
fi

cd "$BUILD_DIR"

echo "=========================================="
echo "TBOX MQTT 服务 - 测试"
echo "=========================================="

# 构建测试命令
CTEST_CMD="ctest --output-on-failure"

if [ "$VERBOSE" = true ]; then
    CTEST_CMD="$CTEST_CMD -V"
fi

if [ -n "$FILTER" ]; then
    CTEST_CMD="$CTEST_CMD -R $FILTER"
fi

# 运行测试
if [ "$RUN_UNIT" = true ] && [ "$RUN_INTEGRATION" = true ]; then
    echo "运行所有测试..."
    echo ""
    $CTEST_CMD
elif [ "$RUN_UNIT" = true ]; then
    echo "运行单元测试..."
    echo ""
    $CTEST_CMD -R "^(ConnectionStateMachine|SecCertInterface|MqttClientManager|OfflineCacheQueue|RouteBridge|ConfigManager|InternalBusInterface|MqttServiceIntegration)"
elif [ "$RUN_INTEGRATION" = true ]; then
    echo "运行集成测试..."
    echo ""
    $CTEST_CMD -R "^EndToEnd"
fi

echo ""
echo "=========================================="
echo "测试完成!"
echo "=========================================="
