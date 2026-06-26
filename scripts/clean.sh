#!/bin/bash
# TBOX MQTT 服务 - 清理脚本
# 用法: ./scripts/clean.sh [选项]
#   --all    清理所有生成文件（包括缓存）
#   --build  仅清理构建目录

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

# 默认选项
CLEAN_BUILD=true
CLEAN_CACHE=false

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            CLEAN_CACHE=true
            shift
            ;;
        --build)
            CLEAN_CACHE=false
            shift
            ;;
        *)
            echo "未知选项: $1"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "TBOX MQTT 服务 - 清理"
echo "=========================================="

# 清理构建目录
if [ "$CLEAN_BUILD" = true ]; then
    echo "清理构建目录..."
    rm -rf "$BUILD_DIR"
    echo "  已删除: $BUILD_DIR"
fi

# 清理缓存
if [ "$CLEAN_CACHE" = true ]; then
    echo "清理缓存文件..."
    
    # 清理 CMake 缓存
    find "$PROJECT_DIR" -name "CMakeCache.txt" -delete 2>/dev/null || true
    find "$PROJECT_DIR" -name "CMakeFiles" -type d -exec rm -rf {} + 2>/dev/null || true
    
    # 清理编译缓存
    find "$PROJECT_DIR" -name "*.o" -delete 2>/dev/null || true
    find "$PROJECT_DIR" -name "*.a" -delete 2>/dev/null || true
    
    # 清理测试缓存
    rm -rf "$PROJECT_DIR/Testing" 2>/dev/null || true
    rm -rf "$PROJECT_DIR/.cache" 2>/dev/null || true
    
    echo "  已清理缓存文件"
fi

echo ""
echo "清理完成!"
echo ""
echo "下一步:"
echo "  - 重新构建: ./scripts/build.sh"
echo "  - 完整验证: ./scripts/verify.sh"
