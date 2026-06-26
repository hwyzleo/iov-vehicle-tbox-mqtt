# 脚本目录

本目录包含 TBOX MQTT 服务的构建和验证脚本。

## 脚本列表

### build.sh - 构建脚本
```bash
./scripts/build.sh              # Release 模式构建
./scripts/build.sh --debug      # Debug 模式构建
./scripts/build.sh --clean      # 清理后重新构建
```

### test.sh - 测试脚本
```bash
./scripts/test.sh               # 运行所有测试
./scripts/test.sh --unit        # 仅运行单元测试
./scripts/test.sh --integration # 仅运行集成测试
./scripts/test.sh --verbose     # 显示详细输出
./scripts/test.sh --filter "Mqtt*"  # 运行匹配的测试
```

### clean.sh - 清理脚本
```bash
./scripts/clean.sh              # 清理构建目录
./scripts/clean.sh --all        # 清理所有生成文件
```

### verify.sh - 完整验证脚本
```bash
./scripts/verify.sh             # 完整构建和测试
./scripts/verify.sh --clean     # 清理后验证
./scripts/verify.sh --debug     # Debug 模式验证
./scripts/verify.sh --skip-test # 仅构建不测试
```

### quick.sh - 快速验证脚本
```bash
./scripts/quick.sh              # 快速构建并测试（自动检测更新）
```

## 使用场景

| 场景 | 推荐脚本 |
|-----|---------|
| 首次构建 | `./scripts/build.sh --clean` |
| 日常开发 | `./scripts/quick.sh` |
| 运行特定测试 | `./scripts/test.sh --filter "测试名"` |
| 提交前验证 | `./scripts/verify.sh` |
| 清理环境 | `./scripts/clean.sh --all` |

## 注意事项

1. 所有脚本必须在项目根目录执行
2. 首次运行需要确保系统依赖已安装（参见 `BUILD_AND_VERIFY.md`）
3. 脚本使用 `set -e`，遇到错误会立即停止
