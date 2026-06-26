# TBOX MQTT 服务 - 构建与验证指南

## 1. 环境要求

### 1.1 开发工具
- **CMake**: >= 3.16
- **C++ 编译器**: 支持 C++17（GCC >= 7, Clang >= 5, Apple Clang >= 10）
- **Git**: 用于获取 GoogleTest 依赖

### 1.2 系统依赖

#### macOS (Homebrew)
```bash
brew install openssl mosquitto sqlite3 nlohmann-json protobuf pkg-config
```

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install -y \
    libssl-dev \
    libmosquitto-dev \
    libsqlite3-dev \
    nlohmann-json3-dev \
    libprotobuf-dev \
    protobuf-compiler \
    pkg-config \
    libgtest-dev
```

#### CentOS/RHEL
```bash
sudo yum install -y \
    openssl-devel \
    mosquitto-devel \
    sqlite-devel \
    json-devel \
    protobuf-devel \
    protobuf-compiler \
    pkgconfig \
    gtest-devel
```

## 2. 快速开始

### 2.1 使用脚本（推荐）

```bash
# 首次构建
./scripts/build.sh --clean

# 运行测试
./scripts/test.sh

# 快速验证（自动检测更新）
./scripts/quick.sh

# 完整验证（构建+测试）
./scripts/verify.sh

# 清理构建
./scripts/clean.sh
```

### 2.2 手动构建

```bash
# 克隆项目
git clone <repository-url>
cd iov-vehicle-tbox-mqtt

# 创建构建目录
mkdir -p build
cd build

# 配置 CMake
cmake ..

# 编译
make -j$(nproc)
```

## 3. 脚本使用说明

### 3.1 build.sh - 构建脚本
```bash
./scripts/build.sh              # Release 模式构建
./scripts/build.sh --debug      # Debug 模式构建
./scripts/build.sh --clean      # 清理后重新构建
```

### 3.2 test.sh - 测试脚本
```bash
./scripts/test.sh               # 运行所有测试
./scripts/test.sh --unit        # 仅运行单元测试
./scripts/test.sh --integration # 仅运行集成测试
./scripts/test.sh --verbose     # 显示详细输出
./scripts/test.sh --filter "Mqtt*"  # 运行匹配的测试
```

### 3.3 clean.sh - 清理脚本
```bash
./scripts/clean.sh              # 清理构建目录
./scripts/clean.sh --all        # 清理所有生成文件
```

### 3.4 verify.sh - 完整验证脚本
```bash
./scripts/verify.sh             # 完整构建和测试
./scripts/verify.sh --clean     # 清理后验证
./scripts/verify.sh --debug     # Debug 模式验证
./scripts/verify.sh --skip-test # 仅构建不测试
```

### 3.5 quick.sh - 快速验证脚本
```bash
./scripts/quick.sh              # 快速构建并测试（自动检测更新）
```

## 4. 测试验证

### 4.1 运行所有测试
```bash
cd build
ctest --output-on-failure
```

### 4.2 运行单个测试
```bash
# 连接状态机测试
./test_connection_state_machine

# MQTT 客户端管理器测试
./test_mqtt_client_manager

# 离线缓存队列测试
./test_offline_cache_queue

# 路由桥接测试
./test_route_bridge

# 配置管理器测试
./test_config_manager

# 内部总线接口测试
./test_internal_bus_interface

# 集成测试
./test_mqtt_service_integration

# 端到端测试
./test_end_to_end
```

### 4.3 测试覆盖率

| 测试模块 | 测试数量 | 覆盖范围 |
|---------|---------|---------|
| ConnectionStateMachineTest | 11 | 状态转换、指数退避、回调 |
| SecCertInterfaceTest | 5 | 证书管理、句柄获取 |
| MqttClientManagerTest | 11 | 初始化、连接、发布、订阅 |
| OfflineCacheQueueTest | 7 | 优先级队列、丢弃策略 |
| RouteBridgeTest | 6 | 路由注册、查找、删除 |
| ConfigManagerTest | 6 | 配置验证、版本管理 |
| InternalBusInterfaceTest | 5 | 服务注册、消息发送 |
| MqttServiceIntegrationTest | 7 | 服务集成、生命周期 |
| EndToEndTest | 5 | 完整流程、离线缓存 |

## 5. 运行服务

### 5.1 启动 MQTT 服务
```bash
./build/tbox_mqtt_service
```

### 5.2 服务输出示例
```
Starting TBOX MQTT Service...
TBOX MQTT Service started successfully
Connection state: 2 -  (CONNECTING 状态)
```

### 5.3 停止服务
按 `Ctrl+C` 发送 SIGINT 信号，服务将优雅关闭。

## 6. 常见问题

### 6.1 链接错误：找不到 mosquitto
```
解决方案：
1. 确认 mosquitto 已安装
2. 检查 pkg-config 是否能找到：pkg-config --libs libmosquitto
3. 手动指定路径：
   cmake -DMOSQUITTO_INCLUDE_DIRS=/path/to/include \
         -DMOSQUITTO_LIBRARY_DIRS=/path/to/lib ..
```

### 6.2 测试超时
```
端到端测试包含 2 秒等待时间，这是正常的。
如果其他测试超时，可能是系统资源问题。
```

### 6.3 编译警告
```
项目使用 -Wall -Wextra 编译选项。
如果出现警告，可能是未使用的变量或类型转换问题。
这些警告不影响功能，但建议修复。
```

## 7. 项目结构

```
iov-vehicle-tbox-mqtt/
├── CMakeLists.txt                 # 主构建配置
├── BUILD_AND_VERIFY.md            # 本文档
├── scripts/                       # 构建脚本
│   ├── build.sh                   # 构建脚本
│   ├── test.sh                    # 测试脚本
│   ├── clean.sh                   # 清理脚本
│   ├── verify.sh                  # 完整验证脚本
│   └── quick.sh                   # 快速验证脚本
├── src/
│   ├── main.cpp                   # 主程序入口
│   └── mqtt_service/
│       ├── include/               # 头文件
│       ├── src/                   # 源文件
│       └── types/                 # 类型定义
├── tests/
│   ├── CMakeLists.txt             # 测试构建配置
│   ├── unit/                      # 单元测试
│   └── integration/               # 集成测试
└── docs/
    └── superpowers/
        └── plans/                 # 开发计划
```

## 8. 依赖版本

| 依赖 | 版本 | 用途 |
|-----|------|------|
| OpenSSL | >= 1.1 | TLS/mTLS 加密 |
| Mosquitto | >= 2.0 | MQTT 客户端库 |
| SQLite3 | >= 3.0 | 离线消息缓存 |
| nlohmann/json | >= 3.0 | 配置文件解析 |
| Protobuf | >= 3.0 | 消息序列化 |
| GoogleTest | 1.14.0 | 单元测试框架

## 9. 开发工作流

### 9.1 添加新功能
1. 在 `src/mqtt_service/include/` 添加头文件
2. 在 `src/mqtt_service/src/` 添加实现
3. 在 `tests/unit/` 添加单元测试
4. 更新 `tests/CMakeLists.txt`
5. 运行测试验证

### 9.2 修复 Bug
1. 编写失败的测试用例
2. 修复 Bug
3. 验证测试通过
4. 提交代码

### 9.3 提交前检查清单
- [ ] 运行 `./scripts/verify.sh` 全部通过
- [ ] 无编译警告
- [ ] 符合代码规范
- [ ] 文档已更新（如需要）
