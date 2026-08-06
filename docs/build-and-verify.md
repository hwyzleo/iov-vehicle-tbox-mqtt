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

编译成功后，将生成以下文件：
- `tbox_mqtt_service` - 主可执行文件
- `libmqtt_service_lib.a` - 静态库

### 2.3 常见问题排查

如果 CMake 找不到 mosquitto，可能需要手动指定路径：
```bash
# macOS
cmake -DMOSQUITTO_INCLUDE_DIRS=/opt/homebrew/include \
      -DMOSQUITTO_LIBRARY_DIRS=/opt/homebrew/lib ..

# Linux (如果安装在非标准路径)
cmake -DMOSQUITTO_INCLUDE_DIRS=/usr/local/include \
      -DMOSQUITTO_LIBRARY_DIRS=/usr/local/lib ..
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
# 运行连接状态机测试
./test_connection_state_machine

# 运行 MQTT 客户端管理器测试
./test_mqtt_client_manager

# 运行离线缓存队列测试
./test_offline_cache_queue

# 运行路由桥接测试
./test_route_bridge

# 运行配置管理器测试
./test_config_manager

# 运行内部总线接口测试
./test_internal_bus_interface

# 运行集成测试
./test_mqtt_service_integration

# 运行端到端测试
./test_end_to_end
```

### 3.3 测试覆盖率

当前测试套件包含 9 个测试模块，共 63+ 个测试用例：

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

## 4. 运行服务

### 4.1 启动 MQTT 服务
```bash
./tbox_mqtt_service
```

### 4.2 服务输出示例
```
Starting TBOX MQTT Service...
TBOX MQTT Service started successfully
Connection state: 2 -  (CONNECTING 状态)
```

### 4.3 停止服务
按 `Ctrl+C` 发送 SIGINT 信号，服务将优雅关闭。

## 5. 代码质量检查

### 5.1 静态分析（可选）
```bash
# 使用 clang-tidy
clang-tidy src/mqtt_service/src/*.cpp -- -std=c++17

# 使用 cppcheck
cppcheck --enable=all src/mqtt_service/
```

### 5.2 内存检查（可选）
```bash
# 使用 Valgrind（Linux）
valgrind --leak-check=full ./test_connection_state_machine

# 使用 Address Sanitizer
cmake -DCMAKE_CXX_FLAGS="-fsanitize=address" ..
make
./test_connection_state_machine
```

## 6. 常见问题

### 6.1 链接错误：找不到 mosquitto
```
解决方案：
1. 确认 mosquitto 已安装
2. 检查 pkg-config 是否能找到：pkg-config --libs libmosquitto
3. 手动指定路径（见 2.3 节）
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

## 8. 项目结构

```
iov-vehicle-tbox-mqtt/
├── CMakeLists.txt                 # 主构建配置
├── docs/
│   └── build-and-verify.md        # 本文档
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
| GoogleTest | 1.14.0 | 单元测试框架 |

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

## 10. CR-011: TBOX-BUILD Orin 构建接入（v1.0.0+）

### 10.1 CMake 包消费
```bash
# 本地 HOST 开发（统一以 CMAKE_PREFIX_PATH 解析已安装 package）
cmake -B build -DCMAKE_PREFIX_PATH=~/.local -DCMAKE_BUILD_TYPE=Release
cmake --build build

# 完整测试（41 个 suite）
cd build && ctest --output-on-failure
```

### 10.2 双 Staging 安装
```bash
# SDK（公共头/静态库/CMake package）-> 供业务服务消费
cmake --install build --component mqtt-sdk --prefix /tmp/staging/sdk

# Runtime（daemon/systemd unit/非秘密默认配置）-> rootfs
cmake --install build --component mqtt-runtime --prefix /tmp/staging/rootfs
```

### 10.3 Installed-package Consumer 验证
```bash
cmake -S tests/consumer -B build-consumer \
    -DCMAKE_PREFIX_PATH="/tmp/staging/sdk;$HOME/.local"
cmake --build build-consumer
./build-consumer/mqtt_consumer
# OK: tbox::mqtt_client installed-package consumer compiles/links
```

### 10.4 Smoke 测试（Orin 生产验证入口）
```bash
# Health: 二进制/服务/socket 就绪检查
tests/smoke/mqtt-health.sh

# Broker/smoke: 安全扫描 + 受控 test Broker 连通性
# （生产 Brokre 禁止使用；TBOX_MQTT_SMOKE_BROKER_HOST 未设则跳过）
tests/smoke/mqtt-broker.sh
```

### 10.5 跨仓库集成验证
```bash
# BUILD 仓库整体构建（框架->PROV->SEC->MQTT）
cd ../iov-vehicle-tbox-build
python3 -m tbox_build build --set tbox-mqtt-orin --dry-run
# 生产构建（需 Docker arm64 + mosquitto sha256 填充）
# ./ci/build-in-docker.sh --set tbox-mqtt-orin
```

### 10.6 关键变更摘要
- **CMake**: `find_package(TboxFramework/TboxProvClient/TboxSecClient CONFIG)` 取代兄弟目录 find_library
- **组件化 install**: `mqtt-sdk`（sdk staging）/ `mqtt-runtime`（rootfs staging），无内部头/库泄漏
- **依赖回归**: 删除僵尸依赖 protobuf/SQLite3；本地化 base64（移除 framework legacy utils.h）
- **systemd unit**: `packaging/systemd/tbox-mqtt.service`（NoNewPrivileges/ProtectSystem/ReadWritePaths）
- **生产安全默认配置**: `config/mqtt.default.yaml`（无生产 Broker 地址/凭据/身份实例值，无顶层 `common:`）
- **配置分层（CR-012）**: `config/dev/`（调试，不安装）/ `config/schema/mqtt.schema.yaml`（声明式 schema）/ `config/mqtt.default.yaml`（唯一发布模板）；纯校验命令 `tools/mqtt-config-check.py`（schema + production/test profile，无副作用）
- **INSTALLED export**: `tbox::mqtt_client`, `TboxMqttClientConfig.cmake` + version 文件
- **Consumer/smoke**: `tests/consumer/` + `tests/smoke/mqtt-health.sh` + `tests/smoke/mqtt-broker.sh`
