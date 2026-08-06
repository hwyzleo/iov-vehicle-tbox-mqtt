# MQTT 配置目录

本目录遵循 **TBOX-MQTT-DSN-CR-012 §17** 的配置分层设计，分离开发调试、服务默认
模板、校验 schema 三类资产。

## 目录结构

```
config/
├── dev/                       # 开发调试（DEVELOPMENT-ONLY，不安装）
│   ├── common.yaml            #   调试用 common（framework-config layer 0）
│   └── mqtt.yaml              #   调试用 mqtt 覆盖（测试 Broker/mock endpoint）
├── schema/
│   └── mqtt.schema.yaml       # 声明式 schema（字段/类型/范围/禁止字段）
└── mqtt.default.yaml          # 唯一发布默认模板（安装为 /etc/tbox/conf.d/mqtt.yaml）
```

## 三层加载顺序（framework-config，保持不变）

```
/etc/tbox/common.yaml          # BUILD 唯一提供（common 所有权归 BUILD）
-> /etc/tbox/conf.d/mqtt.yaml  # mqtt-runtime 安装 mqtt.default.yaml 生成
-> ./mqtt.yaml                 # 工作目录覆盖（仅测试/研发）
```

## 各文件用途与边界

| 文件 | 安装 | 用途 |
|---|---|---|
| `mqtt.default.yaml` | ✅ mqtt-runtime -> `/etc/tbox/conf.d/mqtt.yaml` | 唯一发布默认模板，仅含非秘密 mqtt 默认值，**不含顶层 `common:`** |
| `schema/mqtt.schema.yaml` | ❌（BUILD 源码引用） | 声明式 schema，供 `tools/mqtt-config-check.py` 与 BUILD metadata 引用 |
| `dev/common.yaml` | ❌ | 调试用 common（store/ipc/log）；显式 `config_root=config/dev/` 启动时生效 |
| `dev/mqtt.yaml` | ❌ | 调试用 mqtt 覆盖（测试 Broker）；复制为 `./mqtt.yaml` 或显式引用 |

## 本地开发运行

```bash
# 方式 1：显式指定调试配置根
./build/tbox_mqtt config/dev/

# 方式 2：工作目录覆盖（framework-config 第三层，最高优先级）
cp config/dev/mqtt.yaml ./mqtt.yaml
./build/tbox_mqtt
```

> 注意：CR-012 已将调试文件从 `config/common.yaml`、`config/mqtt.yaml` 迁移至
> `config/dev/`。framework PathResolver 不再自动加载 `./config/common.yaml` 与
> `./config/mqtt.yaml` 作为项目层覆盖，避免调试文件进入正式加载链。

## 校验

```bash
# 校验默认模板（production profile）
python3 tools/mqtt-config-check.py --profile production config/mqtt.default.yaml

# 校验 BUILD Orin overlay 后的最终配置
python3 tools/mqtt-config-check.py --profile production /etc/tbox/conf.d/mqtt.yaml
```

校验命令为纯无副作用检查：无网络/DNS/Broker/PROV/SEC/HSM/socket/store/queue
写入/身份/credential 读取，具有稳定退出码（0=pass, 1=schema fail, 2=profile fail,
3=usage），调用方可用 `timeout` 包裹。

## BUILD Orin Overlay

BUILD 在 staging 阶段检查 `configs/orin/rootfs/etc/tbox/conf.d/mqtt.yaml`：
- 不存在则保留 mqtt-runtime 安装的默认模板；
- 存在则**整文件覆盖**同一 staging 目标（v0.1 不做 YAML 深度合并）；
- 记录 default/platform/final 来源与 SHA-256，执行 schema、production profile、
  路径/资源检查和安全扫描后打包。

MQTT 仓库不维护 `mqtt.orin.yaml` 或 `config/platform/orin/mqtt.yaml`。
