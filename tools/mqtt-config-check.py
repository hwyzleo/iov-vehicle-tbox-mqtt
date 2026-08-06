#!/usr/bin/env python3
# =============================================================================
# TBOX-MQTT-DSN-CR-012 §17.6: MQTT 配置纯校验命令
# =============================================================================
# 校验单个 MQTT 配置文件（默认 /etc/tbox/conf.d/mqtt.yaml 或 BUILD overlay）
# 是否符合 config/schema/mqtt.schema.yaml 与 production/test profile 规则。
#
# 纯无副作用检查（REQ-007 / §17.6）：
#   - 无网络 / DNS / Broker 连接
#   - 无 PROV / SEC / HSM 调用
#   - 无 socket bind
#   - 无 framework-store / queue 写入
#   - 无设备身份或 credential 读取
# 命中日志只记录规则、文件与字段路径，不回显配置值（REQ-010 / §11）。
#
# 退出码（稳定）：
#   0 = 通过
#   1 = schema 校验失败（字段/类型/范围/必填/未知/禁止字段）
#   2 = profile 校验失败（production 规则 / 安全扫描 / 路径规范化）
#   3 = 用法错误 / 依赖缺失 / 文件不可读
#
# 调用方可用 timeout 包裹，例如：
#   timeout 30 python3 tools/mqtt-config-check.py --profile production config/mqtt.default.yaml
# =============================================================================

import argparse
import os
import re
import sys
from typing import Any

try:
    import yaml
except ImportError:
    sys.stderr.write(
        "ERROR: PyYAML is required (pip install pyyaml). "
        "Cannot parse YAML config without it.\n"
    )
    sys.exit(3)


class _ArgParser(argparse.ArgumentParser):
    """argparse 默认对用法错误返回 exit=2，与 profile fail(2) 冲突；
    覆盖为 exit=3（usage error）。"""

    def error(self, message):
        sys.stderr.write(f"{self.prog}: error: {message}\n")
        sys.exit(3)


# ---------------------------------------------------------------------------
# 路径定位
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DEFAULT_SCHEMA = os.path.join(REPO_ROOT, "config", "schema", "mqtt.schema.yaml")


# ---------------------------------------------------------------------------
# 安全扫描规则（值匹配即失败；不回显值，只记录字段路径）
# ---------------------------------------------------------------------------

# PEM 私钥 / 证书标记
RE_PEM_PRIVATE = re.compile(r"-----BEGIN[^-]*PRIVATE KEY-----")
RE_PEM_CERT = re.compile(r"-----BEGIN CERTIFICATE-----")

# 完整设备 Topic：vehicle/<非模板实例>/...（模板变量 {ecu_uid} 合法，实例值非法）
RE_FULL_DEVICE_TOPIC = re.compile(r"^vehicle/[A-Za-z0-9]{4,}/")

# VIN 实例（17 位，不含 I/O/Q）-- 启发式，只对明显实例报错
RE_VIN_INSTANCE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")

# 测试 CA / client key / fixture 路径
RE_TEST_CERT_PATH = re.compile(r"(^|/)(tests?|fixtures?|test-data)/.*\.(pem|crt|key|csr)$", re.I)

# mock endpoint 标记
RE_MOCK = re.compile(r"\bmock\b", re.I)

# 不受控 / 个人 / build tree 路径前缀
UNCONTROLLED_PATH_PREFIXES = ("/home/", "/Users/", "~/", "../", "..\\", "/build/")


def path_is_safe(value: str, profile: str) -> list[str]:
    """路径规范化校验，返回违规原因列表（空列表=安全）。"""
    reasons: list[str] = []
    if not value:
        return reasons
    # 禁止 .. 路径穿越
    if ".." in value:
        reasons.append("path contains '..' (traversal)")
    # 禁止个人 / build tree 前缀
    for bad in UNCONTROLLED_PATH_PREFIXES:
        if value.startswith(bad) or ("/" + bad.strip("/") + "/") in ("/" + value + "/"):
            reasons.append(f"path under uncontrolled/personal/build tree ({bad})")
    # production 禁止相对路径与 /tmp（不受控临时位置）
    if profile == "production":
        if value.startswith("./") or value == "." or (not value.startswith("/")):
            reasons.append("relative path not allowed in production")
        if value.startswith("/tmp/") or value == "/tmp":
            reasons.append("path under /tmp not allowed in production (uncontrolled)")
    return reasons


# ---------------------------------------------------------------------------
# Schema 校验
# ---------------------------------------------------------------------------

class SchemaValidator:
    def __init__(self, schema: dict):
        self.schema = schema
        self.errors: list[str] = []
        self.profile_errors: list[str] = []
        self.profile = "test"

    def _err(self, msg: str):
        self.errors.append(msg)

    def _perr(self, msg: str):
        self.profile_errors.append(msg)

    # 递归字段校验
    def validate_fields(self, node: Any, field_spec: dict, path: str):
        if not isinstance(node, dict):
            self._err(f"{path}: expected mapping, got {type(node).__name__}")
            return
        spec_fields = field_spec.get("fields", {})
        # 未知字段检测
        for key in node:
            if key not in spec_fields:
                self._err(f"{path}.{key}: unknown field (not in schema)")
        # 声明字段校验
        for fname, fspec in spec_fields.items():
            fpath = f"{path}.{fname}"
            required = fspec.get("required", False)
            if fname not in node:
                if required:
                    self._err(f"{fpath}: required field missing")
                continue
            self.validate_value(node[fname], fspec, fpath)

    def validate_value(self, value: Any, spec: dict, path: str):
        ftype = spec.get("type", "string")
        # required 已在外层处理；此处校验类型与约束
        if ftype == "map":
            if not isinstance(value, dict):
                self._err(f"{path}: expected mapping, got {type(value).__name__}")
                return
            self.validate_fields(value, spec, path)
        elif ftype == "string_list":
            if not isinstance(value, list):
                self._err(f"{path}: expected list, got {type(value).__name__}")
                return
            for i, v in enumerate(value):
                if not isinstance(v, str):
                    self._err(f"{path}[{i}]: expected string, got {type(v).__name__}")
        elif ftype == "integer":
            # YAML 可能解析为 bool（True/False 是 int 子类），需排除
            if isinstance(value, bool) or not isinstance(value, int):
                self._err(f"{path}: expected integer, got {type(value).__name__}")
                return
            if "min" in spec and value < spec["min"]:
                self._err(f"{path}: value {value} < min {spec['min']}")
            if "max" in spec and value > spec["max"]:
                self._err(f"{path}: value {value} > max {spec['max']}")
        elif ftype == "number":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                self._err(f"{path}: expected number, got {type(value).__name__}")
                return
            if "min" in spec and value < spec["min"]:
                self._err(f"{path}: value {value} < min {spec['min']}")
        elif ftype == "boolean":
            if not isinstance(value, bool):
                self._err(f"{path}: expected boolean, got {type(value).__name__}")
        elif ftype == "string":
            if not isinstance(value, str):
                self._err(f"{path}: expected string, got {type(value).__name__}")
                return
            if "enum" in spec and value not in spec["enum"]:
                self._err(f"{path}: value not in allowed enum {spec['enum']}")
            if "path_safe" in spec and spec["path_safe"]:
                reasons = path_is_safe(value, self.profile)
                for r in reasons:
                    self._perr(f"{path}: path safety violation: {r}")
            # 安全扫描：string 值
            self.scan_value(value, path)

    # 禁止字段检测（点分路径，如 mqtt.tls.root_ca_path）
    def check_forbidden_fields(self, node: dict):
        for dotpath in self.schema.get("forbidden_fields", []):
            parts = dotpath.split(".")
            if self._has_path(node, parts):
                self._err(f"{dotpath}: forbidden field present (local credential/secret)")

    @staticmethod
    def _has_path(node: Any, parts: list[str]) -> bool:
        cur = node
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return False
        return True

    # 安全扫描：扫描 string 值中的敏感模式
    def scan_value(self, value: str, path: str):
        if not isinstance(value, str):
            return
        if RE_PEM_PRIVATE.search(value):
            self._perr(f"{path}: contains PEM private key marker (secret)")
        if RE_PEM_CERT.search(value):
            self._perr(f"{path}: contains PEM certificate marker (secret)")
        if RE_FULL_DEVICE_TOPIC.match(value):
            self._perr(f"{path}: looks like a full device topic with instance value")
        if RE_VIN_INSTANCE.match(value):
            self._perr(f"{path}: looks like a VIN instance value")
        if RE_TEST_CERT_PATH.search(value):
            self._perr(f"{path}: references test CA/client key/fixture path")

    # production profile 额外规则
    def check_production(self, node: dict, top_keys: list[str]):
        # 禁止顶层 common
        for bad in self.schema.get("production_forbidden_top_level", []):
            if bad in top_keys:
                self._perr(
                    f"top-level '{bad}': forbidden in production profile "
                    f"(common is BUILD-owned; conf.d/mqtt.yaml must not inline it)"
                )
        # broker_host 不得为 mock 占位（localhost 作为 factory fallback 合法，
        # §17.3：真实地址经 framework-store/OTA 下发，非 credential）
        broker_host = self._get_path(node, ["mqtt", "broker_host"])
        if isinstance(broker_host, str) and RE_MOCK.search(broker_host):
            self._perr("mqtt.broker_host: mock endpoint not allowed in production")
        # 任何 string 值含 mock 标记
        self._scan_all_strings(node, "mqtt", self._mock_scan_cb)

    def _mock_scan_cb(self, value: str, path: str):
        if RE_MOCK.search(value):
            self._perr(f"{path}: contains 'mock' marker not allowed in production")

    def _scan_all_strings(self, node: Any, path: str, cb):
        if isinstance(node, dict):
            for k, v in node.items():
                self._scan_all_strings(v, f"{path}.{k}", cb)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                self._scan_all_strings(v, f"{path}[{i}]", cb)
        elif isinstance(node, str):
            cb(node, path)

    @staticmethod
    def _get_path(node: Any, parts: list[str]) -> Any:
        cur = node
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                return None
        return cur


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        print(f"ERROR: {path}: top-level YAML must be a mapping", file=sys.stderr)
        sys.exit(1)
    return data


def main(argv: list[str]) -> int:
    parser = _ArgParser(
        description="MQTT config schema + profile checker (TBOX-MQTT-DSN-CR-012 §17.6). "
                    "Pure, side-effect-free validation with stable exit codes."
    )
    parser.add_argument("config", help="path to the MQTT config file to validate")
    parser.add_argument(
        "--profile", choices=["production", "test"], default="production",
        help="validation profile (default: production)"
    )
    parser.add_argument(
        "--schema", default=DEFAULT_SCHEMA,
        help=f"path to schema file (default: {DEFAULT_SCHEMA})"
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="suppress per-violation output (exit code still reflects result)"
    )
    args = parser.parse_args(argv)

    # 文件可读性
    if not os.path.isfile(args.config):
        print(f"ERROR: config file not found: {args.config}", file=sys.stderr)
        return 3
    if not os.path.isfile(args.schema):
        print(f"ERROR: schema file not found: {args.schema}", file=sys.stderr)
        return 3

    try:
        config = load_yaml(args.config)
    except yaml.YAMLError as e:
        print(f"ERROR: YAML parse error in {args.config}: {e}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"ERROR: cannot read {args.config}: {e}", file=sys.stderr)
        return 3

    try:
        schema = load_yaml(args.schema)
    except (yaml.YAMLError, OSError) as e:
        print(f"ERROR: cannot load schema {args.schema}: {e}", file=sys.stderr)
        return 3

    validator = SchemaValidator(schema)
    validator.profile = args.profile

    top_keys = list(config.keys())
    # 顶层允许键
    allowed_top = schema.get("top_level_allowed", [])
    for k in top_keys:
        if allowed_top and k not in allowed_top:
            validator._err(f"top-level '{k}': not in allowed top-level keys {allowed_top}")

    # 禁止字段
    validator.check_forbidden_fields(config)

    # 字段结构校验（按 schema.fields 递归）
    fields_spec = schema.get("fields", {})
    for top_field, spec in fields_spec.items():
        if top_field in config:
            validator.validate_value(config[top_field], spec, top_field)
        else:
            if spec.get("required", False):
                validator._err(f"{top_field}: required top-level field missing")

    # production profile 规则
    if args.profile == "production":
        validator.check_production(config, top_keys)

    # 输出
    if not args.quiet:
        for e in validator.errors:
            print(f"SCHEMA FAIL: {e}", file=sys.stderr)
        for e in validator.profile_errors:
            print(f"PROFILE FAIL: {e}", file=sys.stderr)

    if validator.errors:
        return 1
    if validator.profile_errors:
        return 2

    if not args.quiet:
        print(f"PASS: {args.config} ({args.profile} profile)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
