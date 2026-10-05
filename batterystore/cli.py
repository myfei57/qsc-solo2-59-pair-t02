"""命令行入口。

六个子命令覆盖日常使用：

``serve``    启动控制台 HTTP 服务，可选地在启动后探测接口；
``demo``     在本地跑一遍完整流程（启动 → 并网切换 → 温度降额 → 均衡 → 充电 →
             放电 → 消防联动 → 复位 → 并发探测），用于交付前的真实运行检查；
``status``   打印当前状态快照；
``config``   打印生效配置；
``alerts``   打印按最新编号路由后的告警；
``reset``    清掉落盘的平台计数。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import replace

from .app import Platform, build_platform
from .clock import ManualClock, SystemClock
from .config import config_to_mapping, default_config, load_config
from .console.server import ConsoleServer, probe_endpoints
from .dispatch.settings import DispatchSettings
from .errors import StoreError

DEFAULT_PROBE_PATHS = ("/api/status", "/api/readiness", "/api/topology", "/api/audit", "/api/racks")


def _build(args: argparse.Namespace) -> Platform:
    config = load_config(args.config) if getattr(args, "config", None) else default_config()
    return build_platform(
        config,
        storage_root=getattr(args, "storage_root", None),
        rack_count=getattr(args, "racks", 2),
    )


def _dump(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def _cmd_serve(args: argparse.Namespace) -> int:
    platform = _build(args)
    server = ConsoleServer(platform, host=args.host, port=args.port, web_root=args.web_root)
    base_url = server.start()
    payload: dict = {
        "listening": base_url,
        "routes": server.api.routes(),
        "pages": server.pages.index(),
        "pages_ready": server.pages.exists(),
    }
    if args.probe:
        payload["probe"] = probe_endpoints(base_url, list(DEFAULT_PROBE_PATHS))
    _dump(payload)
    if args.duration > 0:
        try:
            time.sleep(args.duration)
        except KeyboardInterrupt:  # pragma: no cover - 交互路径
            pass
        server.stop()
        return 0
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:  # pragma: no cover - 交互路径
        server.stop()
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    clock = ManualClock() if args.fast else SystemClock()
    config_path = getattr(args, "config", None)
    config = load_config(config_path) if config_path else default_config()
    platform = build_platform(
        config,
        clock=clock,
        storage_root=getattr(args, "storage_root", None),
        rack_count=args.racks,
    )
    # 演示从干净基线起步：先清掉上一轮落盘的计数与运行态。
    platform.reset_state()

    startup = platform.start()
    tie = platform.transfer_grid_tie(power_kw=args.power_kw, frequency_hz=50.0)
    derate = platform.sample_temperature(temperature_c=args.temperature_c, dt=5.0)
    platform.disturb_cell(cell_index=0, delta_v=0.04)
    balancing = platform.balance(max_steps=args.balance_steps)
    charging = platform.charge(steps=args.steps, dt=600.0)
    discharging = platform.discharge(power_kw=args.discharge_kw, dt=600.0)
    cycles = [platform.cycle(dt=5.0) for _ in range(3)]
    drill = platform.fire_drill(zone="Z1", smoke_ppm=1500.0, temperature_c=30.0, confirm_dt=5.0)
    release = platform.release_fire()
    calibration = platform.calibrate_soc(68.0, "demo")
    probe = platform.concurrency_probe(rounds=args.rounds)
    fault_probe = platform.concurrency_fault_probe(rounds=8)
    alarms = platform.alerts()
    shutdown = platform.stop()

    server = ConsoleServer(platform, port=0, web_root=args.web_root)
    base_url = server.start()
    endpoints = probe_endpoints(base_url, list(DEFAULT_PROBE_PATHS))
    pages = server.pages.index()
    server.stop()

    report = {
        "startup": startup,
        "grid_tie": {"ok": tie["ok"], "steps": [step["action"] for step in tie["steps"]]},
        "derate": {"charge_limit_ratio": derate["charge_limit_ratio"]},
        "balance": {"ok": balancing["ok"], "violations": balancing["violations"]},
        "charge": {"ok": charging["ok"], "violations": charging["violations"]},
        "discharge": {"ok": discharging["ok"], "violations": discharging["violations"]},
        "cycles_run": len(cycles),
        "last_cycle": cycles[-1],
        "fire_drill": {"ok": drill["ok"], "steps": [step["action"] for step in drill["steps"]]},
        "fire_release": release,
        "calibration": calibration,
        "concurrency": probe,
        "concurrency_fault": fault_probe,
        "alerts": alarms,
        "shutdown": shutdown,
        "probe": endpoints,
        "pages": pages,
        "audit": platform.audit.summary(),
    }
    _dump(report)

    failures = [name for name, item in endpoints.items() if not item["ok"]]
    failures.extend(name for name, item in (("grid_tie", tie), ("charge", charging), ("discharge", discharging)) if not item["ok"])
    if not drill["ok"]:
        failures.append("fire_drill")
    if not probe["ok"]:
        failures.append("concurrency_probe")
    if not fault_probe["ok"]:
        failures.append("fault_probe")
    if not all(item["present"] for item in pages):
        failures.append("pages_missing")
    if failures:
        print(f"检查未通过: {failures}", file=sys.stderr)
        return 1
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    platform = _build(args)
    payload = platform.status()
    payload["topology"] = platform.topology_summary()
    payload["capacity"] = platform.capacity().as_dict()
    payload["quota"] = platform.quota_snapshot()
    payload["trend"] = platform.trend_snapshot()
    _dump(payload)
    return 0


def _cmd_config(args: argparse.Namespace) -> int:
    config_path = getattr(args, "config", None)
    config = load_config(config_path) if config_path else default_config()
    _dump(config_to_mapping(config))
    return 0


def _cmd_alerts(args: argparse.Namespace) -> int:
    platform = _build(args)
    _dump({"alerts": platform.alerts(), "mapping": platform.rack_mapping()})
    return 0


def _cmd_reset(args: argparse.Namespace) -> int:
    platform = _build(args)
    _dump(platform.reset_state())
    return 0


def _cmd_dispatch(args: argparse.Namespace) -> int:
    """跑一遍集群调峰闭环：登记站群 → 上报能力 → 部分离线 → 两种分摊对比 →
    超时补发／续传 → 缺口改摊 → 结算 → 与调度端对账。"""

    clock = ManualClock()
    config = load_config(args.config) if getattr(args, "config", None) else default_config()
    settings = replace(
        DispatchSettings.default(),
        ack_timeout_s=float(args.ack_timeout_s),
        execute_timeout_s=float(args.execute_timeout_s),
        retry_backoff_s=float(args.retry_backoff_s),
        reallocate_max=int(args.reallocate_rounds),
    )
    platform = build_platform(
        config,
        clock=clock,
        storage_root=getattr(args, "storage_root", None),
        rack_count=args.racks,
        dispatch_settings=settings,
    )
    ledger = platform.dispatch

    stations = [("st-a", 1000.0), ("st-b", 1000.0), ("st-c", 2000.0)]
    for station_id, installed in stations:
        platform.dispatch_register(station_id, station_id, installed_kw=installed, headroom_kw=installed)
        platform.dispatch_capability(station_id, installed)

    # 场景一：st-c（占一半装机）离线，对比两种策略的首轮分解差异。
    platform.dispatch_offline("st-c")
    capability_plan = platform.dispatch_target("PK-CAP-001", args.target_kw, strategy="capability")
    # 同一分钟重发两次：只能返回原指令，不能第二次执行。
    platform.dispatch_target("PK-CAP-001", args.target_kw, strategy="capability")
    capability_dup = platform.dispatch_target("PK-CAP-001", args.target_kw, strategy="capability")
    clock.advance(1)
    platform.dispatch_poll()
    capability_statement = platform.dispatch_settle("PK-CAP-001")

    installed_plan = platform.dispatch_target("PK-INS-002", args.target_kw, strategy="installed")
    clock.advance(1)
    platform.dispatch_poll()  # 首轮节拍：离线站名义缺口触发改摊水瀑
    installed_statement = platform.dispatch_settle("PK-INS-002")

    # 场景二：st-a 收到了但执行失败（PCS 跳闸），缺口自动改摊给在线有余量的站。
    platform.dispatch_online("st-c")
    platform.dispatch_capability("st-a", 1000.0)
    platform.dispatch_capability("st-b", 1000.0)
    platform.dispatch_capability("st-c", 2000.0)
    platform.dispatch_agent_fail("st-a", "pcs_trip")
    nack_plan = platform.dispatch_target("PK-NACK-003", args.target_kw, strategy="capability")
    clock.advance(1)
    platform.dispatch_poll()
    nack_statement = platform.dispatch_settle("PK-NACK-003")

    # 用能力法结算的真实逐站值与调度端对账（总量与逐站一致）。
    reconciliation = platform.dispatch_reconcile(
        "PK-CAP-001",
        {
            "total_kw": capability_statement["executed_kw"],
            "stations": {
                station_id: value.get("actual_kw") or 0.0
                for station_id, value in capability_statement["stations"].items()
            },
        },
    )

    report = {
        "settings": {
            "ack_timeout_s": settings.ack_timeout_s,
            "execute_timeout_s": settings.execute_timeout_s,
            "retry_backoff_s": settings.retry_backoff_s,
            "reallocate_max": settings.reallocate_max,
        },
        "fleet": platform.dispatch_fleet(),
        "capability": {
            "initial_shares": capability_plan["plan"]["shares"],
            "excluded": capability_plan["plan"]["excluded"],
            "shortfall_kw": capability_plan["plan"]["shortfall_kw"],
            "duplicate_hits": capability_dup["order"]["duplicate_hits"],
            "statement": capability_statement,
        },
        "installed": {
            "initial_shares": installed_plan["plan"]["shares"],
            "nominal": installed_plan["plan"]["nominal"],
            "gap_causes": installed_plan["plan"]["gap_causes"],
            "shortfall_kw": installed_plan["plan"]["shortfall_kw"],
            "reallocations": installed_statement["reallocations"],
            "statement": {
                key: installed_statement[key]
                for key in ("executed_kw", "declared_shortfall_kw", "station_gaps_kw", "stations")
            },
        },
        "nack_reallocate": {
            "initial_shares": nack_plan["plan"]["shares"],
            "statement": {
                key: nack_statement[key]
                for key in ("executed_kw", "declared_shortfall_kw", "station_gaps_kw", "stations", "reallocations")
            },
        },
        "reconciliation": {
            "matched": reconciliation["matched"],
            "matched_total": reconciliation.get("matched_total"),
            "variances": reconciliation["variances"],
        },
        "orders": platform.dispatch_orders(),
    }
    _dump(report)
    failures = []
    if capability_dup["order"]["duplicate_hits"] != 2:
        failures.append("duplicate_dedup")
    if not reconciliation["matched"]:
        failures.append("reconcile_mismatch")
    if nack_statement["declared_shortfall_kw"] != 0:
        failures.append("nack_gap_not_reallocated")
    if failures:
        print(f"检查未通过: {failures}", file=sys.stderr)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    """构造命令行解析器。"""

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="JSON 配置文件路径", default=argparse.SUPPRESS)
    common.add_argument("--storage-root", help="覆盖状态存储目录", default=argparse.SUPPRESS)
    common.add_argument("--racks", type=int, default=2, help="簇数量")
    common.add_argument("--web-root", default="web", help="控制台页面目录")
    parser = argparse.ArgumentParser(
        prog="batterystore", description="电化学储能电站簇级联控服务", parents=[common]
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="启动控制台服务", parents=[common])
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--duration", type=float, default=0.0, help="运行多少秒后退出，0 表示常驻")
    serve.add_argument("--probe", action="store_true", help="启动后探测接口")
    serve.set_defaults(func=_cmd_serve)

    demo = sub.add_parser("demo", help="跑一遍完整流程", parents=[common])
    demo.add_argument("--power-kw", type=float, default=120.0, help="并网输出功率")
    demo.add_argument("--discharge-kw", type=float, default=200.0, help="放电功率")
    demo.add_argument("--temperature-c", type=float, default=46.0, help="演示温度采集")
    demo.add_argument("--steps", type=int, default=3, help="充电电流阶梯数")
    demo.add_argument("--balance-steps", type=int, default=400, help="均衡最大步数")
    demo.add_argument("--rounds", type=int, default=24, help="并发探测轮数")
    demo.add_argument("--fast", action="store_true", help="使用可推进时钟，跳过真实等待")
    demo.set_defaults(func=_cmd_demo)

    status = sub.add_parser("status", help="打印状态快照", parents=[common])
    status.set_defaults(func=_cmd_status)

    config = sub.add_parser("config", help="打印生效配置", parents=[common])
    config.set_defaults(func=_cmd_config)

    alerts = sub.add_parser("alerts", help="打印路由后的告警", parents=[common])
    alerts.set_defaults(func=_cmd_alerts)

    reset = sub.add_parser("reset", help="清掉落盘的平台计数", parents=[common])
    reset.set_defaults(func=_cmd_reset)

    dispatch = sub.add_parser("dispatch", help="跑一遍集群调峰闭环（分解/补发/改摊/对账）", parents=[common])
    dispatch.add_argument("--target-kw", type=float, default=1000.0, help="调峰目标功率（kW）")
    dispatch.add_argument("--ack-timeout-s", type=float, default=15.0, help="接收回执超时")
    dispatch.add_argument("--execute-timeout-s", type=float, default=120.0, help="执行回执超时")
    dispatch.add_argument("--retry-backoff-s", type=float, default=5.0, help="补发基础间隔（指数退避）")
    dispatch.add_argument("--reallocate-rounds", type=int, default=2, help="缺口改摊最大轮数")
    dispatch.set_defaults(func=_cmd_dispatch)
    return parser


def main(argv: list[str] | None = None) -> int:
    """命令行主入口。"""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="backslashreplace")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except StoreError as error:
        print(json.dumps(error.as_record(), ensure_ascii=False), file=sys.stderr)
        return 2
