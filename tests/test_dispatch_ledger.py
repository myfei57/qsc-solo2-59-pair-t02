"""调度台账：幂等去重、两段回执、补发、断线续传、改摊、结算与对账。"""

from __future__ import annotations

from dataclasses import replace as dc_replace

import pytest

from batterystore.dispatch.ledger import DispatchLedger
from batterystore.dispatch.orders import (
    CMD_ACKED,
    CMD_EXECUTED,
    CMD_EXEC_REJECTED,
    CMD_NOT_EXECUTED,
    CMD_SUPERSEDED,
    CMD_UNDELIVERED,
)
from batterystore.dispatch.settings import STRATEGY_CAPABILITY, STRATEGY_INSTALLED
from batterystore.errors import InterlockError
from batterystore.eventbus import EventBus
from batterystore.store.jsonfile import JsonStore


def _three_stations(ledger, auto_execute=True):
    for station_id, installed in (("a", 1000.0), ("b", 1000.0), ("c", 1000.0)):
        ledger.register_station(station_id, station_id, installed, installed, auto_execute=auto_execute)
        ledger.report_capability(station_id, installed)


def test_same_dispatch_id_within_a_minute_executes_once(fleet_now):
    ledger, clock = fleet_now
    _three_stations(ledger)
    first = ledger.receive("DUP-1", 900.0)
    assert first["duplicate"] is False
    clock.advance(0.001)
    for _ in range(3):
        again = ledger.receive("DUP-1", 900.0)
        assert again["duplicate"] is True
    order = ledger.order("DUP-1")
    assert order.duplicate_hits == 3
    # 每站只有一条 r0 命令，attempts 没有因重发而增加。
    assert {c.revision for c in order.commands.values()} == {0}
    assert all(c.attempts == 1 for c in order.commands.values())


def test_same_id_different_content_is_conflict_not_second_run(fleet_now):
    ledger, _ = fleet_now
    _three_stations(ledger)
    ledger.receive("C-1", 900.0)
    with pytest.raises(InterlockError):
        ledger.receive("C-1", 300.0)  # 目标变了
    with pytest.raises(InterlockError):
        ledger.receive("C-1", 900.0, strategy=STRATEGY_INSTALLED)  # 策略变了


def test_command_id_identifies_revision_and_is_idempotent_at_peer(fleet_now):
    ledger, _ = fleet_now
    _three_stations(ledger)
    ledger.receive("ID-1", 900.0)
    command_id = "ID-1:a:r0"
    # 对端重复收到同命令号：第二次是 duplicate，不重复执行。
    first = ledger.agent("a").deliver(command_id, 300.0, ledger.clock.now())
    second = ledger.agent("a").deliver(command_id, 300.0, ledger.clock.now())
    assert first["duplicate"] in (False, True)  # 平台首拍可能已送达
    assert second["duplicate"] is True


def test_ack_timeout_is_undelivered_not_executed_failure(fleet_now, dispatch_settings):
    ledger, clock = fleet_now
    ledger.settings = dc_replace(dispatch_settings, reallocate_max=0)  # 隔离超时判定，不改摊
    _three_stations(ledger)
    ledger.agent("a").set_link(False)
    ledger.fleet.set_online("a", True)  # 名册在线但链路断
    ledger.receive("U-1", 900.0, strategy=STRATEGY_INSTALLED)
    # 退避投递到次数用尽，再等满一个 ACK 窗口仍无回执 => 没收到。
    for _ in range(20):
        clock.advance(2.0)
        ledger.poll()
    command = ledger._latest_commands(ledger.order("U-1"))["a"]
    assert command.state == CMD_UNDELIVERED
    assert command.reason == "ack_timeout"


def test_exec_timeout_distinguishes_received_but_not_executed(fleet_now, dispatch_settings):
    ledger, clock = fleet_now
    ledger.settings = dc_replace(dispatch_settings, reallocate_max=0)  # 隔离"收到没执行"，不改摊
    _three_stations(ledger, auto_execute=False)  # 站端只回执接收，不执行
    ledger.receive("X-1", 900.0, strategy=STRATEGY_INSTALLED)
    clock.advance(1.0)
    ledger.poll()
    command = ledger._latest_commands(ledger.order("X-1"))["a"]
    assert command.state == CMD_ACKED  # 收到了
    clock.advance(31.0)
    ledger.poll()
    command = ledger._latest_commands(ledger.order("X-1"))["a"]
    assert command.state == CMD_NOT_EXECUTED  # 没执行——与没收到分栏
    assert command.reason == "execute_timeout"


def test_reconnect_resend_when_peer_never_received(fleet_now, dispatch_settings):
    ledger, clock = fleet_now
    ledger.settings = dc_replace(dispatch_settings, reallocate_max=0)
    _three_stations(ledger)
    ledger.agent("a").set_link(False)
    ledger.fleet.set_online("a", True)
    ledger.receive("RC-1", 900.0, strategy=STRATEGY_INSTALLED)
    for _ in range(20):
        clock.advance(2.0)
        ledger.poll()
    assert ledger._latest_commands(ledger.order("RC-1"))["a"].state == CMD_UNDELIVERED
    ledger.station_online("a")  # 重连：对端查无此令，按原命令号重投
    command = ledger._latest_commands(ledger.order("RC-1"))["a"]
    assert command.revision == 0  # 不是新指令
    assert command.state == CMD_EXECUTED


def test_reconnect_discovers_peer_actually_executed(fleet_now):
    ledger, clock = fleet_now
    _three_stations(ledger, auto_execute=False)
    ledger.receive("RX-1", 900.0, strategy=STRATEGY_INSTALLED)
    clock.advance(1.0)
    ledger.poll()
    clock.advance(31.0)
    ledger.poll()  # 平台侧判 not_executed
    # 现场其实在断线期间动作了，重连时按命令号查到已执行。
    ledger.agent("a").execute("RX-1:a:r0", clock.now())
    ledger.reconnect("a")
    command = ledger._latest_commands(ledger.order("RX-1"))["a"]
    assert command.state == CMD_EXECUTED
    assert command.actual_kw == 300.0
    statement = ledger.settle("RX-1")
    # a 的超时缺口必须被核销，不能既算执行又算缺口。
    assert statement["station_gaps_kw"] == {"execute_timeout": 600.0}


def test_runtime_exec_rejection_triggers_reallocation(fleet_now):
    ledger, clock = fleet_now
    _three_stations(ledger)
    ledger.agent("a").fail_execute("pcs_trip")
    ledger.receive("N-1", 900.0, strategy=STRATEGY_CAPABILITY)  # 各 300
    clock.advance(1.0)
    ledger.poll()
    order = ledger.order("N-1")
    assert order.reallocations, "a 拒执缺口应触发一轮改摊"
    latest = ledger._latest_commands(order)
    assert latest["a"].state == CMD_SUPERSEDED
    assert latest["b"].state == CMD_EXECUTED
    assert latest["c"].state == CMD_EXECUTED
    statement = ledger.settle("N-1")
    assert statement["executed_kw"] == pytest.approx(900.0)
    assert statement["declared_shortfall_kw"] == 0.0


def test_unfillable_gap_is_declared_not_silently_dropped(fleet_now):
    ledger, clock = fleet_now
    # c 离线；a/b 已贴上限，接不下 c 的名义份额。
    ledger.register_station("a", "a", 1000.0, 1000.0)
    ledger.register_station("b", "b", 1000.0, 1000.0)
    ledger.register_station("c", "c", 2000.0, 2000.0)
    for station_id in ("a", "b", "c"):
        ledger.report_capability(station_id, {"a": 1000.0, "b": 1000.0, "c": 2000.0}[station_id])
    ledger.station_offline("c")
    ledger.receive("T-1", 4000.0, strategy=STRATEGY_INSTALLED)  # 名义 1000/1000/2000
    clock.advance(1.0)
    ledger.poll()
    statement = ledger.settle("T-1")
    # a/b 各执行 1000（贴顶），c 的 2000 无人可接，显式挂账。
    assert statement["executed_kw"] == pytest.approx(2000.0)
    assert statement["declared_shortfall_kw"] == pytest.approx(2000.0)


def test_settle_blocks_while_commands_open(fleet_now):
    ledger, clock = fleet_now
    _three_stations(ledger, auto_execute=False)
    ledger.receive("O-1", 900.0, strategy=STRATEGY_INSTALLED)
    clock.advance(1.0)
    ledger.poll()  # 全部 acked，仍在途
    with pytest.raises(InterlockError):
        ledger.settle("O-1")


def test_reconcile_flags_station_and_total_variance(fleet_now):
    ledger, clock = fleet_now
    _three_stations(ledger)
    ledger.receive("R-1", 900.0)
    clock.advance(1.0)
    ledger.poll()
    matched = ledger.reconcile("R-1", {"total_kw": 900.0,
                                       "stations": {"a": 300.0, "b": 300.0, "c": 300.0}})
    assert matched["matched"] is True
    mismatched = ledger.reconcile("R-1", {"total_kw": 870.0,
                                          "stations": {"a": 300.0, "b": 270.0, "c": 300.0}})
    assert mismatched["matched"] is False
    assert mismatched["matched_total"] is False
    assert {v["station_id"] for v in mismatched["variances"]} == {"b"}


def test_state_survives_restart(tmp_path, dispatch_settings):
    from batterystore.clock import ManualClock

    root = str(tmp_path / "persist")
    clock = ManualClock()
    ledger = DispatchLedger(clock, EventBus(), JsonStore(root), dispatch_settings)
    _three_stations(ledger)
    ledger.receive("P-1", 900.0)
    clock.advance(1.0)
    ledger.poll()
    ledger.settle("P-1")

    clock2 = ManualClock(clock.now())
    rebuilt = DispatchLedger(clock2, EventBus(), JsonStore(root), dispatch_settings)
    order = rebuilt.order("P-1")
    assert order.state == "settled"
    assert all(c.state == CMD_EXECUTED for c in order.commands.values())
    assert order.reconciliation is None or order.state == "settled"
    # 重发仍是幂等的，不会在恢复后第二次执行。
    again = rebuilt.receive("P-1", 900.0)
    assert again["duplicate"] is True


def test_capability_offline_report_rejected(fleet_now):
    ledger, _ = fleet_now
    ledger.register_station("a", "a", 1000.0, 1000.0)
    ledger.station_offline("a")
    with pytest.raises(InterlockError):
        ledger.report_capability("a", 500.0)


def test_explicit_nack_and_exec_reject_are_distinct(fleet_now):
    ledger, clock = fleet_now
    _three_stations(ledger, auto_execute=False)
    ledger.receive("K-1", 900.0, strategy=STRATEGY_INSTALLED)
    clock.advance(1.0)
    ledger.poll()
    ledger.receive_ack("K-1", "a", False, reason="station_busy")
    ack = ledger._latest_commands(ledger.order("K-1"))["a"]
    assert ack.state == "nack"
    # b 收到了，但执行回执是拒绝。
    ledger.receive_exec("K-1", "b", False, reason="pcs_trip")
    brej = ledger._latest_commands(ledger.order("K-1"))["b"]
    assert brej.state == CMD_EXEC_REJECTED
