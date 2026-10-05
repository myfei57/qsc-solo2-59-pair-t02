"""电站群调峰目标分解、可靠下发与对账。"""

from __future__ import annotations

import json
import urllib.request

import pytest

from batterystore.config import default_config
from batterystore.dispatch.allocation import allocate_target
from batterystore.errors import InterlockError


def _fleet(platform):
    platform.register_dispatch_station("s1", 100.0, 100.0)
    platform.register_dispatch_station("s2", 100.0, 50.0)
    platform.register_dispatch_station("s3", 100.0, 80.0, online=False)


def _post(base_url: str, path: str, payload: dict):
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def _get(base_url: str, path: str):
    with urllib.request.urlopen(f"{base_url}{path}", timeout=5) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def test_remaining_allocation_tracks_online_response_capability():
    from batterystore.dispatch.fleet import StationFleet

    fleet = StationFleet()
    online = [
        fleet.register("s1", 100.0, 100.0),
        fleet.register("s2", 100.0, 50.0),
    ]
    remaining = allocate_target(online, 90.0, "remaining")
    assert remaining["allocations"] == {"s1": 60.0, "s2": 30.0}
    assert remaining["shortfall_kw"] == 0.0

    installed = allocate_target(online, 90.0, "installed")
    assert installed["allocations"] == {"s1": 45.0, "s2": 45.0}
    assert installed["shortfall_kw"] == 0.0


def test_installed_allocation_is_unsuitable_for_partial_offline_fleet(platform):
    _fleet(platform)
    remaining = platform.submit_dispatch_target("ord-1", 150.0, method="remaining", deadline_s=120)
    assert remaining["allocation"]["allocations"] == {"s1": 100.0, "s2": 50.0}
    assert remaining["allocation"]["shortfall_kw"] == 0.0

    installed = platform.submit_dispatch_target("ord-2", 150.0, method="installed", deadline_s=120)
    # 容量法先按 1:1 形成 75/75，但 s2 只剩 50kW，平台仍会用剩余能力钳位并回流给 s1；
    # 这说明容量权重只表达考核/计划口径，实时下发必须以剩余能力钳位后的结果为准。
    assert installed["allocation"]["allocations"] == {"s1": 100.0, "s2": 50.0}
    assert installed["allocation"]["shortfall_kw"] == 0.0


def test_allocation_methods_differ_when_remaining_capacity_deviates():
    from batterystore.dispatch.fleet import StationFleet

    fleet = StationFleet()
    online = [
        fleet.register("s1", 100.0, 100.0),
        fleet.register("s2", 100.0, 20.0),
    ]
    remaining = allocate_target(online, 50.0, "remaining")
    assert remaining["allocations"] == {"s1": 41.667, "s2": 8.333}
    installed = allocate_target(online, 50.0, "installed")
    assert installed["allocations"] == {"s1": 30.0, "s2": 20.0}


def test_same_minute_duplicate_target_is_idempotent(platform):
    _fleet(platform)
    first = platform.submit_dispatch_target("ord-3", 60.0, deadline_s=120)
    duplicate_same_id = platform.submit_dispatch_target("ord-3", 60.0, deadline_s=120)
    duplicate_same_fingerprint = platform.submit_dispatch_target("resend-no", 60.0, deadline_s=120)
    assert first["duplicate"] is False
    assert duplicate_same_id["duplicate"] is True
    assert duplicate_same_fingerprint["duplicate"] is True
    assert duplicate_same_fingerprint["order_id"] == "ord-3"
    assert first["allocation"] == duplicate_same_id["allocation"]
    with pytest.raises(InterlockError, match="不同目标内容"):
        platform.submit_dispatch_target("ord-3", 61.0, deadline_s=120)


def test_delivery_and_execution_receipts_distinguish_missing_from_not_executed(platform, clock):
    _fleet(platform)
    platform.fail_dispatch_deliveries("s2", 3)
    order = platform.submit_dispatch_target("ord-4", 120.0, deadline_s=300)
    assert order["assignments"]["s1"]["state"] == "delivered_pending_execution"
    assert order["assignments"]["s1"]["commands"][0]["delivery_attempts"] == 1
    assert order["assignments"]["s1"]["commands"][0]["delivered_ts"] is not None

    clock.advance(30)
    collected = platform.collect_dispatch()
    assert "ord-4" in collected["orders_touched"]
    missing = platform.dispatch_order("ord-4")["assignments"]["s2"]["commands"][0]
    assert missing["state"] == "not_delivered"
    assert missing["delivery_attempts"] == 3

    executed = platform.execute_dispatch_command("ord-4", "s1", 1)
    assert executed["command"]["state"] == "executed"
    assert executed["station_result"]["duplicate"] is False
    repeated = platform.execute_dispatch_command("ord-4", "s1", 1)
    assert repeated["station_result"]["duplicate"] is True
    assert repeated["command"]["state"] == "executed"


def test_not_delivered_gap_is_retried_then_reallocated(platform, clock):
    platform.register_dispatch_station("s1", 100.0, 100.0)
    platform.register_dispatch_station("s2", 100.0, 100.0)
    platform.fail_dispatch_deliveries("s2", 3)
    order = platform.submit_dispatch_target(
        "ord-5", 50.0, deadline_s=300, station_ids=["s2"]
    )
    assert order["allocation"]["allocations"] == {"s2": 50.0}
    assert order["assignments"]["s2"]["commands"][0]["state"] == "not_delivered"

    for _ in range(3):
        clock.advance(30)
        platform.collect_dispatch()
    result = platform.dispatch_order("ord-5")
    original = result["assignments"]["s2"]["commands"][0]
    assert original["delivery_attempts"] == 3
    assert original["state"] == "reallocated"
    assert result["assignments"]["s1"]["state"] == "delivered_pending_execution"
    assert result["assignments"]["s1"]["commands"][-1]["reallocated_from"] == [original["command_id"]]

    platform.execute_dispatch_command("ord-5", "s1", 1)
    report = platform.reconcile_dispatch_order("ord-5", force=True)
    assert report["balanced"] is True
    assert report["executed_kw"] == 50.0
    assert report["reallocated_out_kw"] == 50.0


def test_delivered_but_unexecuted_is_not_reallocated(platform, clock):
    _fleet(platform)
    platform.submit_dispatch_target("ord-6", 100.0, deadline_s=300)
    # 已送达后再离线：平台能从站端幂等窗口查到 delivered，必须归类为“已收到未执行”。
    platform.set_dispatch_station_online("s1", False)
    platform.set_dispatch_station_online("s2", False)
    for _ in range(4):
        clock.advance(30)
        platform.collect_dispatch()
    result = platform.dispatch_order("ord-6")
    assert result["assignments"]["s1"]["state"] == "delivered_pending_execution"
    assert result["assignments"]["s2"]["state"] == "delivered_pending_execution"
    assert len(result["assignments"]["s1"]["commands"]) == 1

    report = platform.reconcile_dispatch_order("ord-6", force=True)
    assert report["balanced"] is False
    assert report["executed_kw"] == 0.0
    assert report["delivered_pending_kw"] == 100.0
    assert report["not_delivered_kw"] == 0.0


def test_execution_rejection_reallocates_but_delivered_pending_does_not(platform):
    platform.register_dispatch_station("s1", 100.0, 100.0)
    platform.register_dispatch_station("s2", 100.0, 100.0)
    platform.submit_dispatch_target("ord-8", 100.0, deadline_s=300)
    platform.dispatch_execution_receipt("ord-8", "s1", 1, "rejected", reason="pcs_fault")
    result = platform.dispatch_order("ord-8")
    assert result["assignments"]["s1"]["state"] == "reallocated"
    assert len(result["assignments"]["s2"]["commands"]) == 2
    platform.execute_dispatch_command("ord-8", "s2", 1)
    platform.execute_dispatch_command("ord-8", "s2", 2)
    report = platform.reconcile_dispatch_order("ord-8", force=True)
    assert report["balanced"] is True
    assert report["executed_kw"] == 100.0
    assert report["rejected_kw"] == 50.0


def test_dispatch_state_restores_after_process_restart(tmp_path, clock):
    config = default_config()
    platform = build_platform(config, clock=clock, storage_root=str(tmp_path / "state"), rack_count=1)
    _fleet(platform)
    platform.submit_dispatch_target("ord-7", 90.0, deadline_s=120)
    platform.execute_dispatch_command("ord-7", "s1", 1)

    restarted = build_platform(config, clock=clock, storage_root=str(tmp_path / "state"), rack_count=1)
    result = restarted.dispatch_order("ord-7")
    assert result["assignments"]["s1"]["commands"][0]["state"] == "executed"
    repeated = restarted.execute_dispatch_command("ord-7", "s1", 1)
    assert repeated["station_result"]["duplicate"] is True


def test_dispatch_console_api_and_audit(server):
    base_url, platform = server
    _post(base_url, "/api/dispatch/stations", {"station_id": "a1", "installed_kw": 100, "remaining_kw": 80})
    status, order = _post(base_url, "/api/dispatch/target", {"order_id": "api-1", "target_kw": 40, "deadline_s": 60})
    assert status == 200
    assert order["assignments"]["a1"]["state"] == "delivered_pending_execution"
    status, executed = _post(
        base_url,
        "/api/dispatch/execute",
        {"order_id": "api-1", "station_id": "a1", "command_no": 1},
    )
    assert status == 200
    assert executed["command"]["state"] == "executed"
    status, report = _post(base_url, "/api/dispatch/reconcile", {"order_id": "api-1", "force": True})
    assert status == 200
    assert report["balanced"] is True
    status, fetched = _get(base_url, "/api/dispatch/order/api-1")
    assert status == 200
    assert fetched["reconciliation"]["balanced"] is True
    actions = {entry["action"] for entry in platform.audit.entries(limit=100)}
    assert "dispatch_target_allocated" in actions
    assert "dispatch_reconciled" in actions


@pytest.fixture()
def server(platform):
    from batterystore.console.server import ConsoleServer

    service = ConsoleServer(platform, port=0)
    base_url = service.start()
    try:
        yield base_url, platform
    finally:
        service.stop()
