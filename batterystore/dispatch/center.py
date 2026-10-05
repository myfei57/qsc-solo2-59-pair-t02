"""调峰指令的分解、下发、回执、补发与对账。

一条调度目标进入 :class:`DispatchCenter` 后先按幂等键去重，再形成不可变批次。
站端指令采用同一业务标识重发：站端已收到时重复报文不会第二次执行；平台用
“送达回执”和“执行回执”两阶段状态区分链路没送到与送到但没执行。只有收到
明确拒收，或站点恢复在线后查到“未送达”，才允许把缺口改派；已送达未执行的
指令存在稍后自行执行的可能，不能盲目二次分摊。
"""

from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Callable
from typing import Any

from ..clock import Clock
from ..errors import InterlockError
from .allocation import ALLOCATION_METHODS, allocate_target
from .fleet import StationFleet, StationInfo

RETRY_INTERVAL_S = 30.0
MAX_ATTEMPTS = 3
DUPLICATE_WINDOW_S = 60.0
COMPONENT = "dispatch"

NOT_DELIVERED = "not_delivered"
DELIVERED_PENDING = "delivered_pending_execution"
EXECUTED = "executed"
REJECTED = "rejected"
REALLOCATED = "reallocated"
ACTIVE_STATES = {NOT_DELIVERED, DELIVERED_PENDING, EXECUTED}
OPEN_COMMAND_STATES = {NOT_DELIVERED, DELIVERED_PENDING}


class DispatchCenter:
    """电站群调峰目标编排器。"""

    def __init__(self, clock: Clock, on_change: Callable[[], None] | None = None) -> None:
        self._clock = clock
        self._fleet = StationFleet()
        self._orders: dict[str, dict] = {}
        self._on_change = on_change
        self._event_sink: Callable[[str, dict, str], None] | None = None
        self._lock = threading.RLock()

    @property
    def fleet(self) -> StationFleet:
        return self._fleet

    def set_event_sink(self, sink: Callable[[str, dict, str], None]) -> None:
        """装配平台事件发布函数。"""

        self._event_sink = sink

    def register_station(self, station_id: str, installed_kw: float, remaining_kw: float, online: bool = True) -> dict:
        with self._lock:
            station = self._fleet.register(station_id, installed_kw, remaining_kw, online)
            self._persist()
            return station.as_dict()

    def set_station_online(self, station_id: str, online: bool) -> dict:
        with self._lock:
            station = self._fleet.set_online(station_id, online)
            self._persist()
            return station.as_dict()

    def update_station_remaining(self, station_id: str, remaining_kw: float) -> dict:
        with self._lock:
            station = self._fleet.update_remaining(station_id, remaining_kw)
            self._persist()
            return station.as_dict()

    def fail_station_deliveries(self, station_id: str, count: int = 1) -> dict:
        """模拟在线站点连续若干次链路发送失败。"""

        with self._lock:
            remaining = self._fleet.fail_next_deliveries(station_id, count)
            self._persist()
            return {"station_id": station_id, "failed_deliveries_remaining": remaining}

    def submit_target(
        self,
        order_id: str,
        target_kw: float,
        *,
        method: str = "remaining",
        deadline_s: float = 120.0,
        station_ids: list[str] | None = None,
    ) -> dict:
        """接收调度目标并完成首次分解、下发。"""

        with self._lock:
            now = self._clock.now()
            self._validate_submission(order_id, target_kw, method, deadline_s)
            fingerprint = self._fingerprint(target_kw, method, station_ids)
            existing = self._orders.get(order_id)
            if existing is not None and existing["fingerprint"] != fingerprint:
                raise InterlockError(f"同一指令标识 {order_id} 携带了不同目标内容", component=COMPONENT)
            duplicate = existing if existing is not None else self._find_fingerprint_duplicate(fingerprint, now)
            if duplicate is not None:
                duplicate["duplicate_arrivals"] += 1
                self._publish("dispatch_duplicate_ignored", {"order_id": order_id}, "notice")
                self._persist()
                return self._order_view(duplicate, duplicate=True)
            stations = self._select_stations(station_ids)
            allocation = allocate_target(stations, target_kw, method)
            order = {
                "order_id": order_id,
                "fingerprint": fingerprint,
                "target_kw": round(float(target_kw), 3),
                "method": method,
                "created_ts": now,
                "deadline_ts": now + float(deadline_s),
                "retry_interval_s": RETRY_INTERVAL_S,
                "max_attempts": MAX_ATTEMPTS,
                "duplicate_arrivals": 0,
                "allocation": allocation,
                "assignments": {},
                "reconciliation": None,
            }
            self._orders[order_id] = order
            for station_id, amount in allocation["allocations"].items():
                self._add_assignment(order, self._fleet.get(station_id), amount, "initial", [], now)
            self._publish(
                "dispatch_target_allocated",
                {
                    "order_id": order_id,
                    "target_kw": order["target_kw"],
                    "allocated_kw": allocation["allocated_kw"],
                    "shortfall_kw": allocation["shortfall_kw"],
                    "method": method,
                },
                "notice" if allocation["shortfall_kw"] == 0 else "warning",
            )
            self._persist()
            return self._order_view(order)

    def collect_due(self, now: float | None = None) -> dict:
        """推进到期指令：查询、补发、改派，并在截止时自动对账。"""

        with self._lock:
            current = self._clock.now() if now is None else float(now)
            touched: list[str] = []
            for order in list(self._orders.values()):
                if order["reconciliation"] is not None:
                    continue
                before = json.dumps(order, sort_keys=True, ensure_ascii=False)
                self._collect_order(order, current)
                if current >= order["deadline_ts"]:
                    self._reconcile(order, current)
                if json.dumps(order, sort_keys=True, ensure_ascii=False) != before:
                    touched.append(order["order_id"])
            self._persist()
            return {"now": round(current, 3), "orders_touched": touched, "open_orders": self._open_count()}

    def mark_delivery_receipt(self, order_id: str, station_id: str, command_no: int, delivered: bool) -> dict:
        """登记站端送达回执。"""

        with self._lock:
            order = self._require_order(order_id)
            command = self._command(order, station_id, command_no)
            now = self._clock.now()
            if delivered:
                self._mark_delivered(order, command, now, via="receipt")
            else:
                command["state"] = NOT_DELIVERED
                command["last_error"] = "station_rejected_delivery"
                command["next_action_ts"] = now
                self._publish_receipt(order, command, "dispatch_delivery_nack", "warning")
                if now < order["deadline_ts"]:
                    self._reallocate_command(order, station_id, command, "delivery_rejected", now)
            self._persist()
            return dict(command)

    def mark_execution_receipt(
        self,
        order_id: str,
        station_id: str,
        command_no: int,
        status: str,
        *,
        power_kw: float | None = None,
        reason: str = "",
    ) -> dict:
        """登记站端执行回执；``executed``/``rejected`` 是最终执行态。"""

        with self._lock:
            order = self._require_order(order_id)
            command = self._command(order, station_id, command_no)
            now = self._clock.now()
            if status == "executed":
                amount = command["power_kw"] if power_kw is None else float(power_kw)
                command["state"] = EXECUTED
                command["executed_ts"] = now
                command["executed_power_kw"] = round(amount, 3)
                command["last_error"] = ""
                self._refresh_assignment(order, station_id)
                self._publish_receipt(order, command, "dispatch_execution_confirmed", "notice")
            elif status == "rejected":
                command["state"] = REJECTED
                command["rejected_power_kw"] = command["power_kw"]
                command["executed_ts"] = None
                command["executed_power_kw"] = 0.0
                command["last_error"] = reason or "station_rejected_execution"
                self._refresh_assignment(order, station_id)
                self._publish_receipt(order, command, "dispatch_execution_rejected", "warning")
                if now < order["deadline_ts"]:
                    self._reallocate_command(order, station_id, command, "execution_rejected", now)
            else:
                raise InterlockError(f"未知执行回执状态: {status}", component=COMPONENT)
            self._persist()
            return dict(command)

    def execute_at_station(self, order_id: str, station_id: str, command_no: int | None = None) -> dict:
        """测试/网关适配用：让模拟站端执行并同步生成执行回执。"""

        with self._lock:
            order = self._require_order(order_id)
            number = command_no
            if number is None:
                active = [cmd for cmd in order["assignments"][station_id]["commands"] if cmd["state"] in OPEN_COMMAND_STATES]
                if not active:
                    raise InterlockError(f"站点 {station_id} 没有待执行指令", component=COMPONENT)
                number = active[-1]["command_no"]
            command = self._command(order, station_id, number)
            result = self._fleet.execute(command["command_id"], command["power_kw"])
            if result["executed"]:
                self.mark_execution_receipt(order_id, station_id, number, "executed", power_kw=result["power_kw"])
            return {"station_result": result, "command": dict(command)}

    def reconcile_order(self, order_id: str, *, force: bool = False) -> dict:
        """与调度端对账；默认到期才允许关账，``force`` 可提前关账。"""

        with self._lock:
            order = self._require_order(order_id)
            now = self._clock.now()
            self._collect_order(order, now)
            if not force and now < order["deadline_ts"]:
                raise InterlockError(f"调峰指令尚未到对账截止时间: {order_id}", component=COMPONENT)
            if order["reconciliation"] is None:
                self._reconcile(order, now)
                self._persist()
            return order["reconciliation"]

    def order(self, order_id: str) -> dict:
        with self._lock:
            return self._order_view(self._require_order(order_id))

    def station_snapshot(self) -> dict:
        with self._lock:
            return {"stations": [station.as_dict() for station in self._fleet.stations()]}

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "stations": [station.as_dict() for station in self._fleet.stations()],
                "orders": [self._order_view(order) for order in sorted(self._orders.values(), key=lambda item: item["created_ts"])],
                "open_orders": self._open_count(),
            }

    def state(self) -> dict:
        with self._lock:
            return {"orders": {key: self._clone(value) for key, value in self._orders.items()}, "fleet": self._fleet.snapshot()}

    def restore(self, state: dict) -> None:
        with self._lock:
            self._orders = {key: self._clone(value) for key, value in dict(state.get("orders", {})).items()}
            self._fleet.restore(dict(state.get("fleet", {})))

    def _collect_order(self, order: dict, now: float) -> None:
        for station_id in list(order["assignments"]):
            for command in list(order["assignments"][station_id]["commands"]):
                self._collect_command(order, station_id, command, now)
            self._refresh_assignment(order, station_id)

    def _collect_command(self, order: dict, station_id: str, command: dict, now: float) -> None:
        if command["state"] in (EXECUTED, REJECTED, REALLOCATED):
            return
        station = self._fleet.get(station_id)
        observed = self._fleet.station_status(command["command_id"])
        if observed["executed"]:
            command["state"] = EXECUTED
            command["executed_ts"] = now
            command["executed_power_kw"] = observed["power_kw"]
            command["last_error"] = ""
            self._publish_receipt(order, command, "dispatch_execution_confirmed", "notice")
            return
        if observed["delivered"]:
            self._mark_delivered(order, command, now, via="poll")
            return
        if now < command["next_action_ts"]:
            return
        if command["delivery_attempts"] >= order["max_attempts"]:
            if station.online:
                if now < order["deadline_ts"]:
                    self._reallocate_command(order, station_id, command, "confirmed_not_delivered", now)
                else:
                    command["state"] = NOT_DELIVERED
                    command["last_error"] = "delivery_deadline_exceeded"
                    self._publish_receipt(order, command, "dispatch_unconfirmed_gap", "warning")
            else:
                self._schedule_retry(order, command, "station_offline", now, increment_attempt=False)
            return
        if not station.online:
            self._schedule_retry(order, command, "station_offline", now)
            return
        delivery = self._fleet.deliver(command["command_id"])
        command["delivery_attempts"] += 1
        command["last_attempt_ts"] = now
        if delivery["delivered"]:
            self._mark_delivered(order, command, now, via="send", duplicate=delivery["duplicate"])
        else:
            self._schedule_retry(order, command, delivery.get("reason", "station_offline") or "station_offline", now)

    def _schedule_retry(
        self, order: dict, command: dict, reason: str, now: float, *, increment_attempt: bool = True
    ) -> None:
        command["state"] = NOT_DELIVERED
        command["last_error"] = reason
        if increment_attempt:
            command["delivery_attempts"] += 1
        if command["delivery_attempts"] >= order["max_attempts"]:
            # 最后一次尝试失败后，下一轮立即进入改派/缺口判定，避免缺口再多等一个补发节拍。
            command["next_action_ts"] = now
        elif command["next_action_ts"] <= now:
            command["next_action_ts"] = now + order["retry_interval_s"]
        self._publish(
            "dispatch_retry_scheduled",
            {
                "order_id": order["order_id"],
                "command_id": command["command_id"],
                "attempts": command["delivery_attempts"],
                "reason": reason,
            },
            "warning",
        )

    def _reallocate_command(self, order: dict, station_id: str, command: dict, reason: str, now: float) -> None:
        amount = command["power_kw"]
        command["state"] = REALLOCATED
        command["reallocated_reason"] = reason
        command["reallocated_ts"] = now
        command["next_action_ts"] = None
        self._publish(
            "dispatch_gap_reallocated",
            {"order_id": order["order_id"], "from_station": station_id, "power_kw": amount, "reason": reason},
            "warning",
        )
        current = {sid: self._active_target(order["assignments"][sid]) for sid in order["assignments"]}
        stations = [
            station for station in self._fleet.stations(online_only=True)
            if station.station_id != station_id and station.station_id not in self._failed_stations(order)
        ]
        allocation = allocate_target(stations, amount, order["method"], current_allocations=current)
        reallocations = order["allocation"].setdefault("reallocations", [])
        reallocations.append({"from_station": station_id, "reason": reason, "power_kw": round(amount, 3), **allocation})
        for new_station_id, increment in allocation["allocations"].items():
            sources = [command["command_id"]]
            existing = order["assignments"].get(new_station_id)
            if existing is not None:
                sources.extend(existing.get("reallocated_from", []))
                self._add_command(order, self._fleet.get(new_station_id), existing, increment, sources, now)
            else:
                self._add_assignment(order, self._fleet.get(new_station_id), increment, REALLOCATED, sources, now)
        if allocation["shortfall_kw"] > 0:
            order["allocation"]["shortfall_kw"] = round(order["allocation"]["shortfall_kw"] + allocation["shortfall_kw"], 3)
        self._refresh_assignment(order, station_id)

    def _add_assignment(
        self,
        order: dict,
        station: StationInfo,
        target_kw: float,
        origin: str,
        sources: list[str],
        now: float,
    ) -> dict:
        assignment = {
            "station_id": station.station_id,
            "state": NOT_DELIVERED,
            "origin": origin,
            "reallocated_from": list(sources) if origin == REALLOCATED else [],
            "commands": [],
        }
        order["assignments"][station.station_id] = assignment
        self._add_command(order, station, assignment, target_kw, assignment["reallocated_from"], now)
        return assignment

    def _add_command(
        self,
        order: dict,
        station: StationInfo,
        assignment: dict,
        power_kw: float,
        sources: list[str],
        now: float,
    ) -> dict:
        number = len(assignment["commands"]) + 1
        command = {
            "command_id": f"{order['order_id']}:{station.station_id}:c{number}",
            "station_id": station.station_id,
            "command_no": number,
            "power_kw": round(float(power_kw), 3),
            "state": NOT_DELIVERED,
            "delivery_attempts": 0,
            "poll_attempts": 0,
            "created_ts": now,
            "last_attempt_ts": None,
            "next_action_ts": now,
            "delivered_ts": None,
            "executed_ts": None,
            "executed_power_kw": 0.0,
            "last_error": "",
            "reallocated_from": list(sources),
            "reallocated_reason": "",
            "reallocated_ts": None,
            "rejected_power_kw": 0.0,
        }
        assignment["commands"].append(command)
        delivery = self._fleet.deliver(command["command_id"])
        command["last_attempt_ts"] = now
        if delivery["delivered"]:
            command["delivery_attempts"] = 1
            self._mark_delivered(order, command, now, via="send", duplicate=delivery["duplicate"])
        else:
            command["delivery_attempts"] = 1
            self._schedule_retry(
                order,
                command,
                delivery.get("reason", "station_offline") or "station_offline",
                now,
                increment_attempt=False,
            )
        self._refresh_assignment(order, station.station_id)
        return command

    def _mark_delivered(self, order: dict, command: dict, now: float, *, via: str, duplicate: bool = False) -> None:
        first = command["delivered_ts"] is None
        command["state"] = DELIVERED_PENDING
        command["last_error"] = ""
        command["next_action_ts"] = now + order["retry_interval_s"]
        if first:
            command["delivered_ts"] = now
        if via == "poll":
            command["poll_attempts"] += 1
        if first:
            self._publish_receipt(order, command, "dispatch_delivery_confirmed", "notice")

    def _reconcile(self, order: dict, now: float) -> None:
        stations = []
        totals = {
            "target_kw": order["target_kw"],
            "allocated_kw": round(order["target_kw"] - order["allocation"]["shortfall_kw"], 3),
            "delivered_kw": 0.0,
            "executed_kw": 0.0,
            "delivered_pending_kw": 0.0,
            "not_delivered_kw": 0.0,
            "rejected_kw": 0.0,
            "reallocated_out_kw": 0.0,
        }
        for station_id, assignment in sorted(order["assignments"].items()):
            self._refresh_assignment(order, station_id)
            commands = assignment["commands"]
            executed = sum(item["executed_power_kw"] for item in commands if item["state"] == EXECUTED)
            pending = sum(item["power_kw"] for item in commands if item["state"] == DELIVERED_PENDING)
            missing = sum(item["power_kw"] for item in commands if item["state"] == NOT_DELIVERED)
            rejected = sum(
                item.get("rejected_power_kw", item["power_kw"] if item["state"] == REJECTED else 0.0)
                for item in commands
            )
            moved_out = sum(item["power_kw"] for item in commands if item["state"] == REALLOCATED)
            rejected_power = rejected
            totals["delivered_kw"] += executed + pending
            totals["executed_kw"] += executed
            totals["delivered_pending_kw"] += pending
            totals["not_delivered_kw"] += missing
            totals["rejected_kw"] += rejected_power
            totals["reallocated_out_kw"] += moved_out
            stations.append(
                {
                    "station_id": station_id,
                    "state": assignment["state"],
                    "target_kw": self._active_target(assignment),
                    "executed_kw": round(executed, 3),
                    "delivered_pending_kw": round(pending, 3),
                    "not_delivered_kw": round(missing, 3),
                    "rejected_kw": round(rejected_power, 3),
                    "reallocated_out_kw": round(moved_out, 3),
                    "commands": [self._clone(item) for item in commands],
                }
            )
        shortfall = round(totals["target_kw"] - totals["executed_kw"], 3)
        reconciliation = {
            "order_id": order["order_id"],
            "reconciled_at": now,
            "deadline_ts": order["deadline_ts"],
            "method": order["method"],
            "balanced": shortfall == 0.0,
            "shortfall_kw": max(0.0, shortfall),
            **{key: round(value, 3) for key, value in totals.items() if key.endswith("_kw")},
            "stations": stations,
        }
        order["reconciliation"] = reconciliation
        self._publish(
            "dispatch_reconciled",
            {"order_id": order["order_id"], "balanced": reconciliation["balanced"], "shortfall_kw": reconciliation["shortfall_kw"]},
            "notice" if reconciliation["balanced"] else "warning",
        )

    def _refresh_assignment(self, order: dict, station_id: str) -> dict:
        assignment = order["assignments"][station_id]
        states = [command["state"] for command in assignment["commands"]]
        active = [state for state in states if state in ACTIVE_STATES]
        if active and all(state == EXECUTED for state in active):
            assignment["state"] = EXECUTED
        elif NOT_DELIVERED in active:
            assignment["state"] = NOT_DELIVERED
        elif DELIVERED_PENDING in active:
            assignment["state"] = DELIVERED_PENDING
        elif REJECTED in states:
            assignment["state"] = REJECTED
        elif REALLOCATED in states:
            assignment["state"] = REALLOCATED
        else:
            assignment["state"] = EXECUTED
        return assignment

    def _failed_stations(self, order: dict) -> set[str]:
        failed = set()
        for station_id, assignment in order["assignments"].items():
            if assignment["state"] in (REJECTED, REALLOCATED) and self._active_target(assignment) == 0.0:
                failed.add(station_id)
        return failed

    def _select_stations(self, station_ids: list[str] | None) -> list[StationInfo]:
        stations = self._fleet.stations(online_only=True)
        if station_ids is None:
            return stations
        selected = {station.station_id: station for station in stations}
        missing = [station_id for station_id in station_ids if station_id not in selected]
        if missing:
            raise InterlockError(f"站点不可用或不在线: {missing}", component=COMPONENT)
        return [selected[station_id] for station_id in station_ids]

    def _validate_submission(self, order_id: str, target_kw: float, method: str, deadline_s: float) -> None:
        if not order_id:
            raise InterlockError("调度指令标识不能为空", component=COMPONENT)
        if float(target_kw) < 0.0:
            raise InterlockError("调峰目标不能为负", component=COMPONENT)
        if method not in ALLOCATION_METHODS:
            raise InterlockError(f"未知分摊方式: {method}", component=COMPONENT)
        if float(deadline_s) <= 0.0:
            raise InterlockError("对账截止时长必须为正", component=COMPONENT)
        if not self._fleet.stations():
            raise InterlockError("尚未登记任何电站", component=COMPONENT)

    def _find_fingerprint_duplicate(self, fingerprint: str, now: float) -> dict | None:
        for order in self._orders.values():
            if order["fingerprint"] == fingerprint and now - order["created_ts"] <= DUPLICATE_WINDOW_S:
                return order
        return None

    def _require_order(self, order_id: str) -> dict:
        order = self._orders.get(order_id)
        if order is None:
            raise InterlockError(f"未知调峰指令: {order_id}", component=COMPONENT)
        return order

    def _command(self, order_id: str | dict, station_id: str, command_no: int) -> dict:
        order = self._require_order(order_id) if isinstance(order_id, str) else order_id
        assignment = order["assignments"].get(station_id)
        if assignment is None:
            raise InterlockError(f"站点未承接该指令: {station_id}", component=COMPONENT)
        for command in assignment["commands"]:
            if command["command_no"] == int(command_no):
                return command
        raise InterlockError(f"站端指令序号不存在: {station_id}:c{command_no}", component=COMPONENT)

    def _publish_receipt(self, order: dict, command: dict, action: str, severity: str) -> None:
        self._publish(
            action,
            {
                "order_id": order["order_id"],
                "command_id": command["command_id"],
                "station_id": command["station_id"],
                "power_kw": command["power_kw"],
                "state": command["state"],
            },
            severity,
        )

    def _publish(self, action: str, payload: dict, severity: str) -> None:
        if self._event_sink is not None:
            self._event_sink(action, payload, severity)

    def _persist(self) -> None:
        if self._on_change is not None:
            self._on_change()

    def _order_view(self, order: dict, *, duplicate: bool = False) -> dict:
        view = self._clone(order)
        view["duplicate"] = duplicate
        view["state"] = "reconciled" if order["reconciliation"] else "open"
        view["assignments"] = {sid: self._assignment_view(item) for sid, item in order["assignments"].items()}
        return view

    def _assignment_view(self, assignment: dict) -> dict:
        view = self._clone(assignment)
        view["target_kw"] = self._active_target(assignment)
        return view

    def _active_target(self, assignment: dict) -> float:
        return round(
            sum(item["power_kw"] for item in assignment["commands"] if item["state"] in ACTIVE_STATES),
            3,
        )

    def _open_count(self) -> int:
        return sum(1 for order in self._orders.values() if order["reconciliation"] is None)

    @staticmethod
    def _fingerprint(target_kw: float, method: str, station_ids: list[str] | None) -> str:
        raw = json.dumps(
            {"target_kw": round(float(target_kw), 3), "method": method, "stations": sorted(station_ids or [])},
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _clone(value: Any) -> Any:
        return json.loads(json.dumps(value, ensure_ascii=False))
