"""集群调峰调度编排台账。

``DispatchLedger`` 把目标分解、下发、两段回执回收、超时补发、断线续传、
缺口改摊与调度端对账串成一条幂等流水线，所有状态迁移同时写事件流水和
``dispatch_orders`` 状态文档：进程重启后从文档恢复，未闭合的命令继续按
原 ``command_id`` 补发，不会变成第二次执行。

时序由外部时钟驱动（验证用可推进时钟）：调用 :meth:`poll` 推进一个节
拍，补发、超时判定与改摊都在节拍里发生，不依赖真实线程定时器。
"""

from __future__ import annotations

from ..eventbus import Event
from ..errors import InterlockError
from ..store.journal import JsonlJournal
from ..store.jsonfile import JsonStore
from .allocation import (
    CAUSE_NACK,
    CAUSE_TIMEOUT,
    AllocationPlan,
    Share,
    allocate,
    reallocate,
)
from .fleet import Fleet
from .orders import (
    CMD_ACKED,
    CMD_EXECUTED,
    CMD_EXEC_REJECTED,
    CMD_NACK,
    CMD_NOT_EXECUTED,
    CMD_PENDING,
    CMD_SENT,
    CMD_SUPERSEDED,
    CMD_UNDELIVERED,
    DONE_STATES,
    ORDER_ALLOCATED,
    ORDER_SETTLED,
    DispatchOrder,
    StationCommand,
    make_command_id,
)
from .agent import PEER_EXECUTED, PEER_RECEIVED, PEER_REJECTED, StationAgent
from .settings import STRATEGY_CAPABILITY, STRATEGY_INSTALLED, DispatchSettings

STATE_DOC = "dispatch_orders"


class DispatchLedger:
    """调度中心侧：目标分解到对账的全过程台账。"""

    def __init__(self, clock, bus, store: JsonStore, settings: DispatchSettings | None = None) -> None:
        self.clock = clock
        self.bus = bus
        self.store = store
        self.settings = (settings or DispatchSettings.default()).validated()
        self.fleet = Fleet()
        self.journal = JsonlJournal(store.root, "dispatch", segment_limit=512)
        self._agents: dict[str, StationAgent] = {}
        self._orders: dict[str, DispatchOrder] = {}
        self._restore()

    # ------------------------------------------------------------------ 名册

    def register_station(
        self,
        station_id: str,
        name: str,
        installed_kw: float,
        headroom_kw: float,
        auto_execute: bool = True,
    ) -> dict:
        """登记成员站并挂上对端代理；默认上线，等待能力上报。"""

        station = self.fleet.register(station_id, name, installed_kw, headroom_kw)
        self._agents[station_id] = StationAgent(station_id, name, auto_execute=auto_execute)
        self.fleet.set_online(station_id, True)
        self._event("station_registered", station_id=station_id, installed_kw=installed_kw, headroom_kw=headroom_kw)
        return station.as_dict()

    def agent(self, station_id: str) -> StationAgent:
        return self._agents[self.fleet.get(station_id).station_id]

    def station_online(self, station_id: str) -> dict:
        self.fleet.set_online(station_id, True)
        self.agent(station_id).set_link(True)
        self._event("station_online", station_id=station_id)
        return self.reconnect(station_id)

    def station_offline(self, station_id: str) -> dict:
        self.fleet.set_online(station_id, False)
        self.agent(station_id).set_link(False)
        self._event("station_offline", station_id=station_id)
        return {"station_id": station_id, "online": False}

    def report_capability(self, station_id: str, capability_kw: float) -> dict:
        now = self.clock.now()
        station = self.fleet.report_capability(station_id, capability_kw, now)
        self._event("capability_reported", station_id=station_id, capability_kw=round(station.capability_kw or 0.0, 3), at=now)
        return station.as_dict()

    def fleet_snapshot(self) -> dict:
        snapshot = self.fleet.snapshot()
        snapshot["capabilities"] = self.fleet.capabilities(self.clock.now(), self.settings.capability_ttl_s)
        snapshot["orders"] = len(self._orders)
        return snapshot

    # -------------------------------------------------------------- 指令入口

    def receive(self, dispatch_id: str, target_kw: float, strategy: str | None = None, now: float | None = None) -> dict:
        """接收一条调度目标并立即分解、首拍下发。

        同号重发（含同一分钟内多次）直接返回原指令并累计 ``duplicate_hits``；
        同号但目标或策略不同按冲突拒绝，绝不二次执行。
        """

        moment = self.clock.now() if now is None else now
        existing = self._orders.get(dispatch_id)
        if existing is not None:
            chosen = strategy or self.settings.strategy
            if abs(existing.target_kw - float(target_kw)) > 1e-9 or existing.strategy != chosen:
                raise InterlockError(
                    f"调度指令号 {dispatch_id} 内容与已收指令冲突", component="dispatch"
                )
            existing.duplicate_hits += 1
            self._event("dispatch_duplicate", dispatch_id=dispatch_id, duplicate_hits=existing.duplicate_hits, at=moment)
            self._persist()
            return {"duplicate": True, "order": existing.as_dict()}

        chosen = strategy or self.settings.strategy
        if chosen not in (STRATEGY_INSTALLED, STRATEGY_CAPABILITY):
            raise InterlockError(f"未知分摊策略: {chosen}", component="dispatch")
        plan = allocate(
            chosen,
            float(target_kw),
            installed={s.station_id: s.installed_kw for s in self.fleet.all()},
            headroom={s.station_id: s.headroom_kw for s in self.fleet.all()},
            capabilities=self.fleet.capabilities(moment, self.settings.capability_ttl_s),
            step_kw=self.settings.share_step_kw,
        )
        order = DispatchOrder(
            dispatch_id=dispatch_id,
            target_kw=float(target_kw),
            strategy=chosen,
            created_ts=moment,
            state=ORDER_ALLOCATED,
            plan=plan.as_dict(),
        )
        for share in plan.shares:
            command_id = make_command_id(dispatch_id, share.station_id, 0)
            order.commands[command_id] = _new_command(command_id, dispatch_id, share.station_id, 0, share.kw, moment)
        # 装机法的初始缺口（离线站名义份额、在线站被能力钳住的份额）先入账，
        # 首轮节拍的改摊水瀑负责把它改摊给有余量的站。
        nominal = plan.nominal
        for station_id, cause in plan.gap_causes.items():
            if station_id == "__fleet__":
                continue
            gap_kw_value = round(max(0.0, nominal.get(station_id, 0.0) - plan.share(station_id)), 6)
            if gap_kw_value <= 0:
                continue
            order.gap_events.append(
                {
                    "command_id": make_command_id(dispatch_id, station_id, 0),
                    "station_id": station_id,
                    "kw": gap_kw_value,
                    "revision": 0,
                    "cause": cause,
                    "reason": "initial_offline" if cause == "offline" else "initial_capped",
                    "ts": moment,
                }
            )
        self._orders[dispatch_id] = order
        self._event(
            "dispatch_received",
            dispatch_id=dispatch_id,
            target_kw=round(float(target_kw), 3),
            strategy=chosen,
            stations=len(plan.shares),
            shortfall_kw=plan.shortfall_kw,
            excluded=",".join(plan.excluded),
            at=moment,
        )
        self._flush(order, moment)
        self._persist()
        return {"duplicate": False, "order": order.as_dict(), "plan": plan.as_dict()}

    # -------------------------------------------------------------- 回执回收

    def receive_ack(self, dispatch_id: str, station_id: str, ok: bool, reason: str | None = None, now: float | None = None) -> dict:
        """回收接收回执：ACK 回答"收到了"，NACK 回答"到站但被对端拒收"。"""

        moment = self.clock.now() if now is None else now
        order = self.order(dispatch_id)
        command = self._current(order, station_id)
        if command.state not in (CMD_PENDING, CMD_SENT, CMD_ACKED):
            return {"station_id": station_id, "state": command.state, "duplicate": True}
        if ok:
            command.state = CMD_ACKED
            command.acked_ts = moment
            self._event("station_ack", dispatch_id=dispatch_id, station_id=station_id, command_id=command.command_id, at=moment)
        else:
            self._mark_gap(order, command, CMD_NACK, CAUSE_NACK, reason or "nack", moment)
        self._persist()
        return {"station_id": station_id, "state": command.state, "command_id": command.command_id}

    def receive_exec(
        self,
        dispatch_id: str,
        station_id: str,
        ok: bool,
        actual_kw: float | None = None,
        reason: str | None = None,
        now: float | None = None,
    ) -> dict:
        """回收执行回执：区分"没收到"与"收到了没执行"的第二栏。"""

        moment = self.clock.now() if now is None else now
        order = self.order(dispatch_id)
        command = self._current(order, station_id)
        if command.state == CMD_EXECUTED:
            return {"station_id": station_id, "state": command.state, "duplicate": True}
        if ok:
            delivered = min(command.kw, command.kw if actual_kw is None else float(actual_kw))
            command.state = CMD_EXECUTED
            command.acked_ts = command.acked_ts or moment
            command.executed_ts = moment
            command.actual_kw = max(0.0, delivered)
            # 晚到的成功回执会把先前的超时/失败判定翻回已执行，对应缺口必须核销，
            # 否则同一份额既计执行又计缺口。
            self._resolve_gaps(order, command.command_id, moment)
            self._event(
                "station_executed",
                dispatch_id=dispatch_id,
                station_id=station_id,
                command_id=command.command_id,
                kw=round(command.actual_kw, 3),
                at=moment,
            )
        else:
            self._mark_gap(order, command, CMD_EXEC_REJECTED, CAUSE_NACK, reason or "exec_rejected", moment)
        self._persist()
        return {"station_id": station_id, "state": command.state, "actual_kw": command.actual_kw}

    # ---------------------------------------------------------- 节拍：补发/超时

    def poll(self, now: float | None = None) -> dict:
        """推进一个调度节拍：补发未回执指令、判超时、对坐实缺口改摊。"""

        moment = self.clock.now() if now is None else now
        for order in list(self._orders.values()):
            if order.state == ORDER_SETTLED:
                continue
            self._flush(order, moment)
            self._sweep_timeouts(order, moment)
            self._maybe_reallocate(order, moment)
        self._persist()
        return self.tick_summary()

    def tick_summary(self) -> dict:
        """返回当前全部在途指令的状态计数。"""

        counts: dict[str, int] = {}
        open_total = 0
        gap_total = 0
        for order in self._orders.values():
            if order.state == ORDER_SETTLED:
                continue
            for command in self._latest_commands(order).values():
                counts[command.state] = counts.get(command.state, 0) + 1
            open_total += len(order.open_commands())
            gap_total += len(self._latest_gap_commands(order))
        return {
            "open_orders": sum(1 for o in self._orders.values() if o.state != ORDER_SETTLED),
            "open_commands": open_total,
            "gap_commands": gap_total,
            "states": counts,
        }

    @staticmethod
    def _latest_gap_commands(order: DispatchOrder) -> list:
        """返回当前版本里已坐实失败（是缺口）的命令。"""

        return [command for command in DispatchLedger._latest_commands(order).values() if command.is_gap()]

    def reconnect(self, station_id: str, now: float | None = None) -> dict:
        """站点重连续传：按当前命令号查对端事实，区分没收到／没执行／已执行。"""

        moment = self.clock.now() if now is None else now
        diagnoses: list[dict] = []
        for order in self._orders.values():
            if station_id not in self._latest_commands(order):
                continue
            command = self._latest_commands(order)[station_id]
            peer = self.agent(station_id).resync(command.command_id)
            diagnosis = {"dispatch_id": order.dispatch_id, "command_id": command.command_id, **peer}
            if peer["state"] == PEER_EXECUTED:
                if command.state != CMD_EXECUTED:
                    command.state = CMD_EXECUTED
                    command.acked_ts = command.acked_ts or peer.get("received_ts", moment)
                    command.executed_ts = peer.get("executed_ts") or moment
                    command.actual_kw = peer.get("actual_kw") if peer.get("actual_kw") is not None else command.kw
                    self._resolve_gaps(order, command.command_id, moment)
                    self._event("station_resynced_executed", dispatch_id=order.dispatch_id, station_id=station_id, command_id=command.command_id, at=moment)
            elif peer["state"] == PEER_REJECTED:
                if command.state not in (CMD_EXEC_REJECTED, CMD_NACK):
                    self._mark_gap(order, command, CMD_EXEC_REJECTED, CAUSE_NACK, peer.get("reason") or "exec_rejected", moment)
            elif peer["state"] == PEER_RECEIVED:
                if command.state in (CMD_PENDING, CMD_SENT):
                    command.state = CMD_ACKED
                    command.acked_ts = peer.get("received_ts", moment)
                # 已收未执行：重连后明确续执行，对端同命令号只执行一次，
                # 成功与否由对端事实（execute 的返回）决定，区分"没收到"与"没执行"。
                resumed = self.agent(station_id).execute(command.command_id, moment)
                if resumed.get("state") == PEER_EXECUTED and resumed.get("ok"):
                    command.state = CMD_EXECUTED
                    command.executed_ts = moment
                    command.actual_kw = resumed.get("actual_kw") if resumed.get("actual_kw") is not None else command.kw
                    self._resolve_gaps(order, command.command_id, moment)
                    self._event("station_resynced_executed", dispatch_id=order.dispatch_id, station_id=station_id, command_id=command.command_id, resumed=True, at=moment)
                elif resumed.get("state") == PEER_REJECTED:
                    self._mark_gap(order, command, CMD_EXEC_REJECTED, CAUSE_NACK, resumed.get("reason") or "exec_rejected", moment)
                else:
                    # 对端暂时不动作：执行窗口从重连时刻重新起算，继续等执行回执。
                    command.acked_ts = moment
            else:
                # 对端查无此令 = 之前没送到；按原命令号立即重投，仍是同一条指令。
                command.state = CMD_PENDING
                command.next_due_ts = moment
                self._event("station_resync_resend", dispatch_id=order.dispatch_id, station_id=station_id, command_id=command.command_id, at=moment)
                self._send(order, command, moment)
            diagnoses.append(diagnosis)
        self._persist()
        return {"station_id": station_id, "diagnoses": diagnoses}

    # -------------------------------------------------------------- 结算/对账

    def settle(self, dispatch_id: str, now: float | None = None) -> dict:
        """闭合一条指令：必须已无在途命令，按实际回执与各原因缺口出账。"""

        moment = self.clock.now() if now is None else now
        order = self.order(dispatch_id)
        self._flush(order, moment)
        self._sweep_timeouts(order, moment)
        self._maybe_reallocate(order, moment)
        current = self._latest_commands(order)
        open_ids = [cmd.command_id for cmd in current.values() if cmd.state not in DONE_STATES]
        if open_ids:
            raise InterlockError(f"指令 {dispatch_id} 尚有未闭合命令: {open_ids}", component="dispatch")
        executed = round(sum(cmd.actual_kw or 0.0 for cmd in current.values() if cmd.state == CMD_EXECUTED), 6)
        # 缺口按"缺口事件 kw - 该缺口后续被改摊走的量"计入，避免残量双算。
        moved_by_command = self._moved_by_command(order)
        gaps: dict[str, float] = {}
        for event in order.gap_events:
            if event.get("resolved"):
                # 该缺口后经续传或晚到回执恢复为已执行，不再算缺口。
                continue
            outstanding = round(max(0.0, event["kw"] - moved_by_command.get(event["command_id"], 0.0)), 6)
            if outstanding <= 0:
                continue
            label = event["reason"]
            gaps[label] = round(gaps.get(label, 0.0) + outstanding, 6)
        plan_shortfall = float(order.plan["shortfall_kw"]) if order.plan else 0.0
        residual = round(sum(item.get("residual_kw", 0.0) for item in order.reallocations), 6)
        shortfall = round(max(0.0, order.target_kw - executed), 6)
        order.declared_shortfall_kw = shortfall
        order.state = ORDER_SETTLED
        statement = {
            "dispatch_id": dispatch_id,
            "strategy": order.strategy,
            "target_kw": round(order.target_kw, 3),
            "executed_kw": round(executed, 3),
            "station_gaps_kw": {key: round(value, 3) for key, value in sorted(gaps.items())},
            "plan_shortfall_kw": round(plan_shortfall, 3),
            "reallocation_residual_kw": round(residual, 3),
            "declared_shortfall_kw": round(shortfall, 3),
            "reallocations": order.reallocations,
            "stations": {
                station_id: {"kw": round(cmd.kw, 3), "actual_kw": cmd.actual_kw, "state": cmd.state}
                for station_id, cmd in sorted(current.items())
            },
            "ts": moment,
        }
        order.reconciliation = statement
        self._event("dispatch_settled", dispatch_id=dispatch_id, executed_kw=round(executed, 3), shortfall_kw=round(shortfall, 3), at=moment)
        self._persist()
        return statement

    def reconcile(self, dispatch_id: str, dispatcher: dict | None = None, now: float | None = None) -> dict:
        """与调度端对一次账。

        无调度端回单时出具平台对账单（含逐站与总量）；带回单时逐站比对，
        超过分摊粒度的偏差列入 ``variances``，总量一致才判 ``matched``。
        对账结果冻结进指令，之后不再改动。
        """

        moment = self.clock.now() if now is None else now
        order = self.order(dispatch_id)
        if order.state != ORDER_SETTLED:
            statement = self.settle(dispatch_id, moment)
        else:
            prior = order.reconciliation
            # settle 时写入的就是对账单；reconcile 之后写入的是带比对结果的报告。
            statement = prior.get("platform", prior) if isinstance(prior, dict) and "platform" in prior else (prior or {})
        tolerance = self.settings.share_step_kw
        report = {"platform": statement, "dispatcher": dispatcher, "matched": True, "variances": [], "ts": moment}
        if dispatcher:
            their_total = dispatcher.get("total_kw")
            their_stations = dispatcher.get("stations") or {}
            platform_stations = statement.get("stations", {})
            for station_id, theirs in sorted(dict(their_stations).items()):
                ours = platform_stations.get(station_id, {}).get("actual_kw")
                ours = 0.0 if ours is None else float(ours)
                if abs(ours - float(theirs)) > tolerance:
                    report["variances"].append(
                        {"station_id": station_id, "platform_kw": round(ours, 3), "dispatcher_kw": float(theirs), "delta_kw": round(float(theirs) - ours, 3)}
                    )
            report["matched_total"] = their_total is None or abs(float(statement["executed_kw"]) - float(their_total)) <= tolerance
            report["matched"] = not report["variances"] and report["matched_total"]
        else:
            report["matched_total"] = True
        order.reconciliation = report
        self._event("dispatch_reconciled", dispatch_id=dispatch_id, matched=report["matched"], variances=len(report["variances"]), at=moment)
        self._persist()
        return report

    # ------------------------------------------------------------------ 查询

    def order(self, dispatch_id: str) -> DispatchOrder:
        if dispatch_id not in self._orders:
            raise KeyError(f"未知调度指令: {dispatch_id}")
        return self._orders[dispatch_id]

    def orders(self) -> list[dict]:
        return [order.as_dict() for _, order in sorted(self._orders.items())]

    # ------------------------------------------------------------ 内部流水线

    def _flush(self, order: DispatchOrder, now: float) -> None:
        """对到点且未回执的命令执行首拍下发或补发。"""

        for command in self._latest_commands(order).values():
            if command.state not in (CMD_PENDING, CMD_SENT):
                continue
            due = command.next_due_ts is None or now >= command.next_due_ts
            if not due:
                continue
            if command.attempts > self.settings.retry_max:
                continue
            self._send(order, command, now)

    def _send(self, order: DispatchOrder, command, now: float) -> None:
        agent = self._agents.get(command.station_id)
        if agent is None:
            return
        result = agent.deliver(command.command_id, command.kw, now)
        command.attempts += 1
        command.last_attempt_ts = now
        command.next_due_ts = now + self.settings.retry_backoff_s * (2 ** max(0, command.attempts - 1))
        if not result["delivered"]:
            command.state = CMD_SENT
            self._event("command_send_failed", dispatch_id=order.dispatch_id, station_id=command.station_id, command_id=command.command_id, attempts=command.attempts, at=now)
            return
        command.state = CMD_ACKED
        command.acked_ts = now
        peer_state = result.get("state")
        self._event("station_ack", dispatch_id=order.dispatch_id, station_id=command.station_id, command_id=command.command_id, duplicate=result.get("duplicate", False), attempts=command.attempts, at=now)
        if peer_state == PEER_EXECUTED:
            command.state = CMD_EXECUTED
            command.executed_ts = now
            command.actual_kw = command.kw
            self._event("station_executed", dispatch_id=order.dispatch_id, station_id=command.station_id, command_id=command.command_id, kw=round(command.kw, 3), duplicate=result.get("duplicate", False), at=now)
        elif peer_state == PEER_REJECTED:
            self._mark_gap(order, command, CMD_EXEC_REJECTED, CAUSE_NACK, "exec_rejected", now)

    def _sweep_timeouts(self, order: DispatchOrder, now: float) -> None:
        """ACK 超时=没收到；执行回执超时=收到了没执行。"""

        for command in list(self._latest_commands(order).values()):
            if command.state in (CMD_SENT, CMD_PENDING) and command.last_attempt_ts is not None:
                retries_exhausted = command.attempts > self.settings.retry_max
                ack_deadline_passed = now - command.last_attempt_ts >= self.settings.ack_timeout_s
                # 补发次数没用完前继续等下一拍重发；次数用尽且 ACK 超时，判"没收到"。
                if retries_exhausted and ack_deadline_passed:
                    self._mark_gap(order, command, CMD_UNDELIVERED, CAUSE_TIMEOUT, "ack_timeout", now)
            elif command.state == CMD_ACKED and command.acked_ts is not None:
                if now - command.acked_ts >= self.settings.execute_timeout_s:
                    self._mark_gap(order, command, CMD_NOT_EXECUTED, CAUSE_TIMEOUT, "execute_timeout", now)

    def _maybe_reallocate(self, order: DispatchOrder, now: float) -> None:
        """把已坐实缺口按当下余量改摊；接不下的残量留待对账显式确认。

        缺口来源有两类，统一进同一轮水瀑：装机法首拍的离线／钳位名义份额，
        以及运行期 NACK、未送达、未执行坐实的份额。已被某轮改摊处理过的
        缺口事件不重复进账。
        """

        rounds = len(order.reallocations)
        if rounds >= self.settings.reallocate_max:
            return
        current = self._latest_commands(order)
        handled = {gap_id for item in order.reallocations for gap_id in item.get("gap_ids", ())}
        gap_kw: dict[str, float] = {}
        gap_ids: list[str] = []
        for event in order.gap_events:
            if event.get("resolved"):
                # 已通过续传/晚到回执确认实际执行并核销的缺口，不能再改摊。
                continue
            if event["revision"] != order.revision and event["cause"] not in ("offline", "capped"):
                continue
            if event["command_id"] in handled:
                continue
            gap_kw[event["station_id"]] = round(gap_kw.get(event["station_id"], 0.0) + event["kw"], 6)
            gap_ids.append(event["command_id"])
        if not gap_kw:
            return
        caps = {}
        for station in self.fleet.all():
            if station.station_id in gap_kw:
                continue
            capability = station.fresh_capability(now, self.settings.capability_ttl_s)
            if capability is None:
                continue
            committed = current[station.station_id].kw if station.station_id in current else 0.0
            caps[station.station_id] = max(0.0, capability - committed)
        base_plan = AllocationPlan(
            strategy=order.strategy,
            target_kw=order.target_kw,
            shares=[_share_of(cmd) for cmd in current.values() if cmd.station_id not in gap_kw],
            nominal=order.plan.get("nominal", {}) if order.plan else {},
            excluded=[],
            shortfall_kw=0.0,
            gap_causes={},
        )
        deltas, residual = reallocate(base_plan, gap_kw, caps, self.settings.share_step_kw)
        # 各缺口被本轮水瀑真正接走的量：按本轮 delta 总量比例摊到各缺口，
        # 接不下的残量留在原缺口里，结算时只计未被接走的部分，不双算。
        gap_total = round(sum(gap_kw.values()), 6)
        moved_total = round(sum(deltas.values()), 6)
        gap_moved: dict[str, float] = {}
        if gap_total > 0 and moved_total > 0:
            # 按本轮处理的每个缺口事件，依各缺口占比分摊被接走的总量，
            # 单个缺口最多接走它自己的 kw；接不下的残量自然留在缺口里。
            remaining = moved_total
            pending = [(event["command_id"], event["kw"]) for event in order.gap_events if event["command_id"] in set(gap_ids) - handled]
            for index, (command_id, value) in enumerate(pending):
                if index == len(pending) - 1:
                    moved = remaining
                else:
                    moved = round(moved_total * value / gap_total, 6)
                moved = min(value, moved, remaining)
                gap_moved[command_id] = round(gap_moved.get(command_id, 0.0) + moved, 6)
                remaining = round(remaining - moved, 6)
        order.revision += 1
        # 缺口站的旧命令一律作废，避免同一份额既挂缺口又被结算重复计算。
        for station_id in gap_kw:
            holder = current.get(station_id)
            if holder is not None and holder.state not in (CMD_EXECUTED,):
                holder.state = CMD_SUPERSEDED
        for station_id, delta in deltas.items():
            previous = current[station_id]
            new_kw = round(previous.kw + delta, 6)
            command_id = make_command_id(order.dispatch_id, station_id, order.revision)
            new_command = _new_command(command_id, order.dispatch_id, station_id, order.revision, new_kw, now)
            order.commands[command_id] = new_command
            if previous.state in (CMD_PENDING, CMD_SENT, CMD_ACKED):
                previous.state = CMD_SUPERSEDED
            self._send(order, new_command, now)
        record = {
            "revision": order.revision,
            "gap_ids": sorted(gap_ids),
            "gap_kw": gap_total,
            "gap_moved": {key: round(value, 3) for key, value in sorted(gap_moved.items())},
            "deltas": {key: round(value, 3) for key, value in sorted(deltas.items())},
            "residual_kw": round(residual, 3),
            "ts": now,
        }
        order.reallocations.append(record)
        self._event(
            "dispatch_reallocated",
            dispatch_id=order.dispatch_id,
            revision=order.revision,
            gap_kw=record["gap_kw"],
            moved_kw=round(sum(deltas.values()), 3),
            residual_kw=round(residual, 3),
            at=now,
        )

    @staticmethod
    def _moved_by_command(order: DispatchOrder) -> dict[str, float]:
        """汇总每条缺口命令历轮被改摊接走的量。"""

        moved: dict[str, float] = {}
        for item in order.reallocations:
            for command_id, value in (item.get("gap_moved") or {}).items():
                moved[command_id] = round(moved.get(command_id, 0.0) + value, 6)
        return moved

    def _mark_gap(self, order: DispatchOrder, command, state: str, cause: str, reason: str, now: float) -> None:
        command.state = state
        command.reason = reason
        order.gap_events.append(
            {"command_id": command.command_id, "station_id": command.station_id, "kw": round(command.kw, 3), "revision": command.revision, "cause": cause, "reason": reason, "ts": now, "resolved": False}
        )
        self._event(
            "station_gap",
            dispatch_id=order.dispatch_id,
            station_id=command.station_id,
            command_id=command.command_id,
            state=state,
            cause=cause,
            kw=round(command.kw, 3),
            at=now,
        )

    def _resolve_gaps(self, order: DispatchOrder, command_id: str, now: float) -> None:
        """核销某命令尚未被改摊的缺口：续传/晚到回执确认它其实已执行。"""

        resolved = 0
        moved = self._moved_by_command(order).get(command_id, 0.0)
        for event in order.gap_events:
            if event["command_id"] != command_id or event.get("resolved"):
                continue
            if event["kw"] - moved <= 0:
                # 这部分缺口已经改摊给别的站，不能核销，否则会把别处的份额抹掉。
                continue
            event["resolved"] = True
            event["resolved_ts"] = now
            resolved += 1
        if resolved:
            self._event(
                "station_gap_resolved",
                dispatch_id=order.dispatch_id,
                command_id=command_id,
                count=resolved,
                at=now,
            )

    @staticmethod
    def _current(order: DispatchOrder, station_id: str):
        commands = DispatchLedger._latest_commands(order)
        if station_id not in commands:
            raise KeyError(f"指令 {order.dispatch_id} 未分摊给站点 {station_id}")
        return commands[station_id]

    @staticmethod
    def _latest_commands(order: DispatchOrder) -> dict:
        """每站只取最高版本命令作为当前命令。"""

        latest: dict[str, object] = {}
        for command in order.commands.values():
            holder = latest.get(command.station_id)
            if holder is None or command.revision > holder.revision:
                latest[command.station_id] = command
        return latest

    # -------------------------------------------------------------- 事件/落盘

    def _event(self, action: str, **payload) -> None:
        record = {"action": action, **payload}
        self.journal.append(record)
        self.bus.publish(Event.of("dispatch", "dispatch", action, **{k: v for k, v in payload.items() if _is_scalar(v)}))

    def _persist(self) -> None:
        self.store.write(STATE_DOC, {"orders": [order.as_dict() for order in self._orders.values()]})

    def _restore(self) -> None:
        data = self.store.read(STATE_DOC)
        if isinstance(data, dict):
            for item in data.get("orders", []):
                order = DispatchOrder.from_dict(item)
                self._orders[order.dispatch_id] = order


def _new_command(command_id: str, dispatch_id: str, station_id: str, revision: int, kw: float, now: float) -> StationCommand:
    command = StationCommand(
        command_id=command_id,
        dispatch_id=dispatch_id,
        station_id=station_id,
        revision=revision,
        kw=round(float(kw), 6),
    )
    command.next_due_ts = now
    return command


def _share_of(command):
    from .allocation import Share

    return Share(station_id=command.station_id, kw=command.kw, basis_kw=command.kw, capped=False)


def _is_scalar(value) -> bool:
    return not isinstance(value, (dict, list, tuple, set))
