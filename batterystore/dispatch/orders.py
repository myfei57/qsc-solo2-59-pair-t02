"""调度指令与站点命令的状态模型。

两级幂等键：

* ``dispatch_id`` 是调度中心给的指令号。同一分钟重发多次，只要 ``dispatch_id``
  相同且内容一致，平台直接返回原指令结果，绝不第二次分解、第二次执行；
  号相同而内容不同则按冲突拒绝，不做静默覆盖。
* ``command_id`` 是平台对每站每次修订生成的确定性编号
  （``<dispatch_id>:<station_id>:r<版本>``）。站端对同一 ``command_id``
  只执行一次，重复送达只回原状态，重复到达不会变成第二次执行。

回执分两段，正好回答"没收到还是没执行"：

1. 接收回执 ACK/NACK——回答指令到没到站；ACK 超时未到 → ``undelivered``（没收到）。
2. 执行回执 EXECUTED/EXEC_REJECTED——回答站有没有动作；ACK 已到但执行回执
   超时 → ``not_executed``（收到了没执行）。两种失败在对账里分栏，补发策略
   也不同：前者重发同一版本，后者不改版本但走改摊。

改摊产生新版本（``revision + 1``、新 ``command_id``、新的绝对目标功率），
旧版本命令置 ``superseded``。对现场一律下发绝对值而不是增减量，重发与改摊
都不会叠加成两次动作。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 指令生命周期。
ORDER_RECEIVED = "received"
ORDER_ALLOCATED = "allocated"
ORDER_SETTLED = "settled"

# 站点命令状态机。
CMD_PENDING = "pending"
CMD_SENT = "sent"
CMD_ACKED = "acked"
CMD_EXECUTED = "executed"
CMD_NACK = "nack"
CMD_EXEC_REJECTED = "exec_rejected"
CMD_UNDELIVERED = "undelivered"
CMD_NOT_EXECUTED = "not_executed"
CMD_SUPERSEDED = "superseded"

# 执行未闭合、可以补发或改摊的状态。
OPEN_STATES = frozenset({CMD_PENDING, CMD_SENT, CMD_ACKED})
# 已坐实失败、份额变成缺口的状态。
GAP_STATES = frozenset({CMD_NACK, CMD_UNDELIVERED, CMD_EXEC_REJECTED, CMD_NOT_EXECUTED})
DONE_STATES = frozenset({CMD_EXECUTED}) | GAP_STATES | {CMD_SUPERSEDED}


def make_command_id(dispatch_id: str, station_id: str, revision: int) -> str:
    return f"{dispatch_id}:{station_id}:r{revision}"


@dataclass
class StationCommand:
    """平台对单个站下发的一条（某一版本的）绝对功率指令。"""

    command_id: str
    dispatch_id: str
    station_id: str
    revision: int
    kw: float
    state: str = CMD_PENDING
    attempts: int = 0
    last_attempt_ts: float | None = None
    next_due_ts: float | None = None
    acked_ts: float | None = None
    executed_ts: float | None = None
    actual_kw: float | None = None
    reason: str | None = None

    def as_dict(self) -> dict:
        return {
            "command_id": self.command_id,
            "dispatch_id": self.dispatch_id,
            "station_id": self.station_id,
            "revision": self.revision,
            "kw": round(self.kw, 3),
            "state": self.state,
            "attempts": self.attempts,
            "last_attempt_ts": self.last_attempt_ts,
            "next_due_ts": self.next_due_ts,
            "acked_ts": self.acked_ts,
            "executed_ts": self.executed_ts,
            "actual_kw": None if self.actual_kw is None else round(self.actual_kw, 3),
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StationCommand":
        return cls(
            command_id=str(data["command_id"]),
            dispatch_id=str(data["dispatch_id"]),
            station_id=str(data["station_id"]),
            revision=int(data["revision"]),
            kw=float(data["kw"]),
            state=str(data.get("state", CMD_PENDING)),
            attempts=int(data.get("attempts", 0)),
            last_attempt_ts=data.get("last_attempt_ts"),
            next_due_ts=data.get("next_due_ts"),
            acked_ts=data.get("acked_ts"),
            executed_ts=data.get("executed_ts"),
            actual_kw=None if data.get("actual_kw") is None else float(data["actual_kw"]),
            reason=data.get("reason"),
        )

    def is_open(self) -> bool:
        return self.state in OPEN_STATES

    def is_gap(self) -> bool:
        return self.state in GAP_STATES


@dataclass
class DispatchOrder:
    """一条调度中心调峰目标在平台内的全生命周期。"""

    dispatch_id: str
    target_kw: float
    strategy: str
    created_ts: float
    issued_ts: float | None = None
    state: str = ORDER_RECEIVED
    duplicate_hits: int = 0
    revision: int = 0
    plan: dict | None = None
    commands: dict[str, StationCommand] = field(default_factory=dict)
    gap_events: list[dict] = field(default_factory=list)
    reallocations: list[dict] = field(default_factory=list)
    declared_shortfall_kw: float = 0.0
    reconciliation: dict | None = None

    def as_dict(self) -> dict:
        return {
            "dispatch_id": self.dispatch_id,
            "target_kw": round(self.target_kw, 3),
            "strategy": self.strategy,
            "created_ts": self.created_ts,
            "issued_ts": self.issued_ts,
            "state": self.state,
            "duplicate_hits": self.duplicate_hits,
            "revision": self.revision,
            "plan": self.plan,
            "commands": {key: value.as_dict() for key, value in sorted(self.commands.items())},
            "gap_events": list(self.gap_events),
            "reallocations": list(self.reallocations),
            "declared_shortfall_kw": round(self.declared_shortfall_kw, 6),
            "reconciliation": self.reconciliation,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DispatchOrder":
        return cls(
            dispatch_id=str(data["dispatch_id"]),
            target_kw=float(data["target_kw"]),
            strategy=str(data["strategy"]),
            created_ts=float(data["created_ts"]),
            issued_ts=data.get("issued_ts"),
            state=str(data.get("state", ORDER_RECEIVED)),
            duplicate_hits=int(data.get("duplicate_hits", 0)),
            revision=int(data.get("revision", 0)),
            plan=data.get("plan"),
            commands={
                key: StationCommand.from_dict(value)
                for key, value in (data.get("commands") or {}).items()
            },
            gap_events=list(data.get("gap_events") or []),
            reallocations=list(data.get("reallocations") or []),
            declared_shortfall_kw=float(data.get("declared_shortfall_kw", 0.0)),
            reconciliation=data.get("reconciliation"),
        )

    def active_commands(self) -> dict[str, StationCommand]:
        """返回当前版本（未作废）的站点命令。"""

        return {key: value for key, value in self.commands.items() if value.revision == self.revision}

    def open_commands(self) -> list[StationCommand]:
        """返回当前版本仍在途（未闭合）的命令。"""

        return [cmd for cmd in self.active_commands().values() if cmd.is_open()]

    def executed_kw(self) -> float:
        return round(
            sum(cmd.actual_kw or 0.0 for cmd in self.active_commands().values() if cmd.state == CMD_EXECUTED),
            6,
        )

    def gap_kw(self) -> float:
        return round(sum(event["kw"] for event in self.gap_events if event["revision"] == self.revision), 6)
