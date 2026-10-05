"""站点侧执行代理（链路对端）。

真实部署里这是各站本地控制系统；平台通过链路下发指令、接收两段回执。
验证环境用本类模拟对端，精确控制三类现场状况：

* 链路断开（``set_link(False)``）：平台任何投递都失败，命令停在
  ``sent``，ACK 超时后判 ``undelivered``——"没收到"；
* 收到但执行不了（``fail_execute``）：ACK 正常回执，执行回执是拒绝，
  命令进 ``exec_rejected``——"收到了，没执行"；
* 中断后续上：平台重连后调用 :meth:`resync` 按 ``command_id`` 查询，
  对端如实返回最后状态（没收到／已收未执行／已执行），状态由对端事实
  决定而不是由平台猜。

对端对同一 ``command_id`` 永远只执行一次：重复送达返回已存回执。
"""

from __future__ import annotations

from dataclasses import dataclass

# 对端在 resync 时报告的客观状态。
PEER_UNKNOWN = "unknown"
PEER_RECEIVED = "received"
PEER_EXECUTED = "executed"
PEER_REJECTED = "rejected"


@dataclass
class _PeerRecord:
    command_id: str
    kw: float
    received_ts: float
    executed_ts: float | None = None
    actual_kw: float | None = None
    rejected: bool = False
    reject_reason: str | None = None


class StationAgent:
    """一个站点的本地执行代理。"""

    def __init__(self, station_id: str, name: str = "", auto_execute: bool = True) -> None:
        self.station_id = station_id
        self.name = name or station_id
        self.online = True
        self.auto_execute = auto_execute
        self._records: dict[str, _PeerRecord] = {}
        self._fail_next_execute = False
        self._fail_reason = "pcs_trip"

    def set_link(self, online: bool) -> None:
        """模拟链路通断。"""

        self.online = bool(online)

    def fail_execute(self, reason: str = "pcs_trip") -> None:
        """让下一条**新**指令的执行被对端拒绝。"""

        self._fail_next_execute = True
        self._fail_reason = reason

    def deliver(self, command_id: str, kw: float, now: float) -> dict:
        """平台向站点投递一条指令。

        返回对端接收结果：链路断时 ``delivered=False``（没有任何落站记录）；
        命令号重复时幂等返回既有记录，不重复执行。
        """

        if not self.online:
            return {"delivered": False, "duplicate": False, "command_id": command_id}
        existing = self._records.get(command_id)
        if existing is not None:
            return {
                "delivered": True,
                "duplicate": True,
                "command_id": command_id,
                "state": self._state_of(existing),
            }
        record = _PeerRecord(command_id=command_id, kw=kw, received_ts=now)
        self._records[command_id] = record
        if self.auto_execute:
            self._run(record)
        return {"delivered": True, "duplicate": False, "command_id": command_id, "state": self._state_of(record)}

    def execute(self, command_id: str, now: float) -> dict:
        """对站端已收到的指令执行动作（非自动执行场景或重连后续执行）。"""

        record = self._records.get(command_id)
        if record is None or not self.online:
            return {"ok": False, "state": PEER_UNKNOWN, "command_id": command_id}
        if record.executed_ts is not None or record.rejected:
            return {"ok": not record.rejected, "state": self._state_of(record), "command_id": command_id, "duplicate": True}
        self._run(record, now=now)
        return {"ok": not record.rejected, "state": self._state_of(record), "command_id": command_id, "actual_kw": record.actual_kw}

    def resync(self, command_id: str) -> dict:
        """重连后平台按命令号查询对端真实状态（断线续传的对端侧）。"""

        record = self._records.get(command_id)
        if record is None:
            return {"command_id": command_id, "found": False, "state": PEER_UNKNOWN}
        return {
            "command_id": command_id,
            "found": True,
            "state": self._state_of(record),
            "kw": round(record.kw, 3),
            "actual_kw": None if record.actual_kw is None else round(record.actual_kw, 3),
            "received_ts": record.received_ts,
            "executed_ts": record.executed_ts,
            "reason": record.reject_reason,
        }

    def _run(self, record: _PeerRecord, now: float | None = None) -> None:
        if self._fail_next_execute:
            self._fail_next_execute = False
            record.rejected = True
            record.reject_reason = self._fail_reason
            return
        record.executed_ts = record.received_ts if now is None else now
        record.actual_kw = record.kw

    @staticmethod
    def _state_of(record: _PeerRecord) -> str:
        if record.rejected:
            return PEER_REJECTED
        if record.executed_ts is not None:
            return PEER_EXECUTED
        return PEER_RECEIVED
