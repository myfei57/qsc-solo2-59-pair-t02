"""电站群台账与站端指令执行模拟。

调度域只需要站端的四类事实：在线状态、装机容量、当前剩余响应能力，以及
站端是否已经收到/执行某条指令。这里用内存态站端模拟真实通信网关：发送只会
把指令写入“已送达”集合；执行是另一个显式动作，这样测试才能稳定复现“已收到
但未执行”的中断位置。
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import InterlockError


@dataclass(frozen=True)
class StationInfo:
    """单站的当前响应画像。"""

    station_id: str
    online: bool
    installed_kw: float
    remaining_kw: float

    def as_dict(self) -> dict:
        return {
            "station_id": self.station_id,
            "online": self.online,
            "installed_kw": round(self.installed_kw, 3),
            "remaining_kw": round(self.remaining_kw, 3),
        }


class StationFleet:
    """电站群台账；同时保存站端已收到、已执行的指令幂等集合。"""

    def __init__(self) -> None:
        self._stations: dict[str, StationInfo] = {}
        self._delivered: set[str] = set()
        self._executed: dict[str, float] = {}
        self._delivery_failures: dict[str, int] = {}

    def register(self, station_id: str, installed_kw: float, remaining_kw: float, online: bool = True) -> StationInfo:
        """登记或刷新一个站点。"""

        station = self._build(station_id, installed_kw, remaining_kw, online)
        self._stations[station_id] = station
        return station

    def set_online(self, station_id: str, online: bool) -> StationInfo:
        """切换站点在线状态，容量画像保持不变。"""

        current = self._require(station_id)
        updated = StationInfo(current.station_id, online, current.installed_kw, current.remaining_kw)
        self._stations[station_id] = updated
        return updated

    def update_remaining(self, station_id: str, remaining_kw: float) -> StationInfo:
        """更新站点当下剩余响应能力。"""

        current = self._require(station_id)
        station = self._build(station_id, current.installed_kw, remaining_kw, current.online)
        self._stations[station_id] = station
        return station

    def fail_next_deliveries(self, station_id: str, count: int = 1) -> int:
        """安排该站接下来的若干次发送在链路上失败（站点本身仍可在线）。"""

        self._require(station_id)
        self._delivery_failures[station_id] = self._delivery_failures.get(station_id, 0) + max(0, int(count))
        return self._delivery_failures[station_id]

    def get(self, station_id: str) -> StationInfo:
        return self._require(station_id)

    def stations(self, *, online_only: bool = False) -> list[StationInfo]:
        values = list(self._stations.values())
        if online_only:
            values = [station for station in values if station.online]
        return sorted(values, key=lambda item: item.station_id)

    def deliver(self, command_id: str) -> dict:
        """向站端发送指令；在线且首次发送才新增一条送达回执。"""

        if not command_id:
            raise InterlockError("站端指令标识不能为空", component="dispatch")
        station_id = self._station_id(command_id)
        station = self._require(station_id)
        if not station.online:
            return {"command_id": command_id, "delivered": False, "duplicate": command_id in self._delivered}
        duplicate = command_id in self._delivered
        failures = self._delivery_failures.get(station_id, 0)
        if failures > 0:
            self._delivery_failures[station_id] = failures - 1
            return {"command_id": command_id, "delivered": False, "duplicate": duplicate, "reason": "transport_failure"}
        self._delivered.add(command_id)
        return {"command_id": command_id, "delivered": True, "duplicate": duplicate, "reason": ""}

    def execute(self, command_id: str, power_kw: float) -> dict:
        """站端执行一条已送达指令；同一指令重复执行只返回首次结果。"""

        self._require(self._station_id(command_id))
        if command_id not in self._delivered:
            return {
                "command_id": command_id,
                "executed": False,
                "duplicate": False,
                "power_kw": None,
                "reason": "not_delivered",
            }
        if command_id in self._executed:
            return {
                "command_id": command_id,
                "executed": True,
                "duplicate": True,
                "power_kw": self._round(self._executed[command_id]),
            }
        self._executed[command_id] = float(power_kw)
        return {"command_id": command_id, "executed": True, "duplicate": False, "power_kw": self._round(power_kw)}

    def station_status(self, command_id: str) -> dict:
        """查询站端指令状态，显式区分未送达、已送达未执行、已执行。"""

        delivered = command_id in self._delivered
        executed = command_id in self._executed
        if executed:
            state = "executed"
        elif delivered:
            state = "delivered_pending_execution"
        else:
            state = "not_delivered"
        return {
            "command_id": command_id,
            "state": state,
            "delivered": delivered,
            "executed": executed,
            "power_kw": self._round(self._executed[command_id]) if executed else None,
        }

    def snapshot(self) -> dict:
        return {
            "stations": [station.as_dict() for station in self.stations()],
            "delivered": sorted(self._delivered),
            "executed": {key: self._round(value) for key, value in sorted(self._executed.items())},
            "delivery_failures": dict(sorted(self._delivery_failures.items())),
        }

    def restore(self, snapshot: dict) -> None:
        """从持久化状态恢复平台和站端的幂等窗口。"""

        self._stations = {}
        for raw in snapshot.get("stations", []):
            self.register(
                str(raw["station_id"]),
                float(raw["installed_kw"]),
                float(raw["remaining_kw"]),
                bool(raw["online"]),
            )
        self._delivered = {str(item) for item in snapshot.get("delivered", [])}
        self._executed = {str(key): float(value) for key, value in dict(snapshot.get("executed", {})).items()}
        self._delivery_failures = {str(key): int(value) for key, value in dict(snapshot.get("delivery_failures", {})).items()}

    def _require(self, station_id: str) -> StationInfo:
        station = self._stations.get(station_id)
        if station is None:
            raise InterlockError(f"未登记的站点: {station_id}", component="dispatch")
        return station

    @staticmethod
    def _station_id(command_id: str) -> str:
        return command_id.rsplit(":c", 1)[0].rsplit(":", 1)[-1]

    @staticmethod
    def _build(station_id: str, installed_kw: float, remaining_kw: float, online: bool) -> StationInfo:
        if not station_id:
            raise InterlockError("站点标识不能为空", component="dispatch")
        installed = float(installed_kw)
        remaining = float(remaining_kw)
        if installed < 0.0 or remaining < 0.0:
            raise InterlockError("容量不能为负", component="dispatch")
        if remaining > installed:
            raise InterlockError(f"站点 {station_id} 剩余响应能力不能超过装机容量", component="dispatch")
        return StationInfo(station_id, bool(online), installed, remaining)

    @staticmethod
    def _round(value: float) -> float:
        return round(float(value), 3)
