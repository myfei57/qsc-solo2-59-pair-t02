"""站群名册与"当下剩余响应能力"。

每个站登记三项静态属性：编号、装机容量（kW）、单次最大可响应功率（kW）。
动态状态有两类：链路在线／离线，以及带时间戳的剩余响应能力上报。能力
上报超过 TTL 即视为过期——按能力分摊时过期站与离线站一样不参与分母，
避免调度拿几分钟前的老状态做当下决策。
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import InterlockError


@dataclass
class Station:
    """电站群中的一个成员站。"""

    station_id: str
    name: str
    installed_kw: float
    headroom_kw: float
    online: bool = False
    capability_kw: float | None = None
    capability_ts: float | None = None

    def fresh_capability(self, now: float, ttl_s: float) -> float | None:
        """返回未过期的剩余响应能力，过期或缺失返回 ``None``。"""

        if not self.online or self.capability_kw is None or self.capability_ts is None:
            return None
        if now - self.capability_ts > ttl_s:
            return None
        return max(0.0, float(self.capability_kw))

    def as_dict(self) -> dict:
        return {
            "station_id": self.station_id,
            "name": self.name,
            "installed_kw": round(self.installed_kw, 3),
            "headroom_kw": round(self.headroom_kw, 3),
            "online": self.online,
            "capability_kw": None if self.capability_kw is None else round(self.capability_kw, 3),
            "capability_ts": self.capability_ts,
        }


class Fleet:
    """电站群成员名册，维护在线状态与最新能力上报。"""

    def __init__(self) -> None:
        self._stations: dict[str, Station] = {}

    def register(self, station_id: str, name: str, installed_kw: float, headroom_kw: float) -> Station:
        """登记一个站；重复登记按冲突拒绝，防止电话口径下"同名两站"。"""

        if not station_id:
            raise ValueError("站点编号不能为空")
        if station_id in self._stations:
            raise InterlockError(f"站点已登记: {station_id}", component="dispatch")
        if installed_kw <= 0 or headroom_kw <= 0:
            raise ValueError("装机容量与最大响应功率必须为正")
        if headroom_kw > installed_kw * 1.000001:
            raise ValueError("最大响应功率不能超过装机容量")
        station = Station(station_id, name, float(installed_kw), float(headroom_kw))
        self._stations[station_id] = station
        return station

    def set_online(self, station_id: str, online: bool) -> Station:
        station = self.get(station_id)
        station.online = bool(online)
        if not online:
            station.capability_kw = None
            station.capability_ts = None
        return station

    def report_capability(self, station_id: str, capability_kw: float, now: float) -> Station:
        """登记一次剩余响应能力上报；离线站上报直接拒绝，避免脏数据进分母。"""

        station = self.get(station_id)
        if not station.online:
            raise InterlockError(f"站点离线，拒收能力上报: {station_id}", component="dispatch")
        if capability_kw < 0:
            raise ValueError("剩余响应能力不能为负")
        station.capability_kw = min(float(capability_kw), station.headroom_kw)
        station.capability_ts = float(now)
        return station

    def get(self, station_id: str) -> Station:
        if station_id not in self._stations:
            raise KeyError(f"未知站点: {station_id}")
        return self._stations[station_id]

    def all(self) -> list[Station]:
        return [self._stations[key] for key in sorted(self._stations)]

    def capabilities(self, now: float, ttl_s: float) -> dict[str, float | None]:
        """返回各站当下有效能力（离线或过期为 ``None``）。"""

        return {station.station_id: station.fresh_capability(now, ttl_s) for station in self.all()}

    def snapshot(self) -> dict:
        return {"stations": [station.as_dict() for station in self.all()]}
