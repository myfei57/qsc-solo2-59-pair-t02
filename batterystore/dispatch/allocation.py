"""调峰目标分摊。

实时控制默认按“当前剩余响应能力”分摊：在线站按还能承担多少调峰功率等比例
接单，站点离线或降额时不会把它的份额继续挂在它名下。按装机容量分摊保留给
日前计划、合同/考核口径，或者能在下发前确认全部站点剩余能力的场景；即使实现
上仍用剩余能力做安全钳位，容量权重也可能把低裕量站压到上限，与实时能力法的
站点责任不同。
"""

from __future__ import annotations

import math

from .fleet import StationInfo

ALLOCATION_METHODS = ("remaining", "installed")
ROUND_DIGITS = 3
SCALE = 10**ROUND_DIGITS


def allocate_target(
    stations: list[StationInfo],
    target_kw: float,
    method: str = "remaining",
    *,
    current_allocations: dict[str, float] | None = None,
) -> dict:
    """把目标功率分摊给站点，返回每站增量、缺口与本次权重。"""

    if target_kw < 0.0:
        raise ValueError("调峰目标不能为负")
    if method not in ALLOCATION_METHODS:
        raise ValueError(f"未知分摊方式: {method}")
    current = current_allocations or {}
    weights: dict[str, float] = {}
    headrooms: dict[str, float] = {}
    for station in stations:
        weight = station.remaining_kw if method == "remaining" else station.installed_kw
        headroom = max(0.0, round(station.remaining_kw - float(current.get(station.station_id, 0.0)), ROUND_DIGITS))
        if weight <= 0.0 or headroom <= 0.0:
            continue
        weights[station.station_id] = weight
        headrooms[station.station_id] = headroom
    continuous = _capped_proportional(weights, headrooms, min(float(target_kw), sum(headrooms.values())))
    allocations = _floor_and_distribute(continuous, headrooms)
    total_allocated = sum(allocations.values())
    shortfall = max(0.0, float(target_kw) - total_allocated)
    return {
        "method": method,
        "target_kw": round(float(target_kw), ROUND_DIGITS),
        "allocated_kw": round(total_allocated, ROUND_DIGITS),
        "shortfall_kw": round(shortfall, ROUND_DIGITS),
        "weights": {key: round(value, ROUND_DIGITS) for key, value in weights.items()},
        "allocations": {key: round(value, ROUND_DIGITS) for key, value in sorted(allocations.items()) if value > 0.0},
    }


def _floor_and_distribute(values: dict[str, float], headrooms: dict[str, float]) -> dict[str, float]:
    """向下取整到毫千瓦，再按小数余量补整，保证总和不超过目标。"""

    units = 0.0
    rounded: dict[str, float] = {}
    fractions: list[tuple[float, str]] = []
    for key, value in values.items():
        floored = math.floor(value * SCALE) / SCALE
        rounded[key] = floored
        fractions.append((value - floored, key))
        units += floored
    wanted_units = math.floor(sum(values.values()) * SCALE) / SCALE
    leftover_units = round((wanted_units - units) * SCALE)
    for _fraction, key in sorted(fractions, reverse=True):
        if leftover_units <= 0:
            break
        if rounded[key] + 1 / SCALE <= headrooms[key] + 1e-9:
            rounded[key] = round(rounded[key] + 1 / SCALE, ROUND_DIGITS)
            leftover_units -= 1
    return rounded


def _capped_proportional(weights: dict[str, float], headrooms: dict[str, float], wanted: float) -> dict[str, float]:
    """按权重连续分摊；触顶站点移除后，用其余站点继续按比例补齐。"""

    allocations = {key: 0.0 for key in weights}
    remaining = float(wanted)
    active = set(weights)
    while remaining > 1e-9 and active:
        total_weight = sum(weights[key] for key in active)
        capped: set[str] = set()
        additions: dict[str, float] = {}
        for key in active:
            take = min(headrooms[key] - allocations[key], remaining * weights[key] / total_weight)
            additions[key] = take
            if headrooms[key] - allocations[key] - take <= 1e-6:
                capped.add(key)
        for key, take in additions.items():
            allocations[key] += take
            remaining -= take
        if not capped:
            break
        active -= capped
    return allocations
