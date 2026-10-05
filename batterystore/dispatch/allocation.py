"""调峰目标分摊：装机容量法与当下能力法。

两种策略的差别在"分母是谁"：

* ``installed``（按装机容量）：分母是全部在册站的铭牌容量，每站分到的是
  合同口径的固定份额，与在不在线无关。离线站分到名义份额但交付为零，缺口
  原因记为 ``offline``，必须靠后面的改摊水瀑才能补上；若不改摊，缺口就
  一直挂着。适用条件：各站能力常年接近铭牌、在线率稳定，且调度按容量权
  责考核（谁的份额谁负责，临时不在线也要留痕）。
* ``capability``（按当下剩余响应能力）：分母只是当前在线且能力上报未过期
  的站；离线、失联、上报过期的站从一开始就不进分母，份额自动由在线站吸
  收，首轮分摊即可闭合（前提是在线能力总和够用）。适用条件：站点在线状
  态波动大、调峰窗口短、首要目标是"把缺口当场补齐"。

两种策略在全站在线且能力等于铭牌时结果一致；**部分离线且不改摊时差别最
大**：装机法留下与离线站名义份额等量的缺口，能力法零缺口。

分摊受每站单次上限钳位（``headroom_kw`` 或当下能力），被钳出的余量在
其他站间继续按权重分配（水瀑），最后按 ``share_step_kw`` 用最大余数法
取整，保证各站份额之和与总目标只差取整残差，残差也明确记账，不悄悄丢。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .settings import STRATEGY_CAPABILITY, STRATEGY_INSTALLED

CAUSE_OFFLINE = "offline"
CAUSE_CAPPED = "capped"
CAUSE_NACK = "nack"
CAUSE_TIMEOUT = "timeout"


@dataclass(frozen=True)
class Share:
    """一个站在一次分摊中的份额。"""

    station_id: str
    kw: float
    basis_kw: float
    capped: bool

    def as_dict(self) -> dict:
        return {"station_id": self.station_id, "kw": round(self.kw, 3), "basis_kw": round(self.basis_kw, 3), "capped": self.capped}


@dataclass(frozen=True)
class AllocationPlan:
    """一次目标分解的完整结果（含名义份额与未闭合缺口）。"""

    strategy: str
    target_kw: float
    shares: list[Share]
    nominal: dict[str, float]
    excluded: list[str]
    shortfall_kw: float
    gap_causes: dict[str, str]
    residual_step_kw: float = field(default=0.0)

    def share(self, station_id: str) -> float:
        for item in self.shares:
            if item.station_id == station_id:
                return item.kw
        return 0.0

    def total_kw(self) -> float:
        return round(sum(item.kw for item in self.shares), 6)

    def as_dict(self) -> dict:
        return {
            "strategy": self.strategy,
            "target_kw": round(self.target_kw, 3),
            "shares": [item.as_dict() for item in self.shares],
            "nominal": {key: round(value, 3) for key, value in sorted(self.nominal.items())},
            "excluded": list(self.excluded),
            "allocated_kw": self.total_kw(),
            "shortfall_kw": round(self.shortfall_kw, 6),
            "gap_causes": dict(self.gap_causes),
            "residual_step_kw": round(self.residual_step_kw, 3),
        }


def _apportion(weights: dict[str, float], caps: dict[str, float], target_kw: float, step_kw: float) -> tuple[dict[str, float], float]:
    """带单站上限的按比例分配（整数单位 + 水填 + 最大余数），返回份额与残差。

    先反复按剩余权重把目标分给未触顶的站，触顶者固定在其上限退出，余量回流
    给其余站；最后一轮不再触顶时用最大余数法取整。这样只要各站上限之和够
    装目标，就一定能分满（残差为 0）；装不下时残差等于装不下的整数单位。
    """

    total_units = max(0, round(target_kw / step_kw))
    # 上限换算用四舍五入而不是向下取整，规避 100.0/0.001=99999.9999 之类浮点误差。
    unit_caps = {key: max(0, round(caps.get(key, 0.0) / step_kw)) for key in weights}
    units = {key: 0 for key in weights}
    remaining = total_units
    active = {key for key, weight in weights.items() if weight > 0 and unit_caps[key] > 0}
    while remaining > 0 and active:
        weight_sum = sum(weights[key] for key in active)
        # 每站本轮"理想整数份额"，但不超过其剩余可接单位。
        ideal = {key: remaining * weights[key] / weight_sum for key in active}
        free_space = {key: unit_caps[key] - units[key] for key in active}
        saturated = {key for key in active if ideal[key] >= free_space[key]}
        if saturated:
            # 触顶站先钉死在上限，把剩余目标回流给未触顶站继续分。
            for key in saturated:
                units[key] = unit_caps[key]
                remaining -= free_space[key]
            active -= saturated
            continue
        floors = {key: int(ideal[key] // 1) for key in active}
        for key, value in floors.items():
            units[key] += value
        leftover = remaining - sum(floors.values())
        ranked = sorted(active, key=lambda key: (ideal[key] - floors[key], key), reverse=True)
        for key in ranked[:leftover]:
            units[key] += 1
        remaining = 0
    shares = {key: round(value * step_kw, 6) for key, value in units.items()}
    allocated_units = sum(units.values())
    residual = round((total_units - allocated_units) * step_kw, 6)
    return shares, residual


def _proportional_nominal(weights: dict[str, float], target_kw: float, step_kw: float) -> dict[str, float]:
    """无上限名义份额（仅用于装机法记账，不做钳位）。"""

    total = sum(weights.values())
    if total <= 0:
        return {key: 0.0 for key in weights}
    raw = {key: target_kw * weight / total for key, weight in weights.items()}
    units = {key: int(value // step_kw) for key, value in raw.items()}
    leftover = max(0, round(target_kw / step_kw)) - sum(units.values())
    ranked = sorted(weights, key=lambda key: (raw[key] - units[key] * step_kw, key), reverse=True)
    for key in ranked[:leftover]:
        units[key] += 1
    return {key: round(value * step_kw, 6) for key, value in units.items()}


def allocate(
    strategy: str,
    target_kw: float,
    installed: dict[str, float],
    headroom: dict[str, float],
    capabilities: dict[str, float | None],
    step_kw: float,
) -> AllocationPlan:
    """按选定策略把调峰目标分解到各站。

    ``capabilities`` 给出各站当下有效能力（离线或过期为 ``None``）。
    """

    if target_kw < 0:
        raise ValueError("调峰目标不能为负")
    if strategy not in (STRATEGY_INSTALLED, STRATEGY_CAPABILITY):
        raise ValueError(f"未知分摊策略: {strategy}")

    all_ids = sorted(installed)
    nominal: dict[str, float] = {}
    gap_causes: dict[str, str] = {}
    weights: dict[str, float] = {}
    caps: dict[str, float] = {}
    shares: dict[str, float] = {}

    if strategy == STRATEGY_INSTALLED:
        # 名义份额按全群装机容量固定摊：每站交付量先等于自己的名义份额，
        # 离线站交付为零、份额原样挂成 offline 缺口；在线站受当下能力钳位。
        # 被钳出的余量本轮**不**自动并给别人——是否改摊是明确的后续动作，
        # 这正是"部分离线且不改摊时两法差别最大"的来源。
        nominal = _proportional_nominal(installed, target_kw, step_kw)
        for key in all_ids:
            capability = capabilities.get(key)
            if capability is None:
                if nominal.get(key, 0.0) > 0:
                    gap_causes[key] = CAUSE_OFFLINE
                continue
            weights[key] = installed[key]
            caps[key] = min(headroom[key], float(capability))
            shares[key] = min(nominal.get(key, 0.0), caps[key])
            if shares[key] + 1e-9 < nominal.get(key, 0.0):
                gap_causes[key] = CAUSE_CAPPED
        residual = 0.0
    else:
        # 只有在线且能力新鲜的站进分母，权重就是当下剩余响应能力本身，
        # 离线份额从一开始就被在线站按能力比例吸收。
        weights = {
            key: float(capabilities[key])
            for key in all_ids
            if capabilities.get(key) is not None and float(capabilities[key]) > 0
        }
        caps = {key: float(capabilities[key]) for key in weights}
        shares, residual = _apportion(weights, caps, target_kw, step_kw)
        nominal = dict(shares)

    excluded = [key for key in all_ids if key not in weights]
    share_objects = [
        Share(
            station_id=key,
            kw=shares.get(key, 0.0),
            basis_kw=weights.get(key, 0.0),
            capped=key in gap_causes and gap_causes[key] == CAUSE_CAPPED,
        )
        for key in all_ids
        if shares.get(key, 0.0) > 0
    ]
    allocated = sum(item.kw for item in share_objects)
    shortfall = round(max(0.0, target_kw - allocated), 6)
    # 能力法下被单站上限钳出的余量若没人能接，记成群级 capped 缺口。
    if shortfall > residual and not gap_causes and strategy == STRATEGY_CAPABILITY:
        gap_causes["__fleet__"] = CAUSE_CAPPED
    shortfall = round(max(0.0, shortfall - residual), 6)
    return AllocationPlan(
        strategy=strategy,
        target_kw=float(target_kw),
        shares=share_objects,
        nominal=nominal,
        excluded=excluded,
        shortfall_kw=shortfall,
        gap_causes=gap_causes,
        residual_step_kw=residual,
    )


def reallocate(
    plan: AllocationPlan,
    gaps: dict[str, float],
    spare_caps: dict[str, float],
    step_kw: float,
) -> tuple[dict[str, float], float]:
    """把已坐实的缺口（离线／拒执／超时）改摊给还有余量的站。

    ``spare_caps`` 直接给出每个承接站"还能再增加多少 kW"（已扣除其当前
    份额与当下能力上限）。按余量比例分配缺口；接不下的部分作为返回值里
    的残余缺口，由调度端对账时显式确认，不做隐性丢弃。
    """

    gap_total = round(sum(max(0.0, value) for value in gaps.values()), 6)
    spare = {key: max(0.0, value) for key, value in spare_caps.items() if key not in gaps and value > 0}
    deltas, residual = _apportion(spare, spare, gap_total, step_kw)
    return {key: value for key, value in deltas.items() if value > 0}, residual
