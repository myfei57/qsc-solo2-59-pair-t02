"""调峰目标分摊：装机容量法 vs 当下剩余响应能力法。

两种方法在全站在线且能力等于铭牌时结果一致；部分离线且不改摊时差别最大：
装机法留下与离线站名义份额等量的显式缺口，能力法零缺口。单站能力上限把
份额钳住时，余量由水瀑改摊，接不下的残量明确记账。
"""

from __future__ import annotations

import pytest

from batterystore.dispatch.allocation import CAUSE_OFFLINE, allocate, reallocate
from batterystore.dispatch.allocation import AllocationPlan, Share
from batterystore.dispatch.settings import STRATEGY_CAPABILITY, STRATEGY_INSTALLED

STEP = 0.001


def _fleet(installed, caps):
    return list(installed), dict(installed), dict(caps)


def test_strategies_agree_when_all_online_at_nameplate():
    ids, installed, headroom = _fleet({"a": 1000.0, "b": 1000.0, "c": 2000.0},
                                      {"a": 1000.0, "b": 1000.0, "c": 2000.0})
    capabilities = {key: headroom[key] for key in ids}
    target = 800.0
    cap = allocate(STRATEGY_CAPABILITY, target, installed, headroom, capabilities, STEP)
    ins = allocate(STRATEGY_INSTALLED, target, installed, headroom, capabilities, STEP)
    assert cap.total_kw() == pytest.approx(800.0, abs=STEP)
    assert ins.total_kw() == pytest.approx(800.0, abs=STEP)
    assert cap.share("a") == pytest.approx(ins.share("a"), abs=0.01)
    assert cap.shortfall_kw == 0.0
    assert ins.shortfall_kw == 0.0


def test_partial_offline_strategies_diverge():
    # c 占一半装机，离线。
    installed = {"a": 1000.0, "b": 1000.0, "c": 2000.0}
    headroom = dict(installed)
    capabilities = {"a": 1000.0, "b": 1000.0, "c": None}

    cap = allocate(STRATEGY_CAPABILITY, 1000.0, installed, headroom, capabilities, STEP)
    ins = allocate(STRATEGY_INSTALLED, 1000.0, installed, headroom, capabilities, STEP)

    # 能力法：离线站不进分母，a/b 直接吸收全部目标，无缺口。
    assert cap.excluded == ["c"]
    assert cap.share("a") + cap.share("b") == pytest.approx(1000.0, abs=STEP)
    assert cap.shortfall_kw == 0.0

    # 装机法：a/b 只交付各自名义份额（各 250），c 的 500 挂成 offline 缺口。
    assert ins.share("a") == pytest.approx(250.0, abs=STEP)
    assert ins.share("b") == pytest.approx(250.0, abs=STEP)
    assert ins.share("c") == 0.0
    assert ins.nominal["c"] == pytest.approx(500.0, abs=STEP)
    assert ins.gap_causes["c"] == CAUSE_OFFLINE
    assert ins.shortfall_kw == pytest.approx(500.0, abs=STEP)


def test_capability_method_excludes_stale_reports(fleet_now):
    ledger, clock = fleet_now
    ledger.register_station("a", "a", 1000.0, 1000.0)
    ledger.register_station("b", "b", 1000.0, 1000.0)
    ledger.report_capability("a", 1000.0)
    ledger.report_capability("b", 1000.0)
    # 超过 TTL 后只重报 a：b 的上报过期，视为当下无能力，不进分母。
    clock.advance(ledger.settings.capability_ttl_s + 1.0)
    ledger.report_capability("a", 1000.0)
    result = ledger.receive("STALE-1", 500.0, strategy=STRATEGY_CAPABILITY)
    stations = {share["station_id"] for share in result["plan"]["shares"]}
    assert stations == {"a"}
    assert "b" in result["plan"]["excluded"]


def test_reallocate_fills_gap_from_spare_and_reports_residual():
    # a、b 已各摊 400，c 离线缺口 200；a/b 各还有 100 新增余量。
    plan = AllocationPlan(
        strategy=STRATEGY_INSTALLED,
        target_kw=1000.0,
        shares=[Share("a", 400.0, 1000.0, False), Share("b", 400.0, 1000.0, False)],
        nominal={"a": 400.0, "b": 400.0, "c": 200.0},
        excluded=["c"],
        shortfall_kw=200.0,
        gap_causes={"c": CAUSE_OFFLINE},
    )
    deltas, residual = reallocate(plan, {"c": 200.0}, {"a": 100.0, "b": 100.0}, STEP)
    assert sum(deltas.values()) == pytest.approx(200.0, abs=STEP)
    assert deltas["a"] == pytest.approx(100.0, abs=STEP)
    assert deltas["b"] == pytest.approx(100.0, abs=STEP)
    assert residual == 0.0

    # 新增余量不足：只剩共 120 可接，80 残量必须显式留下。
    deltas, residual = reallocate(plan, {"c": 200.0}, {"a": 60.0, "b": 60.0}, STEP)
    assert sum(deltas.values()) == pytest.approx(120.0, abs=STEP)
    assert residual == pytest.approx(80.0, abs=STEP)


def test_cap_clamp_does_not_exceed_headroom():
    # 能力法按"当下剩余能力"加权：a 仅剩 100，b 有 3000。
    installed = {"a": 1000.0, "b": 3000.0}
    headroom = {"a": 1000.0, "b": 3000.0}
    capabilities = {"a": 100.0, "b": 3000.0}
    plan = allocate(STRATEGY_CAPABILITY, 1000.0, installed, headroom, capabilities, STEP)
    # a 占能力权重 100/3100，约 32.26，未触顶；其余归 b。
    assert plan.share("a") == pytest.approx(1000.0 * 100.0 / 3100.0, abs=0.01)
    assert plan.share("b") == pytest.approx(1000.0 * 3000.0 / 3100.0, abs=0.01)
    assert plan.total_kw() == pytest.approx(1000.0, abs=STEP)


def test_cap_clamp_when_small_station_saturates():
    # 目标很大时小能力站先被钳到顶，余量由大站吸收，不超过任何一站上限。
    installed = {"a": 1000.0, "b": 3000.0}
    headroom = {"a": 100.0, "b": 3000.0}
    capabilities = {"a": 100.0, "b": 3000.0}
    plan = allocate(STRATEGY_CAPABILITY, 3100.0, installed, headroom, capabilities, STEP)
    assert plan.share("a") <= 100.0 + STEP
    assert plan.share("b") <= 3000.0 + STEP
    assert plan.total_kw() == pytest.approx(3100.0, abs=STEP)


def test_allocation_rejects_unknown_strategy_and_negative_target():
    with pytest.raises(ValueError):
        allocate("bogus", 10.0, {"a": 1.0}, {"a": 1.0}, {"a": 1.0}, STEP)
    with pytest.raises(ValueError):
        allocate(STRATEGY_CAPABILITY, -1.0, {"a": 1.0}, {"a": 1.0}, {"a": 1.0}, STEP)
