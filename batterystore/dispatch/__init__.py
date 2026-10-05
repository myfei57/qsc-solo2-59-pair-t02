"""集群调峰调度。

把调度中心下达的整片电站群调峰目标自动分解到站、下发、回收两段回执，
收不齐的自动补发，坐实的缺口自动改摊，最后与调度端对账。两种分摊策略
见 :mod:`batterystore.dispatch.allocation`，幂等与状态机见
:mod:`batterystore.dispatch.orders`，全过程编排见
:class:`batterystore.dispatch.ledger.DispatchLedger`。
"""

from __future__ import annotations

from .fleet import Fleet, Station
from .ledger import DispatchLedger
from .orders import DispatchOrder, StationCommand
from .settings import STRATEGY_CAPABILITY, STRATEGY_INSTALLED, DispatchSettings

__all__ = [
    "Fleet",
    "Station",
    "DispatchLedger",
    "DispatchOrder",
    "StationCommand",
    "DispatchSettings",
    "STRATEGY_CAPABILITY",
    "STRATEGY_INSTALLED",
]
