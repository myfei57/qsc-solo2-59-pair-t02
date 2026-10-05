"""测试夹具：所有用例都从干净存储和可推进时钟起步。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from batterystore.app import build_platform  # noqa: E402
from batterystore.clock import ManualClock  # noqa: E402
from batterystore.config import default_config  # noqa: E402
from batterystore.dispatch.ledger import DispatchLedger  # noqa: E402
from batterystore.dispatch.settings import DispatchSettings  # noqa: E402
from batterystore.eventbus import EventBus  # noqa: E402
from batterystore.store.jsonfile import JsonStore  # noqa: E402


@pytest.fixture()
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture()
def config():
    return default_config()


@pytest.fixture()
def platform(tmp_path, clock, config):
    return build_platform(config, clock=clock, storage_root=str(tmp_path / "state"), rack_count=2)


@pytest.fixture()
def dispatch_settings():
    return DispatchSettings(
        strategy="capability",
        capability_ttl_s=60.0,
        ack_timeout_s=10.0,
        execute_timeout_s=30.0,
        retry_max=3,
        retry_backoff_s=2.0,
        reallocate_max=2,
        share_step_kw=0.001,
    )


@pytest.fixture()
def fleet_now(tmp_path, clock, dispatch_settings):
    """一个挂在可推进时钟上的调度台账，便于测补发/超时/改摊。"""

    ledger = DispatchLedger(
        clock,
        EventBus(),
        JsonStore(str(tmp_path / "dispatch")),
        dispatch_settings,
    )
    return ledger, clock
