"""集群调峰调度参数。

参数都与挂钟解耦（秒、千瓦），验证环境用可推进时钟驱动超时与补发。
``capability_ttl_s`` 决定"当下剩余响应能力"的新鲜度：超过 TTL 的上报
视为过期，该站不再参与按能力分摊。
"""

from __future__ import annotations

from dataclasses import dataclass

STRATEGY_INSTALLED = "installed"
STRATEGY_CAPABILITY = "capability"
STRATEGIES = (STRATEGY_INSTALLED, STRATEGY_CAPABILITY)


@dataclass(frozen=True)
class DispatchSettings:
    """一次调度周期的时序与重试参数。"""

    strategy: str
    capability_ttl_s: float
    ack_timeout_s: float
    execute_timeout_s: float
    retry_max: int
    retry_backoff_s: float
    reallocate_max: int
    share_step_kw: float

    @classmethod
    def default(cls) -> "DispatchSettings":
        return cls(
            strategy=STRATEGY_CAPABILITY,
            capability_ttl_s=60.0,
            ack_timeout_s=15.0,
            execute_timeout_s=120.0,
            retry_max=3,
            retry_backoff_s=5.0,
            reallocate_max=2,
            share_step_kw=0.001,
        )

    def validated(self) -> "DispatchSettings":
        if self.strategy not in STRATEGIES:
            raise ValueError(f"未知分摊策略: {self.strategy}")
        for field_name in ("capability_ttl_s", "ack_timeout_s", "execute_timeout_s", "retry_backoff_s"):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name} 必须为正")
        if self.retry_max < 0 or self.reallocate_max < 0:
            raise ValueError("重试与改摊次数不能为负")
        if self.share_step_kw <= 0:
            raise ValueError("分摊粒度必须为正")
        return self
