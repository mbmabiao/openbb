from __future__ import annotations

import importlib
import inspect
import pkgutil
from functools import lru_cache

from strategies.base import BaseStrategy


def _iter_strategy_module_names() -> list[str]:
    import strategies
    import strategies.examples

    modules: list[str] = []
    for package in (strategies, strategies.examples):
        for module_info in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
            if module_info.ispkg:
                continue
            if module_info.name.endswith(".base") or module_info.name.endswith(".registry"):
                continue
            modules.append(module_info.name)
    return modules


@lru_cache(maxsize=1)
def discover_strategies() -> dict[str, type[BaseStrategy]]:
    """
    Import strategy modules from src/strategies and src/strategies/examples.
    Return available strategy classes keyed by strategy name.
    """
    discovered: dict[str, type[BaseStrategy]] = {}
    for module_name in _iter_strategy_module_names():
        module = importlib.import_module(module_name)
        for _, value in inspect.getmembers(module, inspect.isclass):
            if value is BaseStrategy or not issubclass(value, BaseStrategy):
                continue
            strategy_name = getattr(value, "name", "")
            if strategy_name and strategy_name != "base":
                discovered[strategy_name] = value
    return dict(sorted(discovered.items(), key=lambda item: item[0]))


def list_strategies() -> list[dict]:
    """
    Return frontend-friendly strategy metadata.
    """
    metadata: list[dict] = []
    for name, strategy_cls in discover_strategies().items():
        metadata.append(
            {
                "name": name,
                "display_name": getattr(strategy_cls, "display_name", name),
                "description": getattr(strategy_cls, "description", ""),
                "required_timeframes": list(getattr(strategy_cls, "required_timeframes", ["1d"])),
                "preferred_primary_timeframe": getattr(strategy_cls, "preferred_primary_timeframe", None),
                "requires_extended_hours": bool(getattr(strategy_cls, "requires_extended_hours", False)),
                "supports_extended_hours": bool(getattr(strategy_cls, "supports_extended_hours", False)),
                "data_requirements": dict(getattr(strategy_cls, "data_requirements", {})),
                "default_config": dict(getattr(strategy_cls, "default_config", {})),
                "config_schema": dict(getattr(strategy_cls, "config_schema", {})),
            }
        )
    return metadata


def get_strategy(name: str, config: dict | None = None) -> BaseStrategy:
    """
    Instantiate a strategy by name.
    """
    strategies = discover_strategies()
    if name not in strategies:
        available = ", ".join(strategies) or "none"
        raise ValueError(f"Unknown strategy '{name}'. Available strategies: {available}")
    return strategies[name](config=config)
