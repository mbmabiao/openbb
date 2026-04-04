from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class BoundaryTesterConfig:
    breakout_buffer_pct: float = 0.002
    probe_buffer_pct: float = 0.001
    lookahead_bars: int = 20
    success_move_pct: float = 0.03
    failure_reentry_bars: int = 5
    min_close_outside_zone: int = 2
    max_event_gap: int = 3
    use_atr_filter: bool = False
    atr_multiple_success: float = 1.5
    atr_window: int = 14
    retest_buffer_pct: float = 0.002
    failed_breakout_reentry_depth_frac: float = 0.25
    failed_breakout_min_consecutive_inside_bars: int = 2

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "BoundaryTesterConfig":
        if not payload:
            return cls()
        valid_fields = {field.name for field in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in payload.items() if k in valid_fields}
        return cls(**filtered)

    @classmethod
    def from_json_file(cls, path: str | Path | None) -> "BoundaryTesterConfig":
        if path is None:
            return cls()
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(payload)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
