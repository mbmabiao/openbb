from __future__ import annotations

from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from engines.replay_engine import prepare_replay_frame


class ReplayEngineTests(unittest.TestCase):
    def test_prepare_replay_frame_includes_selected_date_in_calculations(self) -> None:
        frame = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
                "close": [100.0, 101.0, 102.0],
            }
        )

        _, df_calc = prepare_replay_frame(
            df_plot=frame,
            df_calc=frame,
            replay_date_value=pd.Timestamp("2026-01-02"),
        )

        self.assertEqual(df_calc["date"].dt.strftime("%Y-%m-%d").tolist(), ["2026-01-01", "2026-01-02"])
        self.assertEqual(float(df_calc["close"].iloc[-1]), 101.0)


if __name__ == "__main__":
    unittest.main()
