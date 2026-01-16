from __future__ import annotations

import pandas as pd
import nflreadpy as nfl


def _to_pandas(df):
    """
    nflreadpy commonly returns Polars DataFrames.
    Convert to pandas at the extraction boundary.
    """
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    return df


def fetch_schedule(season: int) -> pd.DataFrame:
    df = nfl.load_schedules([season])
    return _to_pandas(df)


def fetch_player_stats_week(season: int, week: int) -> pd.DataFrame:
    df = nfl.load_player_stats([season])
    df = _to_pandas(df)

    if "week" not in df.columns:
        raise ValueError("Expected a 'week' column in player stats dataset.")

    return df[df["week"] == week].copy()
