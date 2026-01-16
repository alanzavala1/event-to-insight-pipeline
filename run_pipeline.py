from __future__ import annotations

import argparse
from pathlib import Path
import time

import pandas as pd

from src.db import connect, replace_week_slice
from src.extract.nflverse import fetch_schedule, fetch_player_stats_week
from src.report import write_week_report


def normalize_games(schedule: pd.DataFrame, season: int, week: int) -> pd.DataFrame:
    df = schedule.copy()

    rename = {
        "gameday": "game_date",
        "game_date": "game_date",
        "home_team": "home_team",
        "away_team": "away_team",
        "home_score": "home_score",
        "away_score": "away_score",
        "game_id": "game_id",
        "week": "week",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "week" in df.columns:
        df = df[df["week"] == week].copy()

    df["season"] = season
    df["week"] = week

    if "game_id" not in df.columns or df["game_id"].isna().all():
        away = df["away_team"].astype(str) if "away_team" in df.columns else "AWAY"
        home = df["home_team"].astype(str) if "home_team" in df.columns else "HOME"
        df["game_id"] = df["season"].astype(str) + "_W" + df["week"].astype(str) + "_" + away + "_at_" + home

    keep = ["season", "week", "game_id", "game_date", "home_team", "away_team", "home_score", "away_score"]
    for c in keep:
        if c not in df.columns:
            df[c] = None

    df = df[keep].drop_duplicates()
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def normalize_player_stats(stats: pd.DataFrame, season: int, week: int) -> pd.DataFrame:
    df = stats.copy()

    df["season"] = season
    df["week"] = week

    # --- Choose ONE player_name (avoid duplicate player_name columns) ---
    if "player_display_name" in df.columns:
        df["player_name"] = df["player_display_name"]
        df = df.drop(columns=["player_display_name"])
    elif "player_name" in df.columns:
        # keep as-is
        pass
    else:
        df["player_name"] = None

    # Best-effort normalize other column names
    rename = {
        "position": "position",
        "recent_team": "team",
        "team": "team",
        "opponent_team": "opponent",
        "opponent": "opponent",
        "game_id": "game_id",
        "passing_yards": "passing_yards",
        "passing_tds": "passing_tds",
        "rushing_attempts": "rushing_attempts",
        "rushing_yards": "rushing_yards",
        "rushing_tds": "rushing_tds",
        "receptions": "receptions",
        "receiving_yards": "receiving_yards",
        "receiving_tds": "receiving_tds",
        "player_id": "player_id",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    required = [
        "season", "week", "game_id", "team", "opponent", "player_id", "player_name", "position",
        "passing_yards", "passing_tds",
        "rushing_attempts", "rushing_yards", "rushing_tds",
        "receptions", "receiving_yards", "receiving_tds",
    ]
    for c in required:
        if c not in df.columns:
            # numeric defaults
            if any(x in c for x in ["yards", "tds", "attempts", "receptions"]):
                df[c] = 0
            else:
                df[c] = None

    # Coerce numeric
    num_cols = [
        "passing_yards", "passing_tds",
        "rushing_attempts", "rushing_yards", "rushing_tds",
        "receptions", "receiving_yards", "receiving_tds",
    ]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # Keep receiving yards for breakdowns, but do NOT add into official total yards.
    df["player_total_yards_official"] = df["passing_yards"] + df["rushing_yards"]

    keep = required + ["player_total_yards_official"]
    df = df[keep].drop_duplicates()

    # Final safety: remove duplicate col names if any slipped in
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def build_team_week_metrics(player_stats: pd.DataFrame) -> pd.DataFrame:
    agg = player_stats.groupby(["season", "week", "team", "opponent"], as_index=False).agg(
        team_pass_yards=("passing_yards", "sum"),
        team_rush_yards=("rushing_yards", "sum"),
        team_receiving_yards=("receiving_yards", "sum"),
        team_pass_tds=("passing_tds", "sum"),
        team_rush_tds=("rushing_tds", "sum"),
        team_receiving_tds=("receiving_tds", "sum"),
    )

    # Official NFL definition: total yards = passing + rushing
    agg["team_total_yards"] = agg["team_pass_yards"] + agg["team_rush_yards"]

    agg = agg.loc[:, ~agg.columns.duplicated()]
    return agg

def build_team_week_insights(team_week: pd.DataFrame, player_stats: pd.DataFrame) -> pd.DataFrame:
    # --- base columns from team_week ---
    df = team_week[
        ["season", "week", "team", "opponent", "team_pass_yards", "team_rush_yards", "team_total_yards"]
    ].copy()

    # --- pass_share ---
    denom = (df["team_pass_yards"] + df["team_rush_yards"]).replace(0, pd.NA)
    df["pass_share"] = (df["team_pass_yards"] / denom).astype("float64")

    # --- top player (name + yards) per team-week ---
    p = player_stats[["season", "week", "team", "player_name", "player_total_yards_official"]].copy()

    top_rows = (
        p.sort_values(
            ["season", "week", "team", "player_total_yards_official"],
            ascending=[True, True, True, False],
        )
        .drop_duplicates(subset=["season", "week", "team"], keep="first")
        .rename(
            columns={
                "player_name": "top_player_name",
                "player_total_yards_official": "top_player_yards",
            }
        )[["season", "week", "team", "top_player_name", "top_player_yards"]]
    )

    df = df.merge(top_rows, on=["season", "week", "team"], how="left")

    denom2 = df["team_total_yards"].replace(0, pd.NA)
    df["top_player_share"] = (df["top_player_yards"] / denom2).astype("float64")

    df["high_dependency_flag"] = (df["top_player_share"] >= 0.45).fillna(False)

    keep = [
        "season",
        "week",
        "team",
        "opponent",
        "team_total_yards",
        "pass_share",
        "top_player_name",
        "top_player_yards",
        "top_player_share",
        "high_dependency_flag",
    ]
    return df[keep].drop_duplicates()

def parse_weeks_arg(week: int | None, weeks: str | None) -> list[int]:
    if week is not None:
        if not (1 <= week <= 18):
            raise SystemExit("Week must be 1–18 for v1.")
        return [week]

    # weeks like "1-6"
    try:
        start_s, end_s = weeks.split("-", 1)
        start = int(start_s.strip())
        end = int(end_s.strip())
    except Exception:
        raise SystemExit("--weeks must look like: 1-6")

    if start > end:
        raise SystemExit("--weeks start must be <= end")
    if not (1 <= start <= 18 and 1 <= end <= 18):
        raise SystemExit("Weeks must be 1–18 for v1")

    return list(range(start, end + 1))

def compute_volatility(con, season: int) -> None:
    # Create table if needed
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_team_week_volatility (
          season INTEGER,
          week INTEGER,
          team VARCHAR,
          rolling_std_total_yards DOUBLE
        )
        """
    )

    # Recompute the entire season (simple + correct for v1)
    con.execute("DELETE FROM mart_team_week_volatility WHERE season = ?", [season])

    con.execute(
        """
        INSERT INTO mart_team_week_volatility
        SELECT
          season,
          week,
          team,
          STDDEV_SAMP(team_total_yards) OVER (
            PARTITION BY season, team
            ORDER BY week
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
          ) AS rolling_std_total_yards
        FROM core_team_week_metrics
        WHERE season = ?
        """,
        [season],
    )

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--week", type=int)
    group.add_argument("--weeks", type=str)  # format: "start-end"

    ap.add_argument("--db-path", type=str, default="data/analytics.duckdb")
    ap.add_argument("--output-dir", type=str, default="reports")
    args = ap.parse_args()


    #if not (1 <= args.week <= 18):
        #raise SystemExit("Week must be 1–18 for v1.")

    t0 = time.time()

    weeks_list = parse_weeks_arg(args.week, args.weeks)
    con = connect(args.db_path)

    # schedule only needs fetching once per season
    schedule = fetch_schedule(args.season)

    for w in weeks_list:
        games = normalize_games(schedule, args.season, w)

        raw_stats = fetch_player_stats_week(args.season, w)
        player_stats = normalize_player_stats(raw_stats, args.season, w)

        team_week = build_team_week_metrics(player_stats)
        team_insights = build_team_week_insights(team_week, player_stats)

        replace_week_slice(con, "core_games", games, args.season, w)
        replace_week_slice(con, "core_player_game_stats", player_stats, args.season, w)
        replace_week_slice(con, "core_team_week_metrics", team_week, args.season, w)
        replace_week_slice(con, "mart_team_week_insights", team_insights, args.season, w)

    compute_volatility(con, args.season)

    # Report: if a single week, write just that week report.
    # If range, write reports for each week in the range.
    for w in weeks_list:
        report_path = Path(args.output_dir) / f"season_{args.season}" / f"week_{w:02d}.md"
        write_week_report(con, args.season, w, report_path)

    print(f"✅ Done in {time.time() - t0:.1f}s")
    print(f"DB: {args.db_path}")
    print(f"Weeks: {weeks_list[0]}-{weeks_list[-1]}")
    print(f"Reports: {Path(args.output_dir) / f'season_{args.season}'}")

if __name__ == "__main__":
    main()
