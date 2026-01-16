from __future__ import annotations

from pathlib import Path
import duckdb


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    q = """
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = ?
    """
    return con.execute(q, [table]).fetchone()[0] > 0


def _format_pct(x: float) -> str:
    return f"{x * 100:.0f}%"


def write_week_report(con: duckdb.DuckDBPyConnection, season: int, week: int, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # --- Top offenses ---
    top5_yards = con.execute(
        """
        SELECT team, opponent, team_total_yards
        FROM core_team_week_metrics
        WHERE season = ? AND week = ?
        ORDER BY team_total_yards DESC
        LIMIT 5
        """,
        [season, week],
    ).fetchall()

    lines: list[str] = [
        "# Weekly Insights Report",
        f"Season: **{season}** | Week: **{week:02d}**",
        "",
        "## Top 5 Offenses (Total Yards)",
        "",
    ]

    for team, opp, yards in top5_yards:
        lines.append(f"- **{team}** vs {opp}: {int(yards)} yards")

    # --- Insight bullets ---
    bullets: list[str] = []

    if _table_exists(con, "mart_team_week_insights"):
        hi_dep = con.execute(
            """
            SELECT team, opponent, top_player_share, top_player_name
            FROM mart_team_week_insights
            WHERE season=? AND week=? AND top_player_share IS NOT NULL
            ORDER BY top_player_share DESC
            LIMIT 1
            """,
            [season, week],
        ).fetchall()

        if hi_dep:
            team, opp, share, name = hi_dep[0]
            if share >= 0.45:
                bullets.append(
                    f"**{team}** had high dependency vs {opp} "
                    f"({_format_pct(share)} of offense via {name})."
                )

        pass_extreme = con.execute(
            """
            SELECT team, opponent, pass_share
            FROM mart_team_week_insights
            WHERE season=? AND week=? AND pass_share IS NOT NULL
            ORDER BY pass_share DESC
            LIMIT 1
            """,
            [season, week],
        ).fetchall()

        run_extreme = con.execute(
            """
            SELECT team, opponent, pass_share
            FROM mart_team_week_insights
            WHERE season=? AND week=? AND pass_share IS NOT NULL
            ORDER BY pass_share ASC
            LIMIT 1
            """,
            [season, week],
        ).fetchall()

        if pass_extreme:
            team, opp, ps = pass_extreme[0]
            if ps >= 0.75:
                bullets.append(
                    f"**{team}** was pass-heavy vs {opp} "
                    f"(pass share {_format_pct(ps)})."
                )

        if run_extreme:
            team, opp, ps = run_extreme[0]
            if ps <= 0.45:
                bullets.append(
                    f"**{team}** leaned run-heavy vs {opp} "
                    f"(pass share {_format_pct(ps)})."
                )

    if _table_exists(con, "mart_team_week_volatility"):
        most_consistent = con.execute(
            """
            SELECT team, rolling_std_total_yards
            FROM mart_team_week_volatility
            WHERE season=? AND week=? AND rolling_std_total_yards IS NOT NULL
            ORDER BY rolling_std_total_yards ASC
            LIMIT 1
            """,
            [season, week],
        ).fetchall()

        if most_consistent:
            team, stdv = most_consistent[0]
            if stdv <= 15:
                bullets.append(
                    f"**{team}** has been very consistent over the last 3 weeks "
                    f"(stddev {stdv:.1f} yards)."
                )

    if bullets:
        lines += ["", "## Insight Bullets", ""]
        for b in bullets[:3]:
            lines.append(f"- {b}")

    # --- Pass share + dependency tables ---
    if _table_exists(con, "mart_team_week_insights"):
        pass_heavy = con.execute(
            """
            SELECT team, opponent, pass_share
            FROM mart_team_week_insights
            WHERE season=? AND week=? AND pass_share IS NOT NULL
            ORDER BY pass_share DESC
            LIMIT 5
            """,
            [season, week],
        ).fetchall()

        run_heavy = con.execute(
            """
            SELECT team, opponent, pass_share
            FROM mart_team_week_insights
            WHERE season=? AND week=? AND pass_share IS NOT NULL
            ORDER BY pass_share ASC
            LIMIT 5
            """,
            [season, week],
        ).fetchall()

        dependency = con.execute(
            """
            SELECT team, opponent, top_player_share, top_player_name, top_player_yards, team_total_yards
            FROM mart_team_week_insights
            WHERE season=? AND week=? AND top_player_share IS NOT NULL
            ORDER BY top_player_share DESC
            LIMIT 5
            """,
            [season, week],
        ).fetchall()

        lines += ["", "## Offensive Tendencies (Pass Share)", ""]
        lines.append("### Most Pass-Heavy")
        for team, opp, share in pass_heavy:
            lines.append(f"- **{team}** vs {opp}: {share:.2f}")

        lines.append("")
        lines.append("### Most Run-Heavy")
        for team, opp, share in run_heavy:
            lines.append(f"- **{team}** vs {opp}: {share:.2f}")

        lines += ["", "## Dependency Risk (Top Player Share)", ""]
        for team, opp, share, name, yards, total in dependency:
            lines.append(
                f"- **{team}** vs {opp}: {share:.2f} "
                f"({name} — {int(yards)} of {int(total)} yards)"
            )

    # --- Consistency ---
    if _table_exists(con, "mart_team_week_volatility"):
        consistent = con.execute(
            """
            SELECT v.team, m.opponent, v.rolling_std_total_yards
            FROM mart_team_week_volatility v
            JOIN core_team_week_metrics m
              ON m.season=v.season AND m.week=v.week AND m.team=v.team
            WHERE v.season=? AND v.week=? AND v.rolling_std_total_yards IS NOT NULL
            ORDER BY v.rolling_std_total_yards ASC
            LIMIT 5
            """,
            [season, week],
        ).fetchall()

        lines += ["", "## Consistency (Rolling 3-Week StdDev of Total Yards)", ""]
        if not consistent:
            lines.append("_(Not enough prior weeks yet — available starting Week 03.)_")
        else:
            for team, opp, stdv in consistent:
                lines.append(f"- **{team}** vs {opp}: {stdv:.1f}")

    out_path.write_text("\n".join(lines), encoding="utf-8")
