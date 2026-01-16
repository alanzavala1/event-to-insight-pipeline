from __future__ import annotations

import duckdb
import pandas as pd


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    # row format: (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]


def replace_week_slice(
    con: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
    season: int,
    week: int,
) -> None:
    """
    Idempotent overwrite for a (season, week) partition.
    Column-aligned insert, and auto-recreate table if schema drift is detected.
    """
    # Safety: no duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]
    df_cols = list(df.columns)

    con.register("df_view", df)

    # Create if missing
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df_view LIMIT 0")

    # If schema drift: drop + recreate (simple and safe for v1)
    existing_cols = _table_columns(con, table)
    if existing_cols != df_cols:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_view LIMIT 0")

    # Idempotent slice overwrite
    con.execute(f"DELETE FROM {table} WHERE season = ? AND week = ?", [season, week])

    # Column-aligned insert
    col_list = ", ".join([f'"{c}"' for c in df_cols])
    con.execute(f'INSERT INTO {table} ({col_list}) SELECT {col_list} FROM df_view')

    con.unregister("df_view")
