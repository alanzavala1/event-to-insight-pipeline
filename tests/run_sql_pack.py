from __future__ import annotations

from pathlib import Path
import sys
import duckdb


SQL_DIR = Path("sql")
DB_PATH = Path("data/analytics.duckdb")


def main() -> int:
    if not DB_PATH.exists():
        print(f"❌ DB not found at: {DB_PATH}")
        print("Run the pipeline first, e.g.: python run_pipeline.py --season 2025 --weeks 1-6")
        return 1

    con = duckdb.connect(str(DB_PATH))

    # Always create the view first (safe even if it already exists)
    view_file = SQL_DIR / "00_create_mart_view.sql"
    if view_file.exists():
        con.execute(view_file.read_text(encoding="utf-8"))
    else:
        print("❌ Missing sql/00_create_mart_view.sql")
        return 1

    sql_files = sorted(
        [p for p in SQL_DIR.glob("*.sql") if p.name != "00_create_mart_view.sql"]
    )

    if not sql_files:
        print("❌ No SQL files found in /sql (besides 00_create_mart_view.sql).")
        return 1

    failures: list[tuple[str, str]] = []
    ran = 0

    for path in sql_files:
        sql = path.read_text(encoding="utf-8").strip()
        if not sql:
            failures.append((path.name, "File is empty"))
            continue

        try:
            cur = con.execute(sql)
            # Some queries return results; some may not (but yours all should)
            _ = cur.fetchall()
            ran += 1
            print(f"✅ {path.name}")
        except Exception as e:
            failures.append((path.name, str(e)))
            print(f"❌ {path.name}")
            print(f"   {e}")

    con.close()

    print("")
    print(f"Ran {ran} SQL files.")

    if failures:
        print(f"❌ {len(failures)} failures:")
        for name, err in failures:
            print(f"- {name}: {err}")
        return 1

    print("✅ SQL pack passed (all queries executed).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
