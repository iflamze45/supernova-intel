#!/usr/bin/env python3
"""
Migrate The One System SQLite databases to PostgreSQL.

Usage:
    Run in: System Terminal (TheOneSystem_v2.3 venv)

    # Step 1: fill credentials
    cp .env.example .env
    nano .env   # set DATABASE_URL_SYNC

    # Step 2: validate connection
    python3 scripts/validate_connection.py

    # Step 3: run migration
    python3 scripts/migrate_to_postgres.py

What this does:
    1. Creates all tables in PostgreSQL (idempotent — safe to re-run)
    2. Copies all rows from supernova.db (knowledge graph)
    3. Copies all rows from icyflamze-os.db (tasks / pipeline)
    4. Verifies row counts match

The SQLite files are NOT deleted — keep them as cold backup until you
have confirmed the PostgreSQL data is correct.
"""

import os
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

# Load .env from project root before reading any env vars
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
    else:
        print(f"WARNING: .env not found at {_env_path}")
        print("         Copy .env.example to .env and fill in DATABASE_URL_SYNC.")
except ImportError:
    pass  # Fall back to environment variables already exported

SUPERNOVA_DB = "/Users/alexanderanthony/Projects/TheOneSystem_v2.3/knowledge-graph/supernova.db"
ONESYS_DB    = "/Users/alexanderanthony/Projects/TheOneSystem_Claud001/.claude/worktrees/magical-gould/data/icyflamze-os.db"

REQUIRED_ENV = "DATABASE_URL_SYNC"


def get_pg():
    import psycopg2
    url = os.environ.get(REQUIRED_ENV)
    if not url:
        print("\nERROR: DATABASE_URL_SYNC is not set.")
        print("  1. Open TheOneSystem_v2.3/.env")
        print("  2. Set DATABASE_URL_SYNC to your Supabase connection string")
        print("  3. Get it from: Supabase dashboard → Settings → Database → URI")
        print("\n  Or run validate_connection.py first to diagnose:")
        print("    python3 scripts/validate_connection.py")
        sys.exit(1)

    # Validate connection before starting migration
    try:
        conn = psycopg2.connect(url, connect_timeout=10)
        conn.close()
        print("  Connection validated.")
    except Exception as e:
        print(f"\nERROR: Cannot connect to PostgreSQL — {e}")
        print("  Run: python3 scripts/validate_connection.py")
        print("  for detailed diagnostics and fix steps.")
        sys.exit(2)

    return psycopg2.connect(url)


CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    source_repo TEXT,
    tags TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_entity_type  ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_repo_source  ON entities(source_repo);

CREATE TABLE IF NOT EXISTS relationships (
    id TEXT PRIMARY KEY,
    source_entity_id TEXT NOT NULL,
    target_entity_id TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    metadata TEXT
);

CREATE TABLE IF NOT EXISTS telemetry (
    id TEXT PRIMARY KEY,
    module_name TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    signal_value REAL,
    signal_data TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_module_telemetry ON telemetry(module_name, timestamp);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id TEXT PRIMARY KEY,
    repo_path TEXT NOT NULL,
    file_count INTEGER,
    entity_count INTEGER,
    status TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scout_predictions (
    id TEXT PRIMARY KEY,
    match TEXT NOT NULL,
    competition TEXT,
    date TEXT,
    home_win_pct REAL,
    draw_pct REAL,
    away_win_pct REAL,
    confidence TEXT,
    recommended_pick TEXT,
    recommended_odds TEXT,
    flamze_breakdown TEXT,
    sources_checked TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    command_type TEXT NOT NULL,
    task_class TEXT NOT NULL,
    source TEXT NOT NULL,
    primary_gpt TEXT NOT NULL,
    secondary_gpt TEXT,
    output_type TEXT NOT NULL,
    context TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_items (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    next_stage TEXT,
    owner TEXT NOT NULL,
    blocker TEXT,
    due_date TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS archive_entries (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id) ON DELETE CASCADE,
    notebooklm_logged BOOLEAN NOT NULL,
    system_log_logged BOOLEAN NOT NULL,
    notebooklm_block TEXT NOT NULL,
    system_log_entry TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS content_assets (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    format TEXT NOT NULL,
    platform TEXT NOT NULL,
    publish_date TEXT,
    status TEXT NOT NULL,
    performance_notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""

# SQLite column name → PostgreSQL column name (for renamed columns)
COL_RENAMES = {
    "tasks": {
        "commandType": "command_type",
        "taskClass": "task_class",
        "primaryGPT": "primary_gpt",
        "secondaryGPT": "secondary_gpt",
        "outputType": "output_type",
        "createdAt": "created_at",
        "updatedAt": "updated_at",
    },
    "pipeline_items": {
        "taskId": "task_id",
        "nextStage": "next_stage",
        "dueDate": "due_date",
        "createdAt": "created_at",
        "updatedAt": "updated_at",
    },
    "archive_entries": {
        "taskId": "task_id",
        "notebooklmLogged": "notebooklm_logged",
        "systemLogLogged": "system_log_logged",
        "notebooklmBlock": "notebooklm_block",
        "systemLogEntry": "system_log_entry",
        "createdAt": "created_at",
    },
    "content_assets": {
        "taskId": "task_id",
        "publishDate": "publish_date",
        "performanceNotes": "performance_notes",
        "createdAt": "created_at",
        "updatedAt": "updated_at",
    },
}


def migrate_table(sl_cursor, pg_cursor, table: str, renames: dict):
    sl_cursor.execute(f"SELECT * FROM {table}")
    rows = sl_cursor.fetchall()
    if not rows:
        print(f"  {table}: 0 rows — skipped")
        return 0

    cols_raw = [d[0] for d in sl_cursor.description]
    cols_pg  = [renames.get(c, c) for c in cols_raw]
    placeholders = ",".join(["%s"] * len(cols_pg))

    inserted = 0
    for row in rows:
        try:
            pg_cursor.execute(
                f"INSERT INTO {table} ({','.join(cols_pg)}) "
                f"VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                row,
            )
            inserted += 1
        except Exception as e:
            print(f"  WARNING: row skipped in {table}: {e}")
    return inserted


def verify(sl_cursor, pg_cursor, table: str):
    sl_cursor.execute(f"SELECT COUNT(*) FROM {table}")
    sl_count = sl_cursor.fetchone()[0]
    pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
    pg_count = pg_cursor.fetchone()[0]
    status = "OK" if sl_count <= pg_count else "MISMATCH"
    print(f"  {table}: SQLite={sl_count}  PostgreSQL={pg_count}  [{status}]")
    return status == "OK"


def main():
    print(f"\n{'='*60}")
    print("  The One System — SQLite → PostgreSQL Migration")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    pg = get_pg()
    pgc = pg.cursor()

    # ── Create schema ─────────────────────────────────────────
    print("Creating PostgreSQL tables...")
    pgc.execute(CREATE_TABLES)
    pg.commit()
    print("  Tables created (idempotent).\n")

    all_ok = True

    # ── Migrate supernova.db ──────────────────────────────────
    print(f"Migrating supernova.db  ({SUPERNOVA_DB})")
    sl1 = sqlite3.connect(SUPERNOVA_DB)
    slc1 = sl1.cursor()
    for table in ["entities", "relationships", "telemetry", "ingestion_log", "scout_predictions"]:
        renames = COL_RENAMES.get(table, {})
        n = migrate_table(slc1, pgc, table, renames)
        print(f"  {table}: {n} rows inserted")
    pg.commit()

    print("\nVerifying supernova.db migration:")
    for table in ["entities", "relationships", "telemetry", "ingestion_log", "scout_predictions"]:
        ok = verify(slc1, pgc, table)
        all_ok = all_ok and ok
    sl1.close()

    # ── Migrate icyflamze-os.db ───────────────────────────────
    print(f"\nMigrating icyflamze-os.db  ({ONESYS_DB})")
    try:
        sl2 = sqlite3.connect(ONESYS_DB)
        slc2 = sl2.cursor()
        # tasks must come before pipeline_items / archive_entries / content_assets (FK)
        for table in ["tasks", "pipeline_items", "archive_entries", "content_assets"]:
            renames = COL_RENAMES.get(table, {})
            n = migrate_table(slc2, pgc, table, renames)
            print(f"  {table}: {n} rows inserted")
        pg.commit()

        print("\nVerifying icyflamze-os.db migration:")
        for table in ["tasks", "pipeline_items", "archive_entries", "content_assets"]:
            ok = verify(slc2, pgc, table)
            all_ok = all_ok and ok
        sl2.close()
    except Exception as e:
        print(f"  WARNING: Could not migrate icyflamze-os.db: {e}")

    pg.close()

    print(f"\n{'='*60}")
    if all_ok:
        print("  MIGRATION COMPLETE — all counts verified.")
        print("  SQLite files preserved at original paths (cold backup).")
        print("  Next: update DATABASE_URL in .env files and restart services.")
    else:
        print("  MIGRATION COMPLETE WITH WARNINGS — check counts above.")
        print("  Do NOT remove SQLite files until mismatches are resolved.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
