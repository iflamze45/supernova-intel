#!/usr/bin/env python3
"""
Validate PostgreSQL connection before running migration.

Usage:
    Run in: System Terminal (TheOneSystem_v2.3 venv)
    python3 scripts/validate_connection.py

Exit codes:
    0 — connection successful, safe to run migration
    1 — missing DATABASE_URL_SYNC
    2 — connection failed (wrong credentials, host unreachable, etc.)
"""

import os
import sys
from pathlib import Path

# Load .env from project root
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"  Loaded .env from {env_path}")
    else:
        print(f"  WARNING: .env not found at {env_path}")
        print(f"           Copy .env.example to .env and fill in credentials.")
except ImportError:
    print("  WARNING: python-dotenv not installed — reading from environment only")
    print("           Run: pip install python-dotenv")


def check_env():
    url = os.environ.get("DATABASE_URL_SYNC")
    if not url:
        print("\n  ERROR: DATABASE_URL_SYNC is not set.")
        print("         Steps to fix:")
        print("           1. Open TheOneSystem_v2.3/.env")
        print("           2. Fill in DATABASE_URL_SYNC with your Supabase connection string")
        print("           3. Get it from: Supabase dashboard → Settings → Database → URI")
        sys.exit(1)

    # Mask password in display
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        masked = url.replace(parsed.password or "", "***") if parsed.password else url
    except Exception:
        masked = url[:30] + "..."

    print(f"  DATABASE_URL_SYNC: {masked}")
    return url


def check_connection(url: str):
    try:
        import psycopg2
    except ImportError:
        print("\n  ERROR: psycopg2 not installed.")
        print("         Run: pip install psycopg2-binary")
        sys.exit(2)

    print("  Attempting connection...")
    try:
        conn = psycopg2.connect(url, connect_timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT version(), current_database(), current_user;")
        version, dbname, user = cursor.fetchone()
        conn.close()
        print(f"  Connected successfully.")
        print(f"    Database : {dbname}")
        print(f"    User     : {user}")
        print(f"    Server   : {version.split(',')[0]}")
    except Exception as e:
        print(f"\n  ERROR: Connection failed — {e}")
        print("\n  Common fixes:")
        print("    - Check your Supabase project is not paused")
        print("    - Verify password has no special characters that need URL-encoding")
        print("    - Try the 'Connection pooling' URL from Supabase (port 6543)")
        print("    - Confirm your IP is not blocked under Supabase → Settings → Network")
        sys.exit(2)


def main():
    print("\n" + "=" * 56)
    print("  The One System — PostgreSQL Connection Validator")
    print("=" * 56 + "\n")

    url = check_env()
    check_connection(url)

    print("\n" + "=" * 56)
    print("  STATUS: PASS — connection validated")
    print("  Next: run the migration")
    print()
    print("    cd /Users/alexanderanthony/Projects/TheOneSystem_v2.3")
    print("    python3 scripts/migrate_to_postgres.py")
    print("=" * 56 + "\n")


if __name__ == "__main__":
    main()
