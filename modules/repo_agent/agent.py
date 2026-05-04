"""
Repo Agent — The One System
Autonomously discovers, ingests, and tracks repositories into the knowledge graph.

Usage:
  python agent.py scan            # list all pending repos
  python agent.py ingest          # ingest all pending repos
  python agent.py ingest-one <path>  # ingest a specific path
  python agent.py status          # show knowledge graph state
  python agent.py daemon          # run continuously (default: every 5 min)
  python agent.py daemon --interval 60  # custom interval in seconds
"""

import os
import sys
import time
import sqlite3
import hashlib
import argparse
import requests
from datetime import datetime
from pathlib import Path

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BASE_DIR     = Path(__file__).resolve().parents[2]           # TheOneSystem_v2.3/
DB_PATH      = str(BASE_DIR / "knowledge-graph" / "supernova.db")
PARSER_DIR   = str(BASE_DIR / "knowledge-graph")
WATCH_DIRS   = [
    os.path.expanduser("~/Projects"),
]
SKIP_REPOS   = {
    "awesome-design-md", "awesome-design-md-extracted",
    "obsidian-releases", "siyuan", "gstack",
}
TELEMETRY_URL = "http://localhost:8002/telemetry/signal"
LOG_PATH      = str(BASE_DIR / "logs" / "repo_agent.log")

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _send_telemetry(signal_type: str, value: float):
    try:
        requests.post(TELEMETRY_URL, params={
            "module_name": "repo_agent",
            "signal_type":  signal_type,
            "signal_value": value,
        }, timeout=2)
    except Exception:
        pass


def _ingested_paths(conn: sqlite3.Connection) -> set:
    """Return the set of repo paths already in ingestion_log."""
    c = conn.cursor()
    c.execute("SELECT repo_path FROM ingestion_log WHERE status = 'completed'")
    return {row[0] for row in c.fetchall()}

# ─── CORE ─────────────────────────────────────────────────────────────────────

class RepoAgent:
    def __init__(self):
        sys.path.insert(0, PARSER_DIR)
        from repo_parser import RepoParser
        self.parser = RepoParser(db_path=DB_PATH)

    # ── Discovery ─────────────────────────────────────────────────────────────

    def discover(self) -> list[dict]:
        """
        Scan WATCH_DIRS for directories that have .py files and are not yet
        fully ingested. Returns list of {path, name, py_count} dicts.
        """
        conn = sqlite3.connect(DB_PATH)
        already = _ingested_paths(conn)
        conn.close()

        pending = []
        for watch in WATCH_DIRS:
            if not os.path.isdir(watch):
                continue
            for entry in sorted(os.listdir(watch)):
                if entry.startswith(".") or entry in SKIP_REPOS:
                    continue
                full = os.path.join(watch, entry)
                if not os.path.isdir(full):
                    continue
                if full in already:
                    continue
                # Count Python files
                py_files = list(Path(full).rglob("*.py"))
                py_count = len([
                    p for p in py_files
                    if not any(skip in str(p) for skip in
                               ("node_modules", ".venv", "venv", "__pycache__",
                                ".git", "dist", ".pytest_cache"))
                ])
                if py_count == 0:
                    continue
                pending.append({"path": full, "name": entry, "py_count": py_count})

        return pending

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def ingest_one(self, repo_path: str) -> dict:
        """Ingest a single repository. Returns result dict."""
        name = os.path.basename(repo_path)
        _log(f"→ Ingesting: {name}")
        try:
            self.parser.ingest_repo(repo_path)
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM entities WHERE source_repo = ?", (name,))
            count = c.fetchone()[0]
            conn.close()
            _log(f"  ✓ {name}: {count} entities")
            _send_telemetry("entities_ingested", float(count))
            _send_telemetry("repos_ingested", 1.0)
            return {"repo": name, "entities": count, "status": "ok"}
        except Exception as e:
            _log(f"  ✗ {name}: {e}")
            return {"repo": name, "entities": 0, "status": "error", "error": str(e)}

    def ingest_pending(self) -> list[dict]:
        """Discover and ingest all pending repos."""
        pending = self.discover()
        if not pending:
            _log("No pending repos found.")
            return []
        _log(f"Found {len(pending)} pending repo(s).")
        results = []
        for item in pending:
            results.append(self.ingest_one(item["path"]))
        _send_telemetry("scan_complete", float(len(results)))
        return results

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self) -> dict:
        """Return a summary of the knowledge graph state."""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM entities")
        total_entities = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM relationships")
        total_rels = c.fetchone()[0]

        c.execute("SELECT source_repo, COUNT(*) FROM entities GROUP BY source_repo ORDER BY COUNT(*) DESC")
        by_repo = {row[0]: row[1] for row in c.fetchall()}

        c.execute("SELECT repo_path, entity_count, ingested_at FROM ingestion_log WHERE status='completed' ORDER BY ingested_at DESC LIMIT 5")
        recent = [{"repo": os.path.basename(r[0]), "entities": r[1], "at": r[2]} for r in c.fetchall()]

        conn.close()
        pending = self.discover()

        return {
            "total_entities":     total_entities,
            "total_relationships": total_rels,
            "repos_indexed":      len(by_repo),
            "pending_repos":      len(pending),
            "pending_names":      [p["name"] for p in pending],
            "by_repo":            by_repo,
            "recent_ingestions":  recent,
            "timestamp":          datetime.now().isoformat(),
        }

    # ── Daemon ────────────────────────────────────────────────────────────────

    def run_daemon(self, interval: int = 300):
        """Run continuously, scanning every `interval` seconds."""
        _log(f"Repo Agent daemon started — scan interval: {interval}s")
        _send_telemetry("daemon_start", 1.0)
        try:
            while True:
                _log("Running scan...")
                results = self.ingest_pending()
                _log(f"Scan complete — {len(results)} repo(s) ingested.")
                time.sleep(interval)
        except KeyboardInterrupt:
            _log("Daemon stopped.")
            _send_telemetry("daemon_stop", 0.0)

    def close(self):
        self.parser.close()


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _print_status(s: dict):
    print(f"\n── Knowledge Graph Status {'─'*30}")
    print(f"  Total entities    : {s['total_entities']:,}")
    print(f"  Total relations   : {s['total_relationships']:,}")
    print(f"  Repos indexed     : {s['repos_indexed']}")
    print(f"  Pending ingestion : {s['pending_repos']}")
    if s["pending_names"]:
        for n in s["pending_names"]:
            print(f"    → {n}")
    print(f"\n  By repo:")
    for repo, count in s["by_repo"].items():
        print(f"    {repo:<38} {count:>6,}")
    print(f"\n  Recent ingestions:")
    for r in s["recent_ingestions"]:
        print(f"    {r['repo']:<38} {r['entities']:>6,}  {r['at'][:16]}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Repo Agent — The One System")
    parser.add_argument("command", nargs="?", default="status",
                        choices=["scan", "ingest", "ingest-one", "status", "daemon"])
    parser.add_argument("path", nargs="?", help="Repo path for ingest-one")
    parser.add_argument("--interval", type=int, default=300,
                        help="Daemon scan interval in seconds (default: 300)")
    args = parser.parse_args()

    agent = RepoAgent()

    if args.command == "scan":
        pending = agent.discover()
        if not pending:
            print("No pending repos.")
        else:
            print(f"\n{len(pending)} repo(s) pending ingestion:")
            for p in pending:
                print(f"  {p['name']:<40}  {p['py_count']} .py files")

    elif args.command == "ingest":
        results = agent.ingest_pending()
        if results:
            print(f"\n{len(results)} repo(s) ingested:")
            for r in results:
                icon = "✓" if r["status"] == "ok" else "✗"
                print(f"  {icon} {r['repo']:<38} {r.get('entities', 0):>6,} entities")

    elif args.command == "ingest-one":
        if not args.path:
            print("Error: provide a repo path.  e.g.  python agent.py ingest-one ~/Projects/myrepo")
            sys.exit(1)
        result = agent.ingest_one(os.path.expanduser(args.path))
        icon = "✓" if result["status"] == "ok" else "✗"
        print(f"{icon} {result['repo']}: {result.get('entities', 0):,} entities")

    elif args.command == "status":
        s = agent.status()
        _print_status(s)

    elif args.command == "daemon":
        agent.run_daemon(interval=args.interval)

    agent.close()
