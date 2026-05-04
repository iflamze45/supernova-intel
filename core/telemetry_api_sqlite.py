"""
Telemetry API — SQLite fallback edition.
Drop-in replacement for telemetry_api.py when Supabase is unreachable.
Reads from knowledge-graph/supernova.db via aiosqlite.
"""
import os
from datetime import datetime
from pathlib import Path

import aiosqlite
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = Path(__file__).parent.parent / "knowledge-graph" / "supernova.db"

app = FastAPI(title="The One System Telemetry API (SQLite)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _db():
    return aiosqlite.connect(DB_PATH)


# ── Health ─────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    return {"status": "operational", "timestamp": datetime.now().isoformat(), "backend": "sqlite"}


# ── Telemetry ──────────────────────────────────────────────────

@app.get("/telemetry/modules")
async def get_module_status():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT module_name, COUNT(*) as cnt, MAX(signal_value) as latest, MAX(timestamp) as last_ts "
            "FROM telemetry WHERE module_name NOT LIKE 'agent_%' "
            "GROUP BY module_name"
        )
        rows = await cursor.fetchall()
    return {
        row["module_name"]: {
            "signal_count": row["cnt"],
            "latest_value": row["latest"],
            "last_update": row["last_ts"],
            "status": "operational",
        }
        for row in rows
    }


@app.get("/telemetry/active_agent")
async def get_active_agent():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT module_name, timestamp FROM telemetry "
            "WHERE module_name LIKE 'agent_%' "
            "ORDER BY timestamp DESC LIMIT 1"
        )
        row = await cursor.fetchone()
    if row:
        return {"agent": row["module_name"].replace("agent_", ""), "last_pulse": row["timestamp"]}
    return {"agent": "IDLE", "last_pulse": None}


@app.post("/telemetry/signal")
async def log_signal(module_name: str, signal_type: str, signal_value: float):
    signal_id = f"{module_name}:{signal_type}:{datetime.now().timestamp()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO telemetry (id, module_name, signal_type, signal_value, timestamp) "
            "VALUES (?, ?, ?, ?, ?)",
            (signal_id, module_name, signal_type, signal_value, datetime.now().isoformat()),
        )
        await db.commit()
    return {"status": "logged", "id": signal_id}


# ── Knowledge graph ────────────────────────────────────────────

@app.get("/knowledge/graph")
async def get_knowledge_graph():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cursor = await db.execute(
            "SELECT source_repo, COUNT(*) as cnt FROM entities GROUP BY source_repo ORDER BY cnt DESC"
        )
        entity_counts = {row["source_repo"]: row["cnt"] for row in await cursor.fetchall()}

        cursor = await db.execute("SELECT COUNT(*) as cnt FROM relationships")
        relationship_count = (await cursor.fetchone())["cnt"]

        if relationship_count == 0:
            return {
                "entity_counts": entity_counts,
                "relationship_count": 0,
                "most_connected": [],
                "cross_repo_connections": [],
                "repo_dependency_map": {},
                "status": "graph_unpopulated",
                "generated_at": datetime.now().isoformat(),
            }

        cursor = await db.execute("""
            WITH degree_counts AS (
                SELECT source_entity_id AS eid FROM relationships
                UNION ALL
                SELECT target_entity_id FROM relationships
            )
            SELECT e.name, e.entity_type, e.source_repo, COUNT(*) AS degree
            FROM degree_counts d
            JOIN entities e ON d.eid = e.id
            GROUP BY e.id, e.name, e.entity_type, e.source_repo
            ORDER BY degree DESC
            LIMIT 10
        """)
        most_connected = [
            {"name": r["name"], "type": r["entity_type"], "repo": r["source_repo"], "degree": r["degree"]}
            for r in await cursor.fetchall()
        ]

        cursor = await db.execute("""
            SELECT se.name, se.source_repo, te.name, te.source_repo, r.relationship_type
            FROM relationships r
            JOIN entities se ON r.source_entity_id = se.id
            JOIN entities te ON r.target_entity_id = te.id
            WHERE se.source_repo != te.source_repo
            LIMIT 20
        """)
        cross_repo = [
            {"from": r[0], "from_repo": r[1], "to": r[2], "to_repo": r[3], "type": r[4]}
            for r in await cursor.fetchall()
        ]

        cursor = await db.execute("""
            SELECT DISTINCT se.source_repo, te.source_repo
            FROM relationships r
            JOIN entities se ON r.source_entity_id = se.id
            JOIN entities te ON r.target_entity_id = te.id
            WHERE se.source_repo != te.source_repo
        """)
        dep_map: dict = {}
        for row in await cursor.fetchall():
            from_repo, to_repo = row[0], row[1]
            dep_map.setdefault(from_repo, [])
            if to_repo not in dep_map[from_repo]:
                dep_map[from_repo].append(to_repo)

    return {
        "entity_counts": entity_counts,
        "relationship_count": relationship_count,
        "most_connected": most_connected,
        "cross_repo_connections": cross_repo,
        "repo_dependency_map": dep_map,
        "status": "graph_populated",
        "generated_at": datetime.now().isoformat(),
    }


# ── Repo agent ─────────────────────────────────────────────────

@app.get("/repo_agent/status")
async def get_repo_agent_status():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        cursor = await db.execute("SELECT COUNT(*) as cnt FROM entities")
        total_e = (await cursor.fetchone())["cnt"]

        cursor = await db.execute("SELECT COUNT(*) as cnt FROM relationships")
        total_r = (await cursor.fetchone())["cnt"]

        cursor = await db.execute(
            "SELECT source_repo, COUNT(*) as cnt FROM entities GROUP BY source_repo ORDER BY cnt DESC"
        )
        by_repo = {row["source_repo"]: row["cnt"] for row in await cursor.fetchall()}

        cursor = await db.execute(
            "SELECT repo_path, entity_count, ingested_at "
            "FROM ingestion_log WHERE status='completed' "
            "ORDER BY ingested_at DESC LIMIT 5"
        )
        recent = [
            {"repo": os.path.basename(r["repo_path"]), "entities": r["entity_count"], "ingested_at": r["ingested_at"]}
            for r in await cursor.fetchall()
        ]

    return {
        "total_entities": total_e,
        "total_relationships": total_r,
        "repos_indexed": len(by_repo),
        "by_repo": by_repo,
        "recent_ingestions": recent,
        "timestamp": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
