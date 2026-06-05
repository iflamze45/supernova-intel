import os
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import aiosqlite
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from postgrest.types import CountMethod  # type: ignore[attr-defined]
from supabase import acreate_client
from supabase._async.client import AsyncClient  # type: ignore[import-untyped]

from core.voice_bridge import create_voice_bridge_router

load_dotenv(Path(__file__).parent.parent / ".env")

SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")
DB_PATH = Path(__file__).parent.parent / "knowledge-graph" / "supernova.db"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

_client: Optional[AsyncClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = await acreate_client(SUPABASE_URL, SUPABASE_KEY)
    yield


def db() -> AsyncClient:
    assert _client is not None
    return _client


async def _sqlite_health() -> dict[str, str]:
    return {
        "status": "operational",
        "backend": "sqlite",
        "timestamp": datetime.now().isoformat(),
    }


async def _sqlite_modules() -> dict[str, dict[str, object]]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            "SELECT module_name, COUNT(*) as signal_count, MAX(signal_value) as latest_value, MAX(timestamp) as last_update "
            "FROM telemetry WHERE module_name NOT LIKE 'agent_%' "
            "GROUP BY module_name"
        )
        rows = await cursor.fetchall()
    return {
        row["module_name"]: {
            "signal_count": row["signal_count"],
            "latest_value": row["latest_value"],
            "last_update": row["last_update"],
            "status": "operational",
        }
        for row in rows
    }


async def _sqlite_active_agent() -> dict[str, object]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            "SELECT module_name, timestamp FROM telemetry "
            "WHERE module_name LIKE 'agent_%' "
            "ORDER BY timestamp DESC LIMIT 1"
        )
        row = await cursor.fetchone()
    if row:
        return {"agent": row["module_name"].replace("agent_", ""), "last_pulse": row["timestamp"]}
    return {"agent": "IDLE", "last_pulse": None}


async def _sqlite_stats() -> dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM entities")
        total_entities = (await cursor.fetchone())[0]
        cursor = await conn.execute("SELECT COUNT(*) FROM relationships")
        total_relationships = (await cursor.fetchone())[0]
    return {"total_entities": total_entities, "total_relationships": total_relationships}


async def _sqlite_knowledge_graph() -> dict[str, object]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row

        cursor = await conn.execute(
            "SELECT source_repo, COUNT(*) as cnt FROM entities GROUP BY source_repo ORDER BY cnt DESC"
        )
        entity_counts = {row["source_repo"]: row["cnt"] for row in await cursor.fetchall()}

        cursor = await conn.execute("SELECT COUNT(*) as cnt FROM relationships")
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

        cursor = await conn.execute(
            """
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
            """
        )
        most_connected = [
            {"name": r["name"], "type": r["entity_type"], "repo": r["source_repo"], "degree": r["degree"]}
            for r in await cursor.fetchall()
        ]

        cursor = await conn.execute(
            """
            SELECT se.name, se.source_repo, te.name, te.source_repo, r.relationship_type
            FROM relationships r
            JOIN entities se ON r.source_entity_id = se.id
            JOIN entities te ON r.target_entity_id = te.id
            WHERE se.source_repo != te.source_repo
            LIMIT 20
            """
        )
        cross_repo = [
            {"from": r[0], "from_repo": r[1], "to": r[2], "to_repo": r[3], "type": r[4]}
            for r in await cursor.fetchall()
        ]

        cursor = await conn.execute(
            """
            SELECT DISTINCT se.source_repo, te.source_repo
            FROM relationships r
            JOIN entities se ON r.source_entity_id = se.id
            JOIN entities te ON r.target_entity_id = te.id
            WHERE se.source_repo != te.source_repo
            """
        )
        dep_map: dict[str, list[str]] = {}
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


async def _sqlite_repo_agent_status() -> dict[str, object]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row

        cursor = await conn.execute("SELECT COUNT(*) as cnt FROM entities")
        total_e = (await cursor.fetchone())["cnt"]

        cursor = await conn.execute("SELECT COUNT(*) as cnt FROM relationships")
        total_r = (await cursor.fetchone())["cnt"]

        cursor = await conn.execute(
            "SELECT source_repo, COUNT(*) as cnt FROM entities GROUP BY source_repo ORDER BY cnt DESC"
        )
        by_repo = {row["source_repo"]: row["cnt"] for row in await cursor.fetchall()}

        cursor = await conn.execute(
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


app = FastAPI(title="The One System — Telemetry API (Postgres)", lifespan=lifespan)
_ALLOWED_ORIGINS = [
    "http://localhost:5174",
    "http://127.0.0.1:5174",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "https://the-one-system-ui.vercel.app",
    "https://dashboard-one-liard-26.vercel.app",
    *[
        origin.strip()
        for origin in os.getenv("EXTRA_CORS_ORIGINS", "").split(",")
        if origin.strip()
    ],
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)
app.include_router(create_voice_bridge_router())


# ── Health ────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    if SUPABASE_URL and SUPABASE_KEY:
        return {
            "status": "operational",
            "backend": "supabase",
            "timestamp": datetime.now().isoformat(),
        }
    return await _sqlite_health()


# ── Telemetry ─────────────────────────────────────────────────

@app.get("/telemetry/modules")
async def get_module_status():
    try:
        result = await db().rpc("get_module_telemetry").execute()
        return {
            row["module_name"]: {
                "signal_count": row["signal_count"],
                "latest_value": row["latest_value"],
                "last_update": row["last_update"],
                "status": "operational",
            }
            for row in (result.data or [])
        }
    except Exception:
        return await _sqlite_modules()


@app.get("/telemetry/active_agent")
async def get_active_agent():
    try:
        result = (
            await db()
            .table("telemetry")
            .select("module_name,timestamp")
            .like("module_name", "agent_%")
            .order("timestamp", desc=True)
            .limit(1)
            .execute()
        )
        if result.data:
            row = result.data[0]
            return {"agent": row["module_name"].replace("agent_", ""), "last_pulse": row["timestamp"]}
        return {"agent": "IDLE", "last_pulse": None}
    except Exception:
        return await _sqlite_active_agent()


@app.post("/telemetry/signal")
async def log_signal(module_name: str, signal_type: str, signal_value: float):
    signal_id = f"{module_name}:{signal_type}:{datetime.now().timestamp()}"
    await db().table("telemetry").insert({
        "id": signal_id,
        "module_name": module_name,
        "signal_type": signal_type,
        "signal_value": signal_value,
        "timestamp": datetime.now().isoformat(),
    }).execute()
    return {"status": "logged", "id": signal_id}


@app.get("/telemetry/scout/summary")
async def get_scout_summary():
    result = (
        await db()
        .table("telemetry")
        .select("signal_type,signal_value,timestamp")
        .eq("module_name", "scout")
        .order("timestamp", desc=True)
        .limit(5)
        .execute()
    )
    return [{"metric": r["signal_type"], "value": r["signal_value"], "time": r["timestamp"]} for r in (result.data or [])]


@app.get("/telemetry/tree_groove/summary")
async def get_tree_groove_summary():
    result = (
        await db()
        .table("telemetry")
        .select("signal_type,signal_value,timestamp")
        .eq("module_name", "tree_groove")
        .order("timestamp", desc=True)
        .limit(5)
        .execute()
    )
    return [{"metric": r["signal_type"], "value": r["signal_value"], "time": r["timestamp"]} for r in (result.data or [])]


@app.get("/telemetry/social/summary")
async def get_social_summary():
    result = (
        await db()
        .table("telemetry")
        .select("signal_type,signal_value,timestamp")
        .eq("module_name", "social")
        .order("timestamp", desc=True)
        .limit(5)
        .execute()
    )
    return [{"metric": r["signal_type"], "value": r["signal_value"], "time": r["timestamp"]} for r in (result.data or [])]


# ── Stats ─────────────────────────────────────────────────────

@app.get("/stats")
async def get_stats():
    try:
        e_res = await db().table("entities").select("*", count=CountMethod.exact).limit(0).execute()
        r_res = await db().table("relationships").select("*", count=CountMethod.exact).limit(0).execute()
        return {
            "total_entities": e_res.count,
            "total_relationships": r_res.count,
        }
    except Exception:
        return await _sqlite_stats()


# ── Knowledge graph ───────────────────────────────────────────

@app.get("/knowledge/graph")
async def get_knowledge_graph():
    try:
        result = await db().rpc("get_knowledge_graph_data").execute()
        data: dict = result.data
        data["generated_at"] = datetime.now().isoformat()
        return data
    except Exception:
        return await _sqlite_knowledge_graph()


# ── Repo agent ────────────────────────────────────────────────

@app.get("/repo_agent/status")
async def get_repo_agent_status():
    try:
        result = await db().rpc("get_repo_agent_status").execute()
        data: dict = result.data
        data["timestamp"] = datetime.now().isoformat()
        return data
    except Exception:
        return await _sqlite_repo_agent_status()


@app.post("/repo_agent/ingest")
async def trigger_ingest(repo_path: str):
    import sys as _sys
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    _sys.path.insert(0, str(Path(__file__).parent.parent / "modules"))
    from repo_agent.agent import RepoAgent  # type: ignore[import-untyped]

    def _run():
        agent = RepoAgent()
        result = agent.ingest_one(repo_path)
        agent.close()
        return result

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=1) as ex:
        result = await loop.run_in_executor(ex, _run)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
