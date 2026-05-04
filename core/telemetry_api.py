import os
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from postgrest.types import CountMethod  # type: ignore[attr-defined]
from supabase import acreate_client
from supabase._async.client import AsyncClient  # type: ignore[import-untyped]

load_dotenv(Path(__file__).parent.parent / ".env")

SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_KEY", "")

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


app = FastAPI(title="The One System — Telemetry API (Postgres)", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health ────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    return {
        "status": "operational",
        "backend": "supabase",
        "timestamp": datetime.now().isoformat(),
    }


# ── Telemetry ─────────────────────────────────────────────────

@app.get("/telemetry/modules")
async def get_module_status():
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


@app.get("/telemetry/active_agent")
async def get_active_agent():
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
    e_res = await db().table("entities").select("*", count=CountMethod.exact).limit(0).execute()
    r_res = await db().table("relationships").select("*", count=CountMethod.exact).limit(0).execute()
    return {
        "total_entities": e_res.count,
        "total_relationships": r_res.count,
    }


# ── Knowledge graph ───────────────────────────────────────────

@app.get("/knowledge/graph")
async def get_knowledge_graph():
    result = await db().rpc("get_knowledge_graph_data").execute()
    data: dict = result.data
    data["generated_at"] = datetime.now().isoformat()
    return data


# ── Repo agent ────────────────────────────────────────────────

@app.get("/repo_agent/status")
async def get_repo_agent_status():
    result = await db().rpc("get_repo_agent_status").execute()
    data: dict = result.data
    data["timestamp"] = datetime.now().isoformat()
    return data


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
