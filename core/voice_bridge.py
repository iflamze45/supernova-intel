import asyncio
import hmac
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field


COMMAND_MODULE = "voice_vibe_bridge_command"
ACK_MODULE = "voice_vibe_bridge_ack"
ALLOWED_COMMANDS = {
    "on",
    "off",
    "silent",
    "focus",
    "review",
    "policy-strict",
    "policy-open",
    "allow-user-text",
    "mute-user-text",
}
ALLOWED_POLICIES = {"strict", "open"}
EVENT_PATTERN = re.compile(r"^[a-z0-9_]{1,48}$")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def require_bridge_token(authorization: str | None, expected_token: str) -> None:
    if not expected_token:
        raise HTTPException(status_code=503, detail="voice bridge is not configured")
    prefix = "Bearer "
    if not authorization or not authorization.startswith(prefix):
        raise HTTPException(status_code=401, detail="bridge authentication required")
    supplied = authorization[len(prefix):]
    if not hmac.compare_digest(supplied, expected_token):
        raise HTTPException(status_code=401, detail="invalid bridge credentials")


def normalize_rules(payload: dict[str, Any]) -> dict[str, Any]:
    policy = payload.get("policy")
    if policy not in ALLOWED_POLICIES:
        raise HTTPException(status_code=400, detail="unsupported voice policy")

    normalized: list[str] = []
    for raw in payload.get("allowed_events", []):
        event = str(raw).strip().lower().replace(" ", "_")
        if not EVENT_PATTERN.fullmatch(event):
            raise HTTPException(status_code=400, detail="invalid voice event")
        if event not in normalized:
            normalized.append(event)
    if len(normalized) > 32:
        raise HTTPException(status_code=400, detail="too many voice events")

    return {
        "type": "rules",
        "allowed_events": normalized,
        "speak_user_text": bool(payload.get("speak_user_text", False)),
        "policy": policy,
    }


def normalize_command_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload_type = payload.get("type")
    if payload_type == "rules":
        return normalize_rules(payload)
    if payload_type != "command":
        raise HTTPException(status_code=400, detail="unsupported bridge payload")
    command = payload.get("command")
    if command not in ALLOWED_COMMANDS:
        raise HTTPException(status_code=400, detail="unsupported voice-vibe command")
    return {"type": "command", "command": command}


def build_command_record(
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
    ttl_seconds: int = 90,
    command_id: str | None = None,
) -> dict[str, Any]:
    current = now or _utcnow()
    bounded_ttl = min(max(ttl_seconds, 15), 300)
    normalized = normalize_command_payload(payload)
    identifier = command_id or str(uuid.uuid4())
    envelope = {
        "id": identifier,
        "payload": normalized,
        "created_at": current.isoformat(),
        "expires_at": (current + timedelta(seconds=bounded_ttl)).isoformat(),
    }
    return {
        "id": identifier,
        "module_name": COMMAND_MODULE,
        "signal_type": _json(envelope),
        "signal_value": 1.0,
        "timestamp": current.isoformat(),
    }


def decode_pending_commands(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    current = now or _utcnow()
    pending: list[dict[str, Any]] = []
    for row in rows:
        try:
            envelope = json.loads(str(row["signal_type"]))
            expires_at = datetime.fromisoformat(envelope["expires_at"])
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            payload = normalize_command_payload(envelope["payload"])
            identifier = str(envelope["id"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, HTTPException):
            continue
        if expires_at <= current:
            continue
        pending.append(
            {
                "id": identifier,
                "payload": payload,
                "created_at": envelope["created_at"],
                "expires_at": envelope["expires_at"],
            }
        )
    return sorted(pending, key=lambda item: (item["created_at"], item["id"]))


def build_ack_record(
    *,
    command_id: str,
    ok: bool,
    message: str,
    state: dict[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or _utcnow()
    safe_state = {
        key: state[key]
        for key in (
            "enabled",
            "profile",
            "mode",
            "mood",
            "policy",
            "speak_user_text",
            "allowed_events",
            "last_log",
            "updated_at",
        )
        if key in state
    }
    payload = {
        "command_id": command_id[:128],
        "ok": bool(ok),
        "message": message[:512],
        "state": safe_state,
        "acknowledged_at": current.isoformat(),
    }
    return {
        "id": f"ack:{command_id[:128]}:{uuid.uuid4()}",
        "module_name": ACK_MODULE,
        "signal_type": _json(payload),
        "signal_value": 1.0 if ok else 0.0,
        "timestamp": current.isoformat(),
    }


class CommandBody(BaseModel):
    type: str
    command: str | None = None
    allowed_events: list[str] = Field(default_factory=list, max_length=32)
    speak_user_text: bool | None = None
    policy: str | None = None


class AckBody(BaseModel):
    command_id: str = Field(min_length=1, max_length=128)
    ok: bool
    message: str = Field(default="", max_length=1024)
    state: dict[str, Any] = Field(default_factory=dict)


class FileBridgeStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"commands": [], "acks": []}
        try:
            value = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return {"commands": [], "acks": []}
        if not isinstance(value, dict):
            return {"commands": [], "acks": []}
        return {
            "commands": list(value.get("commands", [])),
            "acks": list(value.get("acks", [])),
        }

    def _write(self, value: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        temp.write_text(json.dumps(value, separators=(",", ":"), sort_keys=True))
        temp.replace(self.path)

    async def enqueue(self, record: dict[str, Any]) -> None:
        async with self._lock:
            value = self._read()
            value["commands"] = (value["commands"] + [record])[-100:]
            self._write(value)

    async def poll(self, after: str | None) -> list[dict[str, Any]]:
        async with self._lock:
            value = self._read()
            rows = value["commands"]
            if after:
                rows = [row for row in rows if str(row.get("timestamp", "")) > after]
            pending = decode_pending_commands(rows)
            value["commands"] = [
                row
                for row in value["commands"]
                if any(item["id"] == row.get("id") for item in pending)
            ]
            self._write(value)
            return pending

    async def acknowledge(self, record: dict[str, Any]) -> None:
        async with self._lock:
            value = self._read()
            value["acks"] = (value["acks"] + [record])[-100:]
            self._write(value)

    async def latest_state(self) -> dict[str, Any] | None:
        async with self._lock:
            value = self._read()
            if not value["acks"]:
                return None
            try:
                return json.loads(value["acks"][-1]["signal_type"])
            except (KeyError, TypeError, json.JSONDecodeError):
                return None


def create_voice_bridge_router(
    store: FileBridgeStore | None = None,
    token_provider: Callable[[], str] | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/voice-bridge", tags=["voice-bridge"])
    get_token = token_provider or (lambda: os.getenv("VOICE_VIBE_BRIDGE_TOKEN", ""))
    bridge_store = store or FileBridgeStore(
        Path(os.getenv("VOICE_VIBE_BRIDGE_STORE", "/tmp/voice-vibe-bridge.json"))
    )

    def authenticate(authorization: str | None) -> None:
        require_bridge_token(authorization, get_token())

    @router.post("/commands")
    async def enqueue_command(
        body: CommandBody,
        authorization: str | None = Header(default=None),
    ):
        authenticate(authorization)
        record = build_command_record(body.model_dump(exclude_none=True))
        await bridge_store.enqueue(record)
        envelope = json.loads(record["signal_type"])
        return {
            "status": "queued",
            "command_id": record["id"],
            "expires_at": envelope["expires_at"],
        }

    @router.get("/poll")
    async def poll_commands(
        after: str | None = Query(default=None, max_length=64),
        authorization: str | None = Header(default=None),
    ):
        authenticate(authorization)
        if after:
            try:
                datetime.fromisoformat(after)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="invalid cursor") from exc
        commands = await bridge_store.poll(after)
        return {"commands": commands, "server_time": _utcnow().isoformat()}

    @router.post("/ack")
    async def acknowledge_command(
        body: AckBody,
        authorization: str | None = Header(default=None),
    ):
        authenticate(authorization)
        record = build_ack_record(
            command_id=body.command_id,
            ok=body.ok,
            message=body.message,
            state=body.state,
        )
        await bridge_store.acknowledge(record)
        return {"status": "acknowledged", "command_id": body.command_id}

    @router.get("/state")
    async def get_latest_state(
        authorization: str | None = Header(default=None),
    ):
        authenticate(authorization)
        payload = await bridge_store.latest_state()
        if not payload:
            return {"status": "waiting", "state": None}
        return {
            "status": "online",
            "state": payload.get("state"),
            "last_ack": {
                "command_id": payload.get("command_id"),
                "ok": payload.get("ok"),
                "message": payload.get("message"),
                "acknowledged_at": payload.get("acknowledged_at"),
            },
        }

    return router
