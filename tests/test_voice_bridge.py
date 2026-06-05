from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from core.voice_bridge import (
    FileBridgeStore,
    build_ack_record,
    build_command_record,
    decode_pending_commands,
    normalize_rules,
    require_bridge_token,
)


def test_require_bridge_token_fails_closed_when_secret_missing():
    with pytest.raises(HTTPException) as exc:
        require_bridge_token("Bearer anything", "")

    assert exc.value.status_code == 503


def test_require_bridge_token_rejects_invalid_bearer_token():
    with pytest.raises(HTTPException) as exc:
        require_bridge_token("Bearer wrong", "correct")

    assert exc.value.status_code == 401


def test_require_bridge_token_accepts_matching_bearer_token():
    require_bridge_token("Bearer correct", "correct")


def test_build_command_record_rejects_unknown_command():
    with pytest.raises(HTTPException) as exc:
        build_command_record({"type": "command", "command": "run-arbitrary-shell"})

    assert exc.value.status_code == 400


def test_build_command_record_serializes_allowlisted_command_with_expiry():
    now = datetime(2026, 6, 5, 12, 0, tzinfo=timezone.utc)

    record = build_command_record(
        {"type": "command", "command": "focus"},
        now=now,
        ttl_seconds=90,
    )

    assert record["module_name"] == "voice_vibe_bridge_command"
    assert record["signal_value"] == 1.0
    assert record["timestamp"] == now.isoformat()
    assert '"command":"focus"' in record["signal_type"]
    assert '"expires_at":"2026-06-05T12:01:30+00:00"' in record["signal_type"]


def test_normalize_rules_bounds_and_deduplicates_events():
    rules = normalize_rules(
        {
            "type": "rules",
            "allowed_events": [" Verify Pass ", "verify_pass", "ERROR"],
            "speak_user_text": False,
            "policy": "strict",
        }
    )

    assert rules == {
        "type": "rules",
        "allowed_events": ["verify_pass", "error"],
        "speak_user_text": False,
        "policy": "strict",
    }


def test_decode_pending_commands_filters_expired_and_malformed_rows():
    now = datetime(2026, 6, 5, 12, 0, tzinfo=timezone.utc)
    valid = build_command_record(
        {"type": "command", "command": "on"},
        now=now,
        ttl_seconds=120,
        command_id="valid-id",
    )
    expired = build_command_record(
        {"type": "command", "command": "off"},
        now=now - timedelta(minutes=10),
        ttl_seconds=30,
        command_id="expired-id",
    )
    malformed = {
        "id": "bad-id",
        "signal_type": "not-json",
        "timestamp": now.isoformat(),
    }

    pending = decode_pending_commands([expired, malformed, valid], now=now)

    assert [item["id"] for item in pending] == ["valid-id"]
    assert pending[0]["payload"]["command"] == "on"


def test_build_ack_record_truncates_message_and_preserves_state():
    now = datetime(2026, 6, 5, 12, 0, tzinfo=timezone.utc)

    record = build_ack_record(
        command_id="abc",
        ok=True,
        message="x" * 900,
        state={"enabled": True, "mode": "on"},
        now=now,
    )

    assert record["module_name"] == "voice_vibe_bridge_ack"
    assert record["timestamp"] == now.isoformat()
    assert len(record["signal_type"]) < 1400
    assert '"command_id":"abc"' in record["signal_type"]
    assert '"enabled":true' in record["signal_type"]


@pytest.mark.asyncio
async def test_file_store_round_trip(tmp_path):
    store = FileBridgeStore(tmp_path / "bridge.json")
    command = build_command_record(
        {"type": "command", "command": "silent"},
        command_id="command-1",
    )
    ack = build_ack_record(
        command_id="command-1",
        ok=True,
        message="done",
        state={"enabled": True, "mode": "silent"},
    )

    await store.enqueue(command)
    pending = await store.poll(after=None)
    await store.acknowledge(ack)
    latest = await store.latest_state()

    assert pending[0]["id"] == "command-1"
    assert latest["command_id"] == "command-1"
    assert latest["state"]["mode"] == "silent"
