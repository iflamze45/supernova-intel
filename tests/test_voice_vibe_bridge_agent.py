import json
from pathlib import Path

import pytest

from scripts.voice_vibe_bridge_agent import (
    BridgeAgent,
    apply_rules,
    execute_payload,
)


class FakeClient:
    def __init__(self, commands):
        self.commands = commands
        self.after = None
        self.acks = []

    def poll(self, after):
        self.after = after
        return self.commands

    def acknowledge(self, payload):
        self.acks.append(payload)


def test_execute_payload_rejects_unknown_command():
    with pytest.raises(ValueError, match="unsupported"):
        execute_payload(
            {"type": "command", "command": "rm-everything"},
            runner=lambda command: command,
            conf_path=Path("/tmp/unused"),
        )


def test_execute_payload_runs_allowlisted_voice_vibe_command(tmp_path):
    seen = []

    message = execute_payload(
        {"type": "command", "command": "focus"},
        runner=lambda command: seen.append(command) or "focused",
        conf_path=tmp_path / "voice.conf",
    )

    assert seen == ["focus"]
    assert message == "focused"


def test_apply_rules_updates_only_managed_voice_settings(tmp_path):
    conf = tmp_path / "voice.conf"
    conf.write_text("PROFILE=build\nUNRELATED=keep\nVOICE_REPORT_POLICY=open\n")

    apply_rules(
        conf,
        {
            "type": "rules",
            "allowed_events": ["verify_pass", "error"],
            "speak_user_text": False,
            "policy": "strict",
        },
    )

    text = conf.read_text()
    assert "PROFILE=build" in text
    assert "UNRELATED=keep" in text
    assert "VOICE_REPORT_POLICY=strict" in text
    assert "VOICE_SPEAK_USER_TEXT=false" in text
    assert "VOICE_ALLOW_EVENTS=verify_pass,error" in text


def test_agent_persists_cursor_and_acknowledges_execution(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"enabled": True, "mode": "on"}))
    cursor_path = tmp_path / "cursor"
    client = FakeClient(
        [
            {
                "id": "command-1",
                "payload": {"type": "command", "command": "on"},
                "created_at": "2026-06-05T12:00:00+00:00",
                "expires_at": "2026-06-05T12:01:30+00:00",
            }
        ]
    )
    seen = []
    agent = BridgeAgent(
        client=client,
        cursor_path=cursor_path,
        state_path=state_path,
        conf_path=tmp_path / "voice.conf",
        runner=lambda command: seen.append(command) or "Voice Vibe: ON",
    )

    count = agent.run_once()

    assert count == 1
    assert seen == ["on"]
    assert cursor_path.read_text() == "2026-06-05T12:00:00+00:00"
    assert client.acks == [
        {
            "command_id": "command-1",
            "ok": True,
            "message": "Voice Vibe: ON",
            "state": {"enabled": True, "mode": "on"},
        }
    ]


def test_agent_acknowledges_failure_without_crashing(tmp_path):
    client = FakeClient(
        [
            {
                "id": "command-2",
                "payload": {"type": "command", "command": "off"},
                "created_at": "2026-06-05T12:02:00+00:00",
                "expires_at": "2026-06-05T12:03:30+00:00",
            }
        ]
    )
    agent = BridgeAgent(
        client=client,
        cursor_path=tmp_path / "cursor",
        state_path=tmp_path / "missing-state.json",
        conf_path=tmp_path / "voice.conf",
        runner=lambda _command: (_ for _ in ()).throw(RuntimeError("voice failed")),
    )

    assert agent.run_once() == 1
    assert client.acks[0]["command_id"] == "command-2"
    assert client.acks[0]["ok"] is False
    assert client.acks[0]["message"] == "voice failed"
