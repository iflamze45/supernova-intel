#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable


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
MANAGED_RULE_KEYS = {
    "VOICE_ALLOW_EVENTS",
    "VOICE_SPEAK_USER_TEXT",
    "VOICE_REPORT_POLICY",
}


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def apply_rules(conf_path: Path, payload: dict[str, Any]) -> None:
    events = []
    for raw in payload.get("allowed_events", []):
        event = str(raw).strip().lower().replace(" ", "_")
        if event and event not in events:
            events.append(event)
    policy = payload.get("policy")
    if policy not in {"strict", "open"}:
        raise ValueError("unsupported voice policy")
    updates = {
        "VOICE_ALLOW_EVENTS": ",".join(events),
        "VOICE_SPEAK_USER_TEXT": "true" if payload.get("speak_user_text") else "false",
        "VOICE_REPORT_POLICY": policy,
    }

    conf_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    seen: set[str] = set()
    if conf_path.exists():
        for raw in conf_path.read_text(errors="ignore").splitlines():
            stripped = raw.strip()
            if stripped and not stripped.startswith("#") and "=" in raw:
                key = raw.split("=", 1)[0].strip()
                if key in MANAGED_RULE_KEYS:
                    lines.append(f"{key}={updates[key]}")
                    seen.add(key)
                    continue
            lines.append(raw)
    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")
    conf_path.write_text("\n".join(lines).rstrip() + "\n")


def default_runner(command: str) -> str:
    executable = Path.home() / ".local/bin/voice-vibe"
    completed = subprocess.run(
        [str(executable), command],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip() or "voice-vibe failed")
    return completed.stdout.strip()


def execute_payload(
    payload: dict[str, Any],
    *,
    runner: Callable[[str], str],
    conf_path: Path,
) -> str:
    payload_type = payload.get("type")
    if payload_type == "rules":
        apply_rules(conf_path, payload)
        status = runner("status")
        return status or "Voice Vibe rules updated"
    if payload_type != "command":
        raise ValueError("unsupported bridge payload")
    command = payload.get("command")
    if command not in ALLOWED_COMMANDS:
        raise ValueError("unsupported voice-vibe command")
    return runner(str(command))


class BridgeClient:
    def __init__(self, base_url: str, token: str, timeout: float = 12.0):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = None
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")[:512]
            raise RuntimeError(f"bridge HTTP {exc.code}: {detail}") from exc

    def poll(self, after: str | None) -> list[dict[str, Any]]:
        query = ""
        if after:
            query = "?" + urllib.parse.urlencode({"after": after})
        response = self._request("GET", f"/voice-bridge/poll{query}")
        return list(response.get("commands", []))

    def acknowledge(self, payload: dict[str, Any]) -> None:
        self._request("POST", "/voice-bridge/ack", payload)


class BridgeAgent:
    def __init__(
        self,
        *,
        client: Any,
        cursor_path: Path,
        state_path: Path,
        conf_path: Path,
        runner: Callable[[str], str] = default_runner,
    ):
        self.client = client
        self.cursor_path = cursor_path
        self.state_path = state_path
        self.conf_path = conf_path
        self.runner = runner

    def _read_cursor(self) -> str | None:
        if not self.cursor_path.exists():
            return None
        value = self.cursor_path.read_text(errors="ignore").strip()
        return value or None

    def _write_cursor(self, value: str) -> None:
        self.cursor_path.parent.mkdir(parents=True, exist_ok=True)
        self.cursor_path.write_text(value)

    def _read_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            value = json.loads(self.state_path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def run_once(self) -> int:
        commands = self.client.poll(self._read_cursor())
        for command in commands:
            ok = True
            try:
                message = execute_payload(
                    command["payload"],
                    runner=self.runner,
                    conf_path=self.conf_path,
                )
            except Exception as exc:
                ok = False
                message = str(exc)[:512]
            self.client.acknowledge(
                {
                    "command_id": command["id"],
                    "ok": ok,
                    "message": message,
                    "state": self._read_state(),
                }
            )
            self._write_cursor(command["created_at"])
        return len(commands)


def main() -> int:
    parser = argparse.ArgumentParser(description="Outbound Voice Vibe bridge agent")
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--env-file",
        default=str(Path.home() / ".config/supernova/voice-vibe-bridge.env"),
    )
    args = parser.parse_args()

    env = {**load_env_file(Path(args.env_file)), **os.environ}
    base_url = env.get("VOICE_VIBE_BRIDGE_URL", "").strip()
    token = env.get("VOICE_VIBE_BRIDGE_TOKEN", "").strip()
    if not base_url or not token:
        raise SystemExit("VOICE_VIBE_BRIDGE_URL and VOICE_VIBE_BRIDGE_TOKEN are required")

    agent = BridgeAgent(
        client=BridgeClient(base_url, token),
        cursor_path=Path.home() / ".config/supernova/voice-vibe-bridge.cursor",
        state_path=Path("/tmp/.claude_voice_state.json"),
        conf_path=Path.home() / ".claude/voice/voice.conf",
    )
    interval = min(max(int(env.get("VOICE_VIBE_BRIDGE_POLL_SECONDS", "5")), 2), 60)
    if args.once:
        agent.run_once()
        return 0

    while True:
        try:
            agent.run_once()
        except Exception as exc:
            print(f"voice-vibe-bridge: {exc}", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
