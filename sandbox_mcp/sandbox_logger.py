"""
sandbox_mcp/sandbox_logger.py
==============================
File-based structured logger for the sandbox MCP server.
Writes per-node execution logs, container stats, and MCP-level events
to a local directory on the EC2 host.

Directory structure:
    /home/ubuntu/haseeb/logs/{experiment_name}/{timestamp}_{session_id}/
        sandbox_mcp_events.jsonl
        node_{node_id}/
            metadata.json
            stdout.txt
            stderr.txt
            exit_code.json
            container_stats.jsonl
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("sandbox_mcp.sandbox_logger")

try:
    _sandbox_mcp_dir = Path(__file__).resolve().parent
    _project_root = _sandbox_mcp_dir.parent
except Exception:
    _project_root = Path.cwd()
LOG_ROOT = (_project_root / "logs").resolve()


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


class SandboxLogger:
    def __init__(
        self,
        experiment_name: str,
        session_id: str,
        node_id: Optional[int] = None,
    ):
        self.experiment_name = experiment_name or "unknown_experiment"
        self.session_id = session_id or "unknown_session"
        self.node_id = node_id

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.session_dir = LOG_ROOT / self.session_id / self.experiment_name / ts
        self.node_dir = (
            self.session_dir / f"node_{self.node_id}" if self.node_id is not None else self.session_dir
        )
        self.events_path = self.session_dir / "sandbox_mcp_events.jsonl"
        self._lock = __import__("threading").RLock()

        self._ctx = {
            "experiment_name": self.experiment_name,
            "session_id": self.session_id,
            "node_id": self.node_id,
        }

    def _ensure_session_dir(self):
        self.session_dir.mkdir(parents=True, exist_ok=True)

    def _ensure_node_dir(self):
        if self.node_dir is None:
            return
        self.node_dir.mkdir(parents=True, exist_ok=True)

    # ── MCP-level events ──────────────────────────────────────────────────

    def log_event(self, event_type: str, **extra):
        self._ensure_session_dir()
        entry = {
            "timestamp": _utc_timestamp(),
            "event_type": event_type,
            **self._ctx,
            **extra,
        }
        with self._lock:
            try:
                with open(self.events_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
            except Exception:
                logger.exception("Failed to write sandbox_mcp event")

    # ── Per-node execution logs ───────────────────────────────────────────

    def log_exec_start(self, command: str, cwd: str = ""):
        self._ensure_node_dir()
        metadata = {
            "command": command,
            "cwd": cwd,
            "start_time": _utc_timestamp(),
            "start_epoch": time.time(),
            **self._ctx,
        }
        self._write_json("metadata.json", metadata)
        self.log_event("exec_start", command=command[:200], cwd=cwd)

    def log_exec_end(self, stdout: str, stderr: str, exit_code: int, duration_s: float):
        self._ensure_node_dir()
        self._write_text("stdout.txt", stdout)
        self._write_text("stderr.txt", stderr)
        self._write_json("exit_code.json", {"exit_code": exit_code, "duration_s": round(duration_s, 3)})

        with self._lock:
            p = self.node_dir / "metadata.json"
            try:
                meta = json.loads(p.read_text())
            except Exception:
                meta = {}
            meta["end_time"] = _utc_timestamp()
            meta["duration_s"] = round(duration_s, 3)
            meta["exit_code"] = exit_code
            self._write_json("metadata.json", meta)

        self.log_event(
            "exec_end",
            exit_code=exit_code,
            duration_s=round(duration_s, 3),
            stdout_bytes=len(stdout),
            stderr_bytes=len(stderr),
        )

    # ── Container stats ───────────────────────────────────────────────────

    def log_container_stats(self, stats: dict):
        self._ensure_node_dir()
        entry = {
            "timestamp": _utc_timestamp(),
            "epoch": time.time(),
            **stats,
        }
        with self._lock:
            p = self.node_dir / "container_stats.jsonl"
            try:
                with open(p, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
            except Exception:
                logger.exception("Failed to write container stats")

    # ── Tool call events ──────────────────────────────────────────────────

    def log_tool_call(self, tool_name: str, args_summary: str, duration_ms: float, success: bool, error: str = ""):
        self.log_event(
            "tool_call",
            tool_name=tool_name,
            args=args_summary,
            duration_ms=round(duration_ms, 2),
            success=success,
            error=error,
        )

    # ── Helpers ───────────────────────────────────────────────────────────

    def _write_text(self, filename: str, content: str):
        with self._lock:
            try:
                (self.node_dir / filename).write_text(content, encoding="utf-8")
            except Exception:
                logger.exception(f"Failed to write {filename}")

    def _write_json(self, filename: str, data):
        with self._lock:
            try:
                (self.node_dir / filename).write_text(
                    json.dumps(data, indent=2, default=str), encoding="utf-8"
                )
            except Exception:
                logger.exception(f"Failed to write {filename}")
