"""
coder_agent/tools/sandbox_mcp_client.py
=========================================
Unified async MCP client for the sandbox MCP server.
Implements BaseSandboxClient and provides both context-manager and lazy-connect usage.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Tuple

from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from mcp.types import CallToolResult

from sandbox_client import BaseSandboxClient

logger = logging.getLogger("coder.sandbox_mcp_client")

SANDBOX_MCP_URL = os.environ.get("SANDBOX_MCP_URL", "http://localhost:8081/sse")
MCP_TASK_TTL_MS = int(os.environ.get("MCP_TASK_TTL_MS", str(3_600_000)))
SANDBOX_MCP_AUTH_KEY = os.environ.get("SANDBOX_MCP_AUTH_KEY") or None

POLL_INTERVAL = 2
POLL_TIMEOUT = int(os.environ.get("MCP_POLL_TIMEOUT", "900"))

_TERMINAL = {"completed", "failed", "cancelled"}


class SandboxMCPClient(BaseSandboxClient):
    """MCP client for the sandbox MCP server. Implements BaseSandboxClient.

    Supports both context-manager usage (async with) and lazy-connect usage
    (for LangGraph agent nodes that share a client across invocations).

    Usage (context manager):
        async with SandboxMCPClient(url) as client:
            await client.write_file("/tmp/test.py", "print(42)")
            result = await client.exec_command("python3 /tmp/test.py")

    Usage (lazy connect, for LangGraph state):
        client = SandboxMCPClient(url)
        await client.write_file("/tmp/test.py", "print(42)")  # auto-connects
        await client.close()
    """

    def __init__(self, mcp_url: str | None = None):
        self._url = mcp_url or SANDBOX_MCP_URL
        self._session: ClientSession | None = None
        self._read = None
        self._write = None
        self._sse_ctx = None
        self._session_ctx = None
        self._connected = False

    async def _ensure_connected(self):
        if self._connected:
            return
        headers = {"Authorization": f"Bearer {SANDBOX_MCP_AUTH_KEY}"} if SANDBOX_MCP_AUTH_KEY else None
        self._sse_ctx = sse_client(self._url, headers=headers)
        self._read, self._write = await self._sse_ctx.__aenter__()
        self._session_ctx = ClientSession(self._read, self._write)
        self._session = await self._session_ctx.__aenter__()
        await self._session.initialize()
        self._connected = True
        logger.info(f"[SandboxMCPClient] Connected to {self._url}")

    async def close(self):
        if self._session_ctx:
            await self._session_ctx.__aexit__(None, None, None)
            self._session_ctx = None
        if self._sse_ctx:
            await self._sse_ctx.__aexit__(None, None, None)
            self._sse_ctx = None
        self._session = None
        self._connected = False

    async def __aenter__(self) -> "SandboxMCPClient":
        await self._ensure_connected()
        return self

    async def __aexit__(self, *args):
        await self.close()

    # ── BaseSandboxClient interface ──────────────────────────────────────

    async def write_file(self, path: str, content: str,
                         session_id: str = "", experiment_name: str = "",
                         node_id: int = None) -> bool:
        await self._ensure_connected()
        args = {"path": path, "content": content}
        if session_id:
            args["session_id"] = session_id
        if experiment_name:
            args["experiment_name"] = experiment_name
        if node_id is not None:
            args["node_id"] = node_id
        result = await self._session.call_tool("write_file", args)
        parsed = _parse_tool_result(result)
        return parsed.get("success", False)

    async def read_file(self, path: str) -> str:
        await self._ensure_connected()
        result = await self._session.call_tool("read_file", {"path": path})
        parsed = _parse_tool_result(result)
        return parsed.get("content", "")

    async def exec_shell(self, command: str, cwd: str = "/home/gem/workspace", new_session: bool = True,
                         session_id: str = "", experiment_name: str = "",
                         node_id: int = None) -> Tuple[bool, str, str]:
        await self._ensure_connected()
        full_command = f"mkdir -p {cwd} && cd {cwd} && {command}" if cwd else command
        result = await self.exec_command(
            full_command, session_id=session_id, experiment_name=experiment_name, node_id=node_id
        )
        exit_code = result.get("exit_code", -1)
        stdout = result.get("stdout", "")
        stderr = result.get("stderr", "")
        if exit_code == 0:
            return True, stdout, stderr
        return False, stdout, stderr or f"Command exited with code {exit_code}"

    async def kill_iteration_processes(self, node_id: int = None, iteration: int = None) -> dict:
        await self._ensure_connected()
        args = {}
        if node_id is not None:
            args["node_id"] = node_id
        if iteration is not None:
            args["iteration"] = iteration
        result = await self._session.call_tool("kill_processes", args)
        return _parse_tool_result(result)

    # ── MCP-native methods (used by dispatch/etc.) ───────────────────────

    async def exec_command(self, command: str, timeout: int | None = None,
                           session_id: str = "", experiment_name: str = "",
                           node_id: int = None) -> dict:
        await self._ensure_connected()
        args = {
            "command": command,
            "delivery": "poll",
        }
        if timeout:
            args["timeout"] = timeout
        if session_id:
            args["session_id"] = session_id
        if experiment_name:
            args["experiment_name"] = experiment_name
        if node_id is not None:
            args["node_id"] = node_id

        try:
            create = await self._session.experimental.call_tool_as_task(
                "exec_sandbox", args, ttl=MCP_TASK_TTL_MS)
            task_id = create.task.taskId
            logger.info(f"[exec_command] task_id={task_id} command={command[:80]}")
        except Exception as e:
            logger.error(f"[exec_command] Failed to create task for command '{command[:80]}': {e}")
            # Try falling back to standard call_tool if task creation fails
            fallback = await self._session.call_tool("exec_sandbox", args)
            return _parse_tool_result(fallback)

        result = await self._poll_task_result(task_id)
        return _parse_tool_result(result)

    async def exec_command_async(
        self,
        command: str,
        correlation_id: str,
        callback_url: str,
        resume_url: str = "",
        session_id: str = "",
        experiment_name: str = "",
        node_id: int = None,
        timeout: int | None = None,
    ) -> str:
        await self._ensure_connected()
        args = {
            "command": command,
            "delivery": "callback",
            "callback_url": callback_url,
            "correlation_id": correlation_id,
            "resume_url": resume_url,
        }
        if session_id:
            args["session_id"] = session_id
        if experiment_name:
            args["experiment_name"] = experiment_name
        if node_id is not None:
            args["node_id"] = node_id
        if timeout:
            args["timeout"] = timeout

        create = await self._session.experimental.call_tool_as_task(
            "exec_sandbox", args, ttl=MCP_TASK_TTL_MS)
        task_id = create.task.taskId
        logger.info(f"[exec_command_async] task_id={task_id} corr_id={correlation_id} "
                    f"callback={callback_url[:40]}...")
        return task_id

    async def _poll_task_result(self, task_id: str, poll_timeout: int | None = None) -> dict:
        poll_timeout = poll_timeout or POLL_TIMEOUT
        start = asyncio.get_event_loop().time()
        while True:
            elapsed = asyncio.get_event_loop().time() - start
            if elapsed > poll_timeout:
                raise TimeoutError(f"Task {task_id} did not reach terminal state within {poll_timeout}s")

            try:
                task_resp = await self._session.experimental.get_task(task_id)
            except Exception:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if task_resp.status in _TERMINAL:
                break
            await asyncio.sleep(POLL_INTERVAL)

        result = await self._session.experimental.get_task_result(task_id, CallToolResult)
        logger.info(f"[_poll_task_result] task_id={task_id} terminal")
        return result


def _parse_tool_result(result) -> dict:
    try:
        for block in getattr(result, "content", []):
            if getattr(block, "type", None) == "text":
                return json.loads(block.text)
    except (json.JSONDecodeError, Exception):
        pass
    return {"raw": str(result)}
