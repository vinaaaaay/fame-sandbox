"""
sandbox_mcp/main.py  --  Sandbox MCP Server on EC2 (persistent), co-located with the container
==============================================================================================

Deployment: runs as a long-lived process on the SAME EC2 box as the AIO sandbox container.
Communicates with the container via its REST APIs (localhost:8080).

Tools exposed:
  - exec_sandbox   (task-based)   -- runs a bash command in the container via /v1/bash (Bash Pipe API)
  - write_file     (sync)         -- writes a file in the container via /v1/file/write
  - read_file      (sync)         -- reads a file in the container via /v1/file/read
  - kill_processes (sync)         -- kills processes for a given node/iteration

Two delivery modes for exec_sandbox:
  - delivery="callback"  -> runs command with async_mode, polls until complete, SigV4-POSTs result to GW
  - delivery="poll"      -> runs command with async_mode, polls until complete, stores result in task store

Pin: mcp>=1.23,<2  (experimental tasks; removed in 2.0, returns as an extension).
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Protocol

import anyio
import httpx

from mcp.server import Server
from mcp.server.experimental.task_context import ServerTaskContext
from mcp.types import (
    CallToolResult,
    CreateTaskResult,
    GetTaskRequest,
    GetTaskResult,
    GetTaskPayloadRequest,
    GetTaskPayloadResult,
    TextContent,
    Tool,
    ToolExecution,
    TASK_REQUIRED,
    ErrorData,
    INVALID_PARAMS,
)
from mcp.shared.exceptions import McpError
from mcp.shared.experimental.tasks.in_memory_task_store import InMemoryTaskStore

from callback_delivery import deliver_callback, SILENCE_AUTOCOMPLETE_NOISE
from sandbox_logger import SandboxLogger, LOG_ROOT

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / ".env")

logger = logging.getLogger("sandbox_mcp")

EXEC_TOOL_NAME = "exec_sandbox"
CONTAINER_URL = os.environ.get("CONTAINER_URL", "http://localhost:8080")
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
COMPOSE_DIR = os.environ.get("SANDBOX_COMPOSE_DIR", os.path.dirname(_THIS_DIR))
DOCKER_CMD = os.environ.get("DOCKER_CMD", "docker").split()
AUTH_KEY = os.environ.get("SANDBOX_MCP_AUTH_KEY") or None
TERMINAL = {"completed", "failed", "cancelled"}


def _container_post(path: str, payload: dict, timeout: int = 900) -> dict:
    """Synchronous POST to the sandbox container REST API.
    Called from within anyio.to_thread.run_sync — blocking is fine."""
    url = f"{CONTAINER_URL}{path}"
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.warning(f"[container] HTTP {e.response.status_code} from {url}: {e.response.text[:500]}")
        return {"success": False, "message": str(e), "data": {}}
    except Exception as e:
        logger.error(f"[container] Request failed {url}: {e}")
        return {"success": False, "message": str(e), "data": {}}


def _write_file_on_sandbox(path: str, content: str) -> dict:
    """Write a file inside the container via /v1/file/write."""
    return _container_post("/v1/file/write", {
        "file": path,
        "content": content,
        "encoding": "utf-8",
    })


def _read_file_on_sandbox(path: str) -> dict:
    """Read a file inside the container via /v1/file/read."""
    return _container_post("/v1/file/read", {"file": path})


def _run_command_on_sandbox(arguments: dict) -> dict:
    """Execute a bash command in the container via /v1/bash (Bash Pipe API).
    Returns separate stdout/stderr, supports long-running commands with
    offset-based incremental reads — no output lost at timeout.
    Called from within anyio.to_thread.run_sync — blocking is fine."""
    command = arguments.get("command", "echo 'no command'")
    user_timeout = int(arguments.get("timeout", 1200))
    cwd = arguments.get("cwd", "")
    if cwd:
        command = f"mkdir -p {cwd} && cd {cwd} && {command}"

    experiment_name = arguments.get("experiment_name", "unknown_experiment")
    session_id = arguments.get("session_id", "")
    node_id = arguments.get("node_id")

    exec_logger = SandboxLogger(
        experiment_name=experiment_name,
        session_id=session_id or f"nosession_{uuid.uuid4().hex[:8]}",
        node_id=node_id,
    )
    try:
        exec_logger.log_exec_start(command=command, cwd=cwd)
    except Exception:
        logger.exception("log_exec_start failed, proceeding without logging")

    # ── Container stats sampler ──────────────────────────────────
    container_name = os.environ.get("CONTAINER_NAME", "")
    stats_thread = None
    stats_stop = threading.Event()

    if container_name:
        def _sample_docker_stats():
            import subprocess as _sp
            while not stats_stop.is_set():
                try:
                    result = _sp.run(
                        [*DOCKER_CMD, "stats", container_name, "--no-stream", "--format", "json"],
                        capture_output=True, text=True, timeout=10,
                    )
                    if result.stdout.strip():
                        stats_data = json.loads(result.stdout.strip())
                        exec_logger.log_container_stats(stats_data)
                except Exception:
                    pass
                stats_stop.wait(5)

        stats_thread = threading.Thread(target=_sample_docker_stats, daemon=True)
        stats_thread.start()

    t0 = time.time()
    result = {}

    exec_url = f"{CONTAINER_URL}/v1/bash/exec"
    output_url = f"{CONTAINER_URL}/v1/bash/output"
    bash_soft_timeout = min(user_timeout, 5)
    hard_timeout = user_timeout + 5
    poll_deadline = user_timeout
    accumulated_stdout = ""
    accumulated_stderr = ""

    try:
        with httpx.Client(timeout=bash_soft_timeout + 30) as client:
            resp = client.post(exec_url, json={
                "command": command,
                "timeout": bash_soft_timeout,
                "hard_timeout": hard_timeout,
            })
            resp.raise_for_status()
            data = resp.json()

        if not data.get("success"):
            result = {
                "stdout": "",
                "stderr": data.get("message", "bash exec failed"),
                "exit_code": -1,
            }
        else:
            cmd_data = data.get("data", {})
            status = cmd_data.get("status", "")
            bash_session_id = cmd_data.get("session_id", "")

            if status == "completed":
                result = {
                    "stdout": cmd_data.get("stdout", "") or "",
                    "stderr": cmd_data.get("stderr", "") or "",
                    "exit_code": cmd_data.get("exit_code", -1),
                }
            elif status == "running":
                accumulated_stdout = cmd_data.get("stdout", "") or ""
                accumulated_stderr = cmd_data.get("stderr", "") or ""
                offset = cmd_data.get("offset", len(accumulated_stdout))
                stderr_offset = cmd_data.get("stderr_offset", len(accumulated_stderr))

                while True:
                    elapsed = time.time() - t0
                    if elapsed > poll_deadline:
                        break

                    remaining = poll_deadline - elapsed
                    wait_timeout = min(remaining, 10)

                    try:
                        with httpx.Client(timeout=wait_timeout + 15) as client:
                            out_resp = client.post(output_url, json={
                                "session_id": bash_session_id,
                                "offset": offset,
                                "stderr_offset": stderr_offset,
                                "wait": True,
                                "wait_timeout": wait_timeout,
                            })
                            out_resp.raise_for_status()
                            out_data = out_resp.json()
                    except Exception:
                        break

                    out_d = out_data.get("data", {})
                    out_stdout = out_d.get("stdout") or ""
                    out_stderr = out_d.get("stderr") or ""
                    if out_stdout:
                        accumulated_stdout += out_stdout
                        offset += len(out_stdout)
                    if out_stderr:
                        accumulated_stderr += out_stderr
                        stderr_offset += len(out_stderr)

                    new_status = out_d.get("status", "running")
                    if new_status != "running":
                        exit_code = out_d.get("exit_code", -1) if new_status == "completed" else 124
                        stderr_msg = accumulated_stderr
                        if new_status == "timed_out":
                            if stderr_msg:
                                stderr_msg += "\n"
                            stderr_msg += f"Command timed out after {user_timeout} seconds."
                        result = {
                            "stdout": accumulated_stdout,
                            "stderr": stderr_msg,
                            "exit_code": exit_code,
                        }
                        break

                if not result:
                    result = {
                        "stdout": accumulated_stdout,
                        "stderr": (accumulated_stderr + "\n" if accumulated_stderr else "") + f"Command timed out after {user_timeout} seconds.",
                        "exit_code": 124,
                    }
            else:
                result = {
                    "stdout": cmd_data.get("stdout", "") or "",
                    "stderr": cmd_data.get("stderr", "") or "",
                    "exit_code": -1,
                }

    except httpx.HTTPStatusError as e:
        result = {
            "stdout": "",
            "stderr": f"Container HTTP {e.response.status_code}: {e.response.text[:500]}",
            "exit_code": -1,
        }
    except Exception as e:
        result = {"stdout": "", "stderr": str(e), "exit_code": -1}
    finally:
        if stats_thread:
            stats_stop.set()

    duration = time.time() - t0
    exec_logger.log_exec_end(
        stdout=result.get("stdout", ""),
        stderr=result.get("stderr", ""),
        exit_code=result.get("exit_code", -1),
        duration_s=duration,
    )

    return result


def _kill_processes_on_sandbox(node_id, iteration) -> dict:
    """Kill processes associated with a node/iteration in the sandbox container."""
    kill_pattern = f"node_{node_id}" if node_id is not None else f"iteration_{iteration}"
    command = (
        f"pkill -f '{kill_pattern}' --exclude-pid $$ 2>/dev/null; "
        f"echo KILL_DONE_{kill_pattern}"
    )
    exec_url = f"{CONTAINER_URL}/v1/bash/exec"
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(exec_url, json={"command": command})
            resp.raise_for_status()
            result = resp.json()
            data = result.get("data", {})
            out = data.get("stdout", "")
            err = data.get("stderr", "")
            output_lines = (out or "") + ("\n" + err if err else "")
            return {"output": output_lines.strip(), "pattern": kill_pattern}
    except Exception as e:
        return {"output": str(e), "pattern": kill_pattern, "error": True}


def _do_reset_sandbox() -> dict:
    result = subprocess.run(
        [*DOCKER_CMD, "compose", "down"],
        cwd=COMPOSE_DIR,
        capture_output=True, text=True, timeout=120,
    )
    logger.info(f"[reset] docker compose down rc={result.returncode}")
    if result.returncode != 0:
        logger.warning(f"[reset] compose down stderr: {result.stderr.strip()[:500]}")

    result = subprocess.run(
        [*DOCKER_CMD, "compose", "up", "-d"],
        cwd=COMPOSE_DIR,
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        logger.error(f"[reset] compose up stderr: {result.stderr.strip()[:500]}")
        return {"success": False, "message": f"compose up failed: {result.stderr[:300]}"}
    logger.info("[reset] docker compose up succeeded")

    health_url = f"{CONTAINER_URL}/v1/shell/exec"
    probe_cmd = "/home/gem/workspace/.venv/bin/python -c 'print(\"ready\")'"
    max_wait = 120
    start = time.time()
    while time.time() - start < max_wait:
        try:
            with httpx.Client(timeout=15) as client:
                resp = client.post(health_url, json={"command": probe_cmd})
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success") and "ready" in data.get("data", {}).get("output", ""):
                        logger.info(f"[reset] container healthy after {time.time() - start:.1f}s")
                        break
        except Exception:
            pass
        time.sleep(2)
    else:
        return {"success": True, "message": "container up but health check timed out in 120s"}

    def _prune():
        try:
            logger.info("[reset] background prune starting...")
            result = subprocess.run(
                [*DOCKER_CMD, "system", "prune", "-a", "--volumes", "--force"],
                capture_output=True, text=True, timeout=300,
            )
            logger.info(f"[reset] prune finished: {result.stdout.strip()[-500:]}")
        except Exception as exc:
            logger.error(f"[reset] prune error: {exc}")

    threading.Thread(target=_prune, daemon=True).start()
    return {"success": True, "message": "sandbox reset complete"}


async def _exec_sandbox_fallback(arguments: dict) -> CallToolResult:
    """Fallback for clients that don't support experimental tasks.
    Runs directly without TASK_REQUIRED validation."""
    mode = arguments.get("delivery", "callback")
    callback_url = arguments.get("callback_url")
    correlation_id = arguments.get("correlation_id")
    resume_url = arguments.get("resume_url", "")

    t0 = time.time()
    payload = await anyio.to_thread.run_sync(_run_command_on_sandbox, arguments)
    duration_ms = (time.time() - t0) * 1000

    is_error = payload.get("exit_code", 0) != 0
    result = CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload))],
        isError=is_error,
    )

    exec_session_id = arguments.get("session_id", "")
    el = SandboxLogger(
        experiment_name=arguments.get("experiment_name", "unknown_experiment"),
        session_id=exec_session_id or "nosession",
        node_id=arguments.get("node_id"),
    )
    el.log_tool_call(
        tool_name="exec_sandbox",
        args_summary=f"cmd={arguments.get('command', '')[:200]}",
        duration_ms=duration_ms,
        success=not is_error,
        error=payload.get("stderr", "") if is_error else "",
    )

    if mode == "callback":
        await deliver_callback(
            callback_url,
            task_id=correlation_id or f"notask_{uuid.uuid4().hex[:8]}",
            correlation_id=correlation_id,
            result=result,
            resume_url=resume_url,
        )

    return result


def build_server(store: InMemoryTaskStore | None = None) -> Server:
    server = Server("coder-sandbox-mcp")
    store = store or InMemoryTaskStore()

    server.experimental.enable_tasks(store=store)

    @server.experimental.get_task()
    async def _get_task(req: GetTaskRequest) -> GetTaskResult:
        task = await store.get_task(req.params.taskId)
        if task is None:
            raise McpError(ErrorData(code=INVALID_PARAMS, message="task not found"))
        return GetTaskResult(**task.model_dump())

    @server.experimental.get_task_result()
    async def _get_task_result(req: GetTaskPayloadRequest) -> GetTaskPayloadResult:
        task_id = req.params.taskId
        while True:
            task = await store.get_task(task_id)
            if task is None:
                raise McpError(ErrorData(code=INVALID_PARAMS, message="task not found"))
            if task.status in TERMINAL:
                break
            await store.wait_for_update(task_id)
        result = await store.get_result(task_id)
        if result is None:
            raise McpError(ErrorData(code=INVALID_PARAMS, message="task failed or no result stored"))
        payload = GetTaskPayloadResult.model_validate(result.model_dump())
        payload.meta = {"io.modelcontextprotocol/related-task": {"taskId": task_id}}
        return payload

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name=EXEC_TOOL_NAME,
                description="Execute a bash command in the sandbox container and return stdout/stderr/exit_code.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "command": {"type": "string", "description": "The bash command to execute"},
                        "delivery": {"type": "string", "enum": ["callback", "poll"]},
                        "callback_url": {"type": "string"},
                        "correlation_id": {"type": "string"},
                        "resume_url": {"type": "string"},
                        "cwd": {"type": "string", "description": "Working directory inside the container"},
                        "timeout": {"type": "integer", "description": "Max execution time in seconds (default 3600, max 3600)"},
                        "session_id": {"type": "string", "description": "Orchestration session ID for logging"},
                        "experiment_name": {"type": "string", "description": "Experiment name for log grouping"},
                        "node_id": {"type": "integer", "description": "MCTS node ID for log grouping"},
                    },
                    "required": ["command"],
                },
                execution=ToolExecution(taskSupport=TASK_REQUIRED),
            ),
            Tool(
                name="write_file",
                description="Write a file inside the sandbox container.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Absolute path inside the container"},
                        "content": {"type": "string", "description": "File content to write"},
                        "session_id": {"type": "string", "description": "Orchestration session ID for logging"},
                        "experiment_name": {"type": "string", "description": "Experiment name for log grouping"},
                        "node_id": {"type": "integer", "description": "MCTS node ID for log grouping"},
                    },
                    "required": ["path", "content"],
                },
            ),
            Tool(
                name="read_file",
                description="Read a file from the sandbox container.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Absolute path inside the container"},
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="kill_processes",
                description="Kill sandbox processes for a given node_id or iteration.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "node_id": {"type": "integer"},
                        "iteration": {"type": "integer"},
                    },
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> CallToolResult | CreateTaskResult:
        if name == "write_file":
            path = arguments.get("path", "")
            content = arguments.get("content", "")
            session_id = arguments.get("session_id", "")
            experiment_name = arguments.get("experiment_name", "")
            node_id = arguments.get("node_id")

            if not path:
                return CallToolResult(
                    content=[TextContent(type="text", text=json.dumps({"success": False, "message": "path is required"}))],
                    isError=True)

            t0 = time.time()
            result = await anyio.to_thread.run_sync(_write_file_on_sandbox, path, content)
            duration_ms = (time.time() - t0) * 1000

            wl = SandboxLogger(
                experiment_name=experiment_name,
                session_id=session_id or "nosession",
                node_id=node_id,
            )
            wl.log_tool_call(
                tool_name="write_file",
                args_summary=f"path={path} size={len(content)}",
                duration_ms=duration_ms,
                success=result.get("success", False),
                error=result.get("message", ""),
            )

            return CallToolResult(
                content=[TextContent(type="text", text=json.dumps(result))],
                isError=not result.get("success", False),
            )

        if name == "read_file":
            path = arguments.get("path", "")
            if not path:
                return CallToolResult(
                    content=[TextContent(type="text", text=json.dumps({"success": False, "message": "path is required"}))],
                    isError=True)
            result = await anyio.to_thread.run_sync(_read_file_on_sandbox, path)
            data = result.get("data", {})
            return CallToolResult(
                content=[TextContent(type="text", text=json.dumps({
                    "success": result.get("success", False),
                    "content": data.get("content", ""),
                    "file": data.get("file", path),
                }))],
                isError=not result.get("success", False),
            )

        if name == "kill_processes":
            node_id = arguments.get("node_id")
            iteration = arguments.get("iteration", 0)
            payload = await anyio.to_thread.run_sync(_kill_processes_on_sandbox, node_id, iteration)
            return CallToolResult(
                content=[TextContent(type="text", text=json.dumps(payload))],
                isError=payload.get("error", False),
            )

        if name != EXEC_TOOL_NAME:
            return CallToolResult(
                content=[TextContent(type="text", text=f"unknown tool: {name}")],
                isError=True)

        ctx = server.request_context

        try:
            ctx.experimental.validate_task_mode(TASK_REQUIRED)
        except McpError:
            return await _exec_sandbox_fallback(arguments)

        mode = arguments.get("delivery", "callback")
        callback_url = arguments.get("callback_url")
        correlation_id = arguments.get("correlation_id")
        resume_url = arguments.get("resume_url", "")

        async def work(task: ServerTaskContext) -> CallToolResult:
            with anyio.CancelScope(shield=True):
                if mode == "poll":
                    await task.update_status("executing")

                t0 = time.time()
                payload = await anyio.to_thread.run_sync(_run_command_on_sandbox, arguments)
                duration_ms = (time.time() - t0) * 1000

                if task.is_cancelled:
                    return CallToolResult(
                        content=[TextContent(type="text", text="cancelled")], isError=False)

                is_error = payload.get("exit_code", 0) != 0
                result = CallToolResult(
                    content=[TextContent(type="text", text=json.dumps(payload))],
                    isError=is_error)

                exec_session_id = arguments.get("session_id", "")
                el = SandboxLogger(
                    experiment_name=arguments.get("experiment_name", "unknown_experiment"),
                    session_id=exec_session_id or "nosession",
                    node_id=arguments.get("node_id"),
                )
                el.log_tool_call(
                    tool_name="exec_sandbox",
                    args_summary=f"cmd={arguments.get('command', '')[:200]}",
                    duration_ms=duration_ms,
                    success=not is_error,
                    error=payload.get("stderr", "") if is_error else "",
                )

                if mode == "callback":
                    await deliver_callback(
                        callback_url,
                        task_id=task.task_id,
                        correlation_id=correlation_id,
                        result=result,
                        resume_url=resume_url,
                    )

                return result

        return await ctx.experimental.run_task(work)

    return server


def create_app():
    from starlette.applications import Starlette
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import Response, JSONResponse
    from starlette.routing import Route, Mount
    from mcp.server.sse import SseServerTransport

    class _AuthMiddleware:
        def __init__(self, app):
            self.app = app
            
        async def __call__(self, scope, receive, send):
            if scope["type"] not in ("http", "websocket"):
                await self.app(scope, receive, send)
                return

            if AUTH_KEY:
                headers = dict(scope.get("headers", []))
                auth = headers.get(b"authorization", b"").decode("latin1")
                token = auth[7:] if auth.startswith("Bearer ") else ""
                if not secrets.compare_digest(token, AUTH_KEY):
                    response = JSONResponse({"error": "unauthorized"}, status_code=401)
                    await response(scope, receive, send)
                    return
            await self.app(scope, receive, send)

    SILENCE_AUTOCOMPLETE_NOISE()

    server = build_server()
    sse = SseServerTransport("/messages/")

    logging.getLogger("mcp").setLevel(logging.WARNING)

    async def handle_reset_sandbox(request):
        try:
            result = await anyio.to_thread.run_sync(_do_reset_sandbox)
        except Exception as e:
            logger.error(f"[reset] exception: {e}")
            return JSONResponse({"success": False, "message": str(e)}, status_code=500)
        return JSONResponse(result, status_code=200 if result["success"] else 500)

    async def handle_sse(request):
        async with sse.connect_sse(
            request.scope, request.receive, request._send
        ) as streams:
            await server.run(
                streams[0], streams[1], server.create_initialization_options()
            )
        return Response()

    port = int(os.environ.get("SANDBOX_MCP_PORT", "8081"))
    app = Starlette(
        debug=False,
        routes=[
            Route("/reset_sandbox", endpoint=handle_reset_sandbox, methods=["POST"]),
            Route("/sse", endpoint=handle_sse, methods=["GET"]),
            Mount("/messages", app=sse.handle_post_message),
        ],
    )
    class _LoggingMiddleware:
        def __init__(self, app):
            self.app = app

        async def __call__(self, scope, receive, send):
            try:
                await self.app(scope, receive, send)
            except Exception as e:
                logger.exception(f"Unhandled ASGI exception on {scope.get('method', '')} {scope.get('path', '')}")
                raise

    app.add_middleware(_AuthMiddleware)
    app.add_middleware(_LoggingMiddleware)
    return app, port


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO)
    app, port = create_app()
    logger.info(f"Starting coder-sandbox-mcp server on 0.0.0.0:{port} (container at {CONTAINER_URL})")
    uvicorn.run(app, host="0.0.0.0", port=port)
