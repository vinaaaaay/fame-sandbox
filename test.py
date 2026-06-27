#!/usr/bin/env python3
"""
test_terminal_mcp.py (v4) -- Step 4: timeout behavior of sandbox_execute_bash

Fires `sleep 45; echo done` with timeout=10 and inspects:
  - what `status` comes back as (completed / running / timed_out / killed / etc)
  - whether "done" appears in output (i.e. did it actually wait the full 45s
    despite timeout=10, or cut off early)
  - how long the call actually took (wall clock)
  - whether the sleep process is still alive after the call returns
  - if alive, whether it eventually finishes on its own (poll until done)
"""

import asyncio
import json
import time

import httpx

MCP_URL = "http://localhost:8080/mcp"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


async def mcp_call(client, method, params=None, req_id=1, timeout=120):
    payload = {"jsonrpc": "2.0", "method": method, "id": req_id}
    if params is not None:
        payload["params"] = params
    print(f"\n>>> {method} {json.dumps(params) if params else ''}")
    t0 = time.time()
    resp = await client.post(MCP_URL, headers=HEADERS, json=payload, timeout=timeout)
    elapsed = time.time() - t0
    resp.raise_for_status()

    ctype = resp.headers.get("content-type", "")
    if "text/event-stream" in ctype:
        data_lines = [l[5:].strip() for l in resp.text.splitlines() if l.startswith("data:")]
        result = json.loads(data_lines[-1]) if data_lines else {}
    else:
        result = resp.json()

    print(f"<<< (took {elapsed:.2f}s)", json.dumps(result, indent=2)[:1500])
    return result, elapsed


async def exec_bash(client, cmd, req_id, new_session=False, cwd=None, timeout_arg=30, http_timeout=120):
    args = {"cmd": cmd, "new_session": new_session, "timeout": timeout_arg}
    if cwd:
        args["cwd"] = cwd
    return await mcp_call(client, "tools/call", {"name": "sandbox_execute_bash", "arguments": args}, req_id=req_id, timeout=http_timeout)


def extract_text(result):
    content = result.get("result", {}).get("content", [])
    return "\n".join(b["text"] for b in content if b.get("type") == "text")


async def main():
    async with httpx.AsyncClient() as client:
        # --- Step 4a: long sleep with short tool-level timeout ---
        # Use a fresh session so it's isolated from other test runs.
        # Mark start time so the printed timestamps let us correlate
        # against the eventual "done" line.
        cmd = "echo START:$(date +%H:%M:%S); sleep 45; echo DONE:$(date +%H:%M:%S)"
        print(f"\n=== Firing long command (sleep 45) with timeout=10 ===")
        result, elapsed = await exec_bash(client, cmd, req_id=1, new_session=True, timeout_arg=10, http_timeout=120)
        text = extract_text(result)
        print("\n--- result text ---")
        print(text)
        print(f"\n--- wall clock elapsed: {elapsed:.2f}s ---")

        try:
            parsed = json.loads(text)
            status = parsed.get("status")
            output = parsed.get("output", "")
        except json.JSONDecodeError:
            status = "UNPARSEABLE"
            output = text

        print(f"\nParsed status: {status}")
        print(f"'DONE:' in output: {'DONE:' in output}")

        # --- Step 4b: is the sleep process still alive right after return? ---
        await asyncio.sleep(1)
        proc_check, _ = await exec_bash(
            client,
            "ps aux | grep 'sleep 45' | grep -v grep || echo NO_PROCESS_FOUND",
            req_id=2,
            new_session=True,
        )
        print("\n--- process check immediately after tool call returned ---")
        print(extract_text(proc_check))

        # --- Step 4c: if status != completed, poll until it either
        #     finishes naturally or the process disappears ---
        if status != "completed":
            print("\n=== status != completed, polling every 5s for up to 60s ===")
            for i in range(12):
                await asyncio.sleep(5)
                proc_check, _ = await exec_bash(
                    client,
                    "ps aux | grep 'sleep 45' | grep -v grep || echo NO_PROCESS_FOUND",
                    req_id=10 + i,
                    new_session=True,
                )
                proc_text = extract_text(proc_check)
                print(f"\n--- poll {i+1} (t+{(i+1)*5}s after first call) ---")
                print(proc_text)
                if "NO_PROCESS_FOUND" in proc_text:
                    print("Process gone.")
                    break

        # --- Step 4d: control case - same command but timeout=60 (>45) ---
        print("\n\n=== Control: same command but timeout=60 (should comfortably finish) ===")
        cmd2 = "echo START:$(date +%H:%M:%S); sleep 45; echo DONE:$(date +%H:%M:%S)"
        result2, elapsed2 = await exec_bash(client, cmd2, req_id=20, new_session=True, timeout_arg=60, http_timeout=120)
        text2 = extract_text(result2)
        print("\n--- result text (control) ---")
        print(text2)
        print(f"\n--- wall clock elapsed: {elapsed2:.2f}s ---")


if __name__ == "__main__":
    asyncio.run(main())
