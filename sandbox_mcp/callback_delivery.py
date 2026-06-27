"""
sandbox_mcp/callback_delivery.py
=================================
The sandbox MCP (executor) side of the bridge: deliver the result to GW.

This is what makes the MCP "callback" real. It is the analog of the A2A agent
pushing a notification -- but in MCP there is no native push, so this is an
ordinary SigV4-signed HTTPS POST issued from inside the task's work().

VERIFIED CONSTRAINT (do not skip): delivery must be SESSION-INDEPENDENT.
The coder dispatches the task and disconnects immediately. The sandbox MCP's work()
keeps running (it lives in the server's lifespan task group, not the
request/connection scope). BUT any session-coupled MCP call inside work() after
disconnect will raise ClosedResourceError:

    - task.update_status(...)         -> sends notifications/tasks/status -> DEAD session
    - the SDK auto-complete notify    -> same

If work() hits one of those after disconnect, the work body aborts and the task
is marked failed. So the rule is:

    * In the callback path, work() MUST NOT call update_status().
    * Deliver via this module (a plain httpx POST) BEFORE returning.
    * The SDK's post-return auto-complete notification may still log a harmless
      ClosedResourceError; quiet it via SILENCE_AUTOCOMPLETE_NOISE below.

OUTBOUND CONTRACT (consumed by gateway-mcp):
  POST {callback_url}
  Headers: x-fame-correlation-id: <correlation_id>   (SigV4-signed; GW URL is IAM)
  Body   : {"taskId","correlation_id","status","result": CallToolResult.model_dump(),
            "resume_url": "<coder Lambda function URL>"}
"""
from __future__ import annotations

import json
import logging
import os

import anyio
import boto3
import httpx
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

from mcp.types import CallToolResult

logger = logging.getLogger("sandbox_mcp.callback")

AWS_REGION   = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
HTTP_TIMEOUT = float(os.environ.get("CALLBACK_TIMEOUT", "30"))


def _sign(method: str, url: str, body: bytes, headers: dict[str, str]) -> dict[str, str]:
    creds = boto3.Session(region_name=AWS_REGION).get_credentials().get_frozen_credentials()
    req = AWSRequest(method=method, url=url, data=body, headers=headers)
    SigV4Auth(creds, "lambda", AWS_REGION).add_auth(req)   # GW Function URL = lambda service
    return dict(req.headers)


async def deliver_callback(callback_url: str, *, task_id: str, correlation_id: str,
                           result: CallToolResult, resume_url: str = "") -> bool:
    """Session-independent SigV4 POST of the result to GW.

    Includes resume_url so the gateway can dynamically resume the correct Lambda
    (coder, fa2, or any future agent) without hardcoding per-agent function URLs.
    Returns True on 2xx."""
    if not callback_url:
        logger.warning("[callback] no callback_url; result not delivered (corr_id=%s)",
                       correlation_id)
        return False

    payload = {
        "taskId": task_id,
        "correlation_id": correlation_id,
        "status": "failed" if result.isError else "completed",
        "result": result.model_dump(mode="json"),
        "resume_url": resume_url,
    }
    body = json.dumps(payload).encode("utf-8")
    base_headers = {
        "Content-Type": "application/json",
        "x-fame-correlation-id": correlation_id,   # signed; GW reads this first
    }
    signed = await anyio.to_thread.run_sync(_sign, "POST", callback_url, body, base_headers)

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        try:
            resp = await client.post(callback_url, content=body, headers=signed)
            if 200 <= resp.status_code < 300:
                logger.info("[callback] delivered corr_id=%s task_id=%s",
                            correlation_id, task_id)
                return True
            logger.warning("[callback] GW %s corr_id=%s",
                           resp.status_code, correlation_id)
        except Exception as e:
            logger.warning("[callback] POST failed corr_id=%s: %s",
                           correlation_id, e)

    logger.error("[callback] failed corr_id=%s", correlation_id)
    return False


def SILENCE_AUTOCOMPLETE_NOISE() -> None:
    """Quiet the cosmetic ClosedResourceError the SDK logs when it tries to notify
    a disconnected session after work() returns. Delivery already happened."""
    logging.getLogger("mcp.server.streamable_http_manager").setLevel(logging.CRITICAL)
    logging.getLogger("mcp.server.session").setLevel(logging.CRITICAL)
