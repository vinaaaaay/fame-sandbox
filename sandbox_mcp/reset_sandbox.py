import logging
import os
import time

import httpx

logger = logging.getLogger("reset_sandbox")

AUTH_KEY = os.environ.get("SANDBOX_MCP_AUTH_KEY") or None


def reset_and_wait(mcp_url: str = "http://localhost:8081", timeout: int = 480) -> bool:
    """POST to the MCP server's /reset_sandbox endpoint and wait for completion.

    Returns True if the sandbox was successfully reset, False otherwise.
    The endpoint handles: docker compose down -> up -> health check -> async prune.
    """
    reset_url = f"{mcp_url.rstrip('/')}/reset_sandbox"
    headers = {"Authorization": f"Bearer {AUTH_KEY}"} if AUTH_KEY else None
    logger.info(f"Calling reset at {reset_url} ...")
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(reset_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            if data.get("success"):
                logger.info("Sandbox reset successful")
                return True
            logger.error(f"Sandbox reset failed: {data.get('message', 'unknown error')}")
            return False
    except httpx.HTTPStatusError as e:
        logger.error(f"Reset HTTP {e.response.status_code}: {e.response.text[:500]}")
        return False
    except Exception as e:
        logger.error(f"Reset request failed: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ok = reset_and_wait()
    exit(0 if ok else 1)
