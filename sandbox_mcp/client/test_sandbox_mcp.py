import pytest
import asyncio
from dotenv import load_dotenv

load_dotenv()

from main import SandboxMCPClient

async def run_test_success():
    """Test a command that completes successfully."""
    async with SandboxMCPClient() as client:
        success, stdout, stderr = await client.exec_shell("echo 'hello success'")
        assert success is True
        assert "hello success" in stdout

def test_success():
    asyncio.run(run_test_success())

async def run_test_failure():
    """Test a command that intentionally fails."""
    async with SandboxMCPClient() as client:
        success, stdout, stderr = await client.exec_shell("ls /non_existent_directory_123")
        assert success is False
        assert "No such file or directory" in stderr or "No such file or directory" in stdout

def test_failure():
    asyncio.run(run_test_failure())

async def run_test_timeout():
    """Test a command that exceeds the MCP server timeout."""
    async with SandboxMCPClient() as client:
        # Pass a custom short timeout of 2 seconds, and sleep for 5 seconds
        result = await client.exec_command("sleep 5", timeout=2)
        
        # The exact format of the timeout result depends on how the server implements it.
        # Usually it will either return a specific exit code or have an error message.
        # We can assert that it did not complete successfully.
        assert result.get("exit_code") != 0
        assert "timed out" in result.get("error", "").lower() or "timed out" in result.get("stderr", "").lower() or result.get("timeout_reached", False)

def test_timeout():
    asyncio.run(run_test_timeout())
