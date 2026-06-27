import asyncio
import os
from dotenv import load_dotenv

# Load env vars to get SANDBOX_MCP_AUTH_KEY
load_dotenv()

from main import SandboxMCPClient

async def main():
    print("Testing SandboxMCPClient...")
    async with SandboxMCPClient() as client:
        print("Connected.")
        
        # Test write_file
        print("Testing write_file...")
        success = await client.write_file("/tmp/test_mcp.txt", "hello from mcp")
        print(f"write_file success: {success}")
        
        # Test read_file
        print("Testing read_file...")
        content = await client.read_file("/tmp/test_mcp.txt")
        print(f"read_file content: {content!r}")
        
        # Test exec_shell
        print("Testing exec_shell...")
        success, stdout, stderr = await client.exec_shell("cat /tmp/test_mcp.txt", cwd="/tmp")
        print(f"exec_shell success: {success}")
        print(f"stdout: {stdout!r}")
        print(f"stderr: {stderr!r}")

if __name__ == "__main__":
    asyncio.run(main())
