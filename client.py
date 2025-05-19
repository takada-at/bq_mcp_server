from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pathlib import Path


root = Path(__file__).parent.resolve()

# Create server parameters for stdio connection
server_params = StdioServerParameters(
    command="python",  # Executable
    args=["-m", "bq_meta_api.mcp_server"],  # Optional command line arguments
    env={
        "PYTHONPATH": str(root),  # Set PYTHONPATH to the root directory
    }
)


async def run():
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(
            read, write
        ) as session:
            # Initialize the connection
            await session.initialize()
            tools = await session.list_tools()
            print("Available tools:", tools)
            result = await session.call_tool("get_datasets")
            print(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
