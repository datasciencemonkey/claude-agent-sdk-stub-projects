import asyncio
from dotenv import load_dotenv
load_dotenv('../.env')
from claude_agent_sdk import query, ClaudeAgentOptions

async def main():
    async for message in query(
        prompt="write me a python function that can run any unix command",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Edit", "Bash"],
            system_prompt="You are a senior engineer"
        )
    ):
        print(message)

asyncio.run(main())