from claude_agent_sdk import ClaudeSDKClient, ClaudeAgentOptions
from dotenv import load_dotenv
load_dotenv('../.env')
import asyncio


async def main():
    options = ClaudeAgentOptions(
        allowed_tools=["Bash", "Read"],
        system_prompt="You are a helpful assistant"
    )
    
    async with ClaudeSDKClient(options=options) as client:
        await client.query("What files are in this directory?")
        async for msg in client.receive_response():
            print(msg)
        
        # Continue the conversation - Claude remembers context
        await client.query("Now count the Python files")
        async for msg in client.receive_response():
            print(msg)

asyncio.run(main())