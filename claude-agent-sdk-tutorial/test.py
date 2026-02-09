# test_claude_sdk_databricks.py

import asyncio
import os
from dotenv import load_dotenv
from claude_agent_sdk import ClaudeSDKClient, ClaudeAgentOptions
load_dotenv()
# Databricks FM API configuration
os.environ["ANTHROPIC_BASE_URL"] = os.getenv("DATABRICKS_HOST", "https://your-workspace.azuredatabricks.net/serving-endpoints") + "/anthropic"
os.environ["ANTHROPIC_AUTH_TOKEN"] = os.getenv("DATABRICKS_TOKEN", "")  # Your Databricks PAT
os.environ["ANTHROPIC_API_KEY"] = ""  # Must be empty string
os.environ["CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS"] = "1"  # Disable experimental betas for Databricks compatibility
os.environ["ANTHROPIC_CUSTOM_HEADERS"] = "x-databricks-disable-beta-headers: true"
os.environ['DISABLE_PROMPT_CACHING'] = '1'  # Disable prompt caching for testing

async def test():
    options = ClaudeAgentOptions(
        model="databricks-claude-opus-4-5",  # Your FM API endpoint name
        system_prompt="You are a helpful assistant. Be concise."
    )

    async with ClaudeSDKClient(options=options) as client:
        await client.query("What is 2 + 2? Reply in one word.")

        async for message in client.receive_response():
            print(message)


if __name__ == "__main__":
    asyncio.run(test())
