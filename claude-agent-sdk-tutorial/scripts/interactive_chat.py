"""
Interactive Chat with Claude Code SDK
=====================================

A simple interactive chat loop that waits for user input.

Usage:
    uv run python scripts/interactive_chat.py
"""

import asyncio
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / '.env')

from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    ResultMessage,
    TextBlock,
)


async def interactive_chat():
    """Interactive chat loop with Claude."""

    options = ClaudeAgentOptions(
        system_prompt="You are a helpful assistant. Be concise.",
        allowed_tools=["Read", "Bash", "Glob"],
        max_turns=10,
    )

    print("=" * 50)
    print("Interactive Claude Chat")
    print("Type 'quit' or 'exit' to end the conversation")
    print("=" * 50)

    async with ClaudeSDKClient(options=options) as client:
        while True:
            # Wait for user input
            try:
                user_input = input("\nYou: ").strip()
            except EOFError:
                break

            # Check for exit commands
            if user_input.lower() in ('quit', 'exit', 'q'):
                print("\nGoodbye!")
                break

            # Skip empty input
            if not user_input:
                continue

            # Send to Claude and stream response
            await client.query(user_input)

            print("\nClaude: ", end="", flush=True)
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            print(block.text, end="", flush=True)
                elif isinstance(msg, ResultMessage):
                    print()  # Newline after response


async def main():
    await interactive_chat()


if __name__ == "__main__":
    asyncio.run(main())
