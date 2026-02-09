"""
MCP Tools with Interactive User Permission
==========================================

Demonstrates combining custom MCP tools with user permission hooks.
This pattern is useful for:
- Auditing what Claude does with your custom tools
- High-security environments where tool usage needs approval
- Educational purposes to understand Claude's tool usage

Usage:
    uv run python scripts/mcp_with_permission.py
"""

import asyncio
from pathlib import Path
from typing import Any
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / '.env')

from claude_agent_sdk import (
    ClaudeSDKClient,
    ClaudeAgentOptions,
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
    # MCP tools
    tool,
    create_sdk_mcp_server,
    # Hooks
    HookMatcher,
    HookContext,
)
from claude_agent_sdk.types import HookInput, HookJSONOutput


# =============================================================================
# Define Custom MCP Tools
# =============================================================================

@tool(
    name="database_query",
    description="Execute a read-only SQL query against the database",
    input_schema={"query": str, "database": str}
)
async def database_query_tool(args: dict[str, Any]) -> dict:
    """Simulates executing a database query."""
    query = args.get("query", "")
    database = args.get("database", "main")

    # Simulate query execution
    if "SELECT" in query.upper():
        return {
            "content": [{
                "type": "text",
                "text": f"Query executed on '{database}':\n"
                        f"Results: [row1: id=1, name='Alice'], [row2: id=2, name='Bob']"
            }]
        }
    else:
        return {
            "content": [{
                "type": "text",
                "text": "Error: Only SELECT queries are allowed"
            }]
        }


@tool(
    name="file_manager",
    description="Manage files: list, read, or delete files in a directory",
    input_schema={"action": str, "path": str}  # action: list, read, delete
)
async def file_manager_tool(args: dict[str, Any]) -> dict:
    """Simulates file management operations."""
    action = args.get("action", "list")
    path = args.get("path", ".")

    if action == "list":
        return {
            "content": [{
                "type": "text",
                "text": f"Files in '{path}': config.json, data.csv, README.md"
            }]
        }
    elif action == "read":
        return {
            "content": [{
                "type": "text",
                "text": f"Contents of '{path}': [file contents would appear here]"
            }]
        }
    elif action == "delete":
        return {
            "content": [{
                "type": "text",
                "text": f"Deleted: '{path}'"
            }]
        }
    else:
        return {
            "content": [{
                "type": "text",
                "text": f"Unknown action: {action}"
            }]
        }


@tool(
    name="send_notification",
    description="Send a notification to a user via email or SMS",
    input_schema={"recipient": str, "message": str, "channel": str}
)
async def send_notification_tool(args: dict[str, Any]) -> dict:
    """Simulates sending a notification."""
    recipient = args.get("recipient", "")
    message = args.get("message", "")
    channel = args.get("channel", "email")

    return {
        "content": [{
            "type": "text",
            "text": f"Notification sent via {channel} to {recipient}: '{message}'"
        }]
    }


# =============================================================================
# Permission Hook
# =============================================================================

# Define risk levels for tools
TOOL_RISK_LEVELS: dict[str, str] = {
    # Low risk - auto-approve
    "mcp__secure_tools__database_query": "low",
    # Medium risk - prompt user
    "mcp__secure_tools__file_manager": "medium",
    # High risk - always prompt with warning
    "mcp__secure_tools__send_notification": "high",
}


async def permission_hook(
    input_data: HookInput,
    session_id: str | None,
    context: HookContext
) -> HookJSONOutput:
    """Interactive permission hook with risk-based prompting."""
    if input_data.get("hook_event_name") != "PreToolUse":
        return {}  # type: ignore

    tool_name = input_data.get("tool_name", "unknown")
    tool_input = input_data.get("tool_input", {})

    # Auto-allow internal/system tools (MCPSearch, etc.)
    INTERNAL_TOOLS = {"MCPSearch", "Read", "Glob", "Grep"}
    if tool_name in INTERNAL_TOOLS:
        return {}  # type: ignore

    # Get risk level
    risk_level = TOOL_RISK_LEVELS.get(tool_name, "unknown")

    # Low risk - auto-approve with log
    if risk_level == "low":
        print(f"  ✓ Auto-approved (low risk): {tool_name}")
        print(f"    Input: {tool_input}")
        return {}  # type: ignore

    # Medium risk - show details and prompt
    if risk_level == "medium":
        print(f"\n  ⚠️  Medium Risk Tool: {tool_name}")
        print(f"    Action: {tool_input.get('action', 'unknown')}")
        print(f"    Path: {tool_input.get('path', 'unknown')}")

        # Special handling for delete operations
        if tool_input.get("action") == "delete":
            print("    ⛔ DELETE operation detected!")

        response = await asyncio.to_thread(input, "    Allow? (y/n): ")
        if response.strip().lower() in ('y', 'yes'):
            print("    ✓ Allowed by user")
            return {}  # type: ignore
        else:
            print("    ✗ Blocked by user")
            return {"decision": "block", "reason": "User denied medium-risk tool"}  # type: ignore

    # High risk - warn and prompt
    if risk_level == "high":
        print(f"\n  🚨 HIGH RISK Tool: {tool_name}")
        print(f"    Recipient: {tool_input.get('recipient', 'unknown')}")
        print(f"    Message: {tool_input.get('message', 'unknown')}")
        print(f"    Channel: {tool_input.get('channel', 'unknown')}")
        print("    ⚠️  This will send a real notification!")

        response = await asyncio.to_thread(
            input, "    Type 'CONFIRM' to allow (or anything else to deny): "
        )
        if response.strip() == 'CONFIRM':
            print("    ✓ Confirmed by user")
            return {}  # type: ignore
        else:
            print("    ✗ Blocked - confirmation not provided")
            return {"decision": "block", "reason": "User did not confirm high-risk operation"}  # type: ignore

    # Unknown tool - prompt by default
    print(f"\n  ❓ Unknown Tool: {tool_name}")
    print(f"    Input: {tool_input}")
    response = await asyncio.to_thread(input, "    Allow? (y/n): ")
    if response.strip().lower() in ('y', 'yes'):
        return {}  # type: ignore
    return {"decision": "block", "reason": "User denied unknown tool"}  # type: ignore


async def audit_hook(
    input_data: HookInput,
    session_id: str | None,
    context: HookContext
) -> HookJSONOutput:
    """Log all tool executions for audit purposes."""
    if input_data.get("hook_event_name") != "PostToolUse":
        return {}  # type: ignore

    tool_name = input_data.get("tool_name", "unknown")
    tool_result = input_data.get("tool_result", "")

    # Truncate long results
    result_preview = str(tool_result)[:80] + "..." if len(str(tool_result)) > 80 else str(tool_result)
    print(f"  📋 Audit: {tool_name} completed")
    print(f"     Result: {result_preview}")

    return {}  # type: ignore


# =============================================================================
# Main Demo
# =============================================================================

async def main():
    print("=" * 60)
    print("MCP Tools with Interactive User Permission")
    print("=" * 60)

    # Create MCP server with our tools
    mcp_server = create_sdk_mcp_server(
        name="secure_tools",
        tools=[database_query_tool, file_manager_tool, send_notification_tool]
    )

    # Configure hooks
    hooks: dict[str, list[HookMatcher]] = {
        "PreToolUse": [
            HookMatcher(matcher=None, hooks=[permission_hook])  # type: ignore
        ],
        "PostToolUse": [
            HookMatcher(matcher=None, hooks=[audit_hook])  # type: ignore
        ],
    }

    # Configure options
    options = ClaudeAgentOptions(
        mcp_servers={"secure_tools": mcp_server},
        allowed_tools=[
            "mcp__secure_tools__database_query",
            "mcp__secure_tools__file_manager",
            "mcp__secure_tools__send_notification",
        ],
        system_prompt="""You have access to these tools:
- database_query: Execute SQL queries (read-only)
- file_manager: List, read, or delete files
- send_notification: Send email/SMS notifications

When asked, use these tools to help the user. Be concise.""",
        max_turns=10,
        permission_mode='acceptEdits',  # SDK level auto-approve, hooks handle permission
        hooks=hooks,  # type: ignore
    )

    print("\nRisk Levels:")
    print("  - database_query: LOW (auto-approved)")
    print("  - file_manager: MEDIUM (prompted)")
    print("  - send_notification: HIGH (requires 'CONFIRM')")
    print("\nStarting interactive session...")
    print("Type 'quit' to exit.\n")

    async with ClaudeSDKClient(options=options) as client:
        while True:
            try:
                user_input = input("\nYou: ").strip()
            except EOFError:
                break

            if user_input.lower() in ('quit', 'exit', 'q'):
                print("\nGoodbye!")
                break

            if not user_input:
                continue

            await client.query(user_input)

            print("\nClaude: ", end="", flush=True)
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            print(block.text, end="", flush=True)
                        elif isinstance(block, ToolUseBlock):
                            print(f"\n  [Calling: {block.name}]")
                elif isinstance(message, ResultMessage):
                    print()  # Newline after response


if __name__ == "__main__":
    asyncio.run(main())
