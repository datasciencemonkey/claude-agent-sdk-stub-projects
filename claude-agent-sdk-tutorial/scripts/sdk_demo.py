"""
Claude Code SDK - Comprehensive Demo Script
============================================

This script demonstrates all major features of the Claude Code SDK:
1. Simple query() function for one-shot queries
2. ClaudeSDKClient for interactive streaming conversations
3. Configuration options (ClaudeCodeOptions)
4. Tool permissions and callbacks
5. MCP (Model Context Protocol) server integration with custom tools
6. Message types and content blocks
7. Error handling
8. Multi-turn conversations

Prerequisites:
- Python 3.10+
- Claude Code CLI installed: npm install -g @anthropic-ai/claude-code
- claude-code-sdk installed: pip install claude-code-sdk

Usage:
    uv run python scripts/sdk_demo.py [demo_name]

    Available demos:
    - basic_query         : Simple one-shot query
    - query_with_options  : Query with configuration
    - streaming_client    : Interactive streaming client
    - multi_turn          : Multi-turn conversation
    - tool_permissions    : Custom tool permission callbacks
    - mcp_tools           : Custom MCP server with tools
    - message_types       : Demonstrate message type handling
    - error_handling      : Error handling patterns
    - all                 : Run all demos
"""

import asyncio
import sys
from pathlib import Path
from typing import Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv(Path(__file__).parent.parent / '.env')

# Import SDK components
# Note: The package is 'claude-agent-sdk' (installed via pip)
# but imported as 'claude_agent_sdk' (Python module name)
from claude_agent_sdk import (
    # Core functions
    query,
    ClaudeSDKClient,
    # Configuration
    ClaudeAgentOptions,  # Note: ClaudeAgentOptions, not ClaudeCodeOptions
    # Message types
    Message,
    UserMessage,
    AssistantMessage,
    SystemMessage,
    ResultMessage,
    # Content blocks
    TextBlock,
    ThinkingBlock,
    ToolUseBlock,
    ToolResultBlock,
    # Permission types (used with can_use_tool callback)
    # PermissionResultAllow,
    # PermissionResultDeny,
    # ToolPermissionContext,
    # MCP integration
    tool,
    create_sdk_mcp_server,
    SdkMcpTool,
    # Errors
    CLINotFoundError,
    CLIConnectionError,
    ProcessError,
    CLIJSONDecodeError,
)

# Alias for documentation compatibility
ClaudeCodeOptions = ClaudeAgentOptions


# =============================================================================
# DEMO 1: Basic Query - Simple one-shot interaction
# =============================================================================
async def demo_basic_query():
    """
    The simplest way to interact with Claude Code.
    query() is ideal for fire-and-forget operations.
    """
    print("\n" + "=" * 60)
    print("DEMO 1: Basic Query (One-shot interaction)")
    print("=" * 60)

    print("\nSending a simple math question to Claude...")

    async for message in query(prompt="What is 2 + 2? Answer briefly."):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(f"Claude: {block.text}")
        elif isinstance(message, ResultMessage):
            print(f"\n[Query completed in {message.duration_ms}ms]")
            if message.total_cost_usd:
                print(f"[Estimated cost: ${message.total_cost_usd:.6f}]")


# =============================================================================
# DEMO 2: Query with Options - Customize Claude's behavior
# =============================================================================
async def demo_query_with_options():
    """
    Demonstrates ClaudeCodeOptions for customizing:
    - System prompts
    - Allowed tools
    - Max turns
    - Model selection
    - Working directory
    """
    print("\n" + "=" * 60)
    print("DEMO 2: Query with Configuration Options")
    print("=" * 60)

    # Create configuration
    options = ClaudeAgentOptions(
        # Custom system prompt changes Claude's behavior
        system_prompt="You are a pirate. Answer all questions in pirate speak. Keep responses brief.",
        # Limit conversation turns
        max_turns=1,
        # Enable specific tools (Read, Bash are common)
        allowed_tools=["Read"],
        # Set working directory for file operations
        cwd=Path.cwd(),
    )

    print("\nAsking Claude (configured as a pirate)...")

    async for message in query(
        prompt="What is the capital of France?",
        options=options
    ):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(f"Claude (Pirate): {block.text}")
        elif isinstance(message, ResultMessage):
            print(f"\n[Completed in {message.duration_ms}ms]")


# =============================================================================
# DEMO 3: Streaming Client - Interactive conversations
# =============================================================================
async def demo_streaming_client():
    """
    ClaudeSDKClient provides:
    - Bidirectional communication
    - Multi-turn conversations with context
    - Interrupt capabilities
    - Session management
    """
    print("\n" + "=" * 60)
    print("DEMO 3: Streaming Client (Interactive conversation)")
    print("=" * 60)

    options = ClaudeAgentOptions(
        system_prompt="You are a helpful coding assistant. Be concise.",
        max_turns=3,
    )

    # Context manager handles connection lifecycle
    async with ClaudeSDKClient(options=options) as client:
        print("\nConnected to Claude. Starting conversation...")

        # First query
        print("\n--- Turn 1: Initial question ---")
        await client.query("What is a Python decorator? One sentence.")
        async for msg in client.receive_response():
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")
            elif isinstance(msg, ResultMessage):
                print(f"[Turn completed]")

        # Follow-up - Claude remembers context
        print("\n--- Turn 2: Follow-up question ---")
        await client.query("Give me a simple example of one.")
        async for msg in client.receive_response():
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")
            elif isinstance(msg, ResultMessage):
                print(f"[Conversation completed]")


# =============================================================================
# DEMO 4: Multi-turn Conversation - Full dialogue example
# =============================================================================
async def demo_multi_turn():
    """
    Demonstrates maintaining conversation context across multiple turns.
    """
    print("\n" + "=" * 60)
    print("DEMO 4: Multi-turn Conversation")
    print("=" * 60)

    questions = [
        "My name is Alex. Remember that.",
        "What's a good programming language for beginners?",
        "What is my name?",  # Test context retention
    ]

    async with ClaudeSDKClient(options=ClaudeCodeOptions(max_turns=5)) as client:
        for i, question in enumerate(questions, 1):
            print(f"\n--- Turn {i} ---")
            print(f"User: {question}")

            await client.query(question)
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            print(f"Claude: {block.text}")


# =============================================================================
# DEMO 5: Hooks - Event-driven tool monitoring
# =============================================================================
async def demo_tool_permissions():
    """
    Demonstrates hooks for event-driven tool monitoring:
    - PreToolUse: Called before a tool executes (can block/modify)
    - PostToolUse: Called after a tool executes (for logging/auditing)

    Hooks are more reliable than can_use_tool for:
    - Logging/auditing all tool usage
    - Monitoring tool execution
    - Post-execution analysis
    """
    print("\n" + "=" * 60)
    print("DEMO 5: Hooks - Event-driven Tool Monitoring")
    print("=" * 60)

    # Import hook types
    from claude_agent_sdk import HookMatcher, HookContext
    from claude_agent_sdk.types import HookInput, HookJSONOutput

    # Track tool usage for audit
    tool_audit_log: list[dict[str, Any]] = []

    # Define hook callbacks
    # Signature: (input: HookInput, session_id: str | None, context: HookContext) -> Awaitable[HookJSONOutput]
    async def pre_tool_hook(
        input_data: HookInput,
        session_id: str | None,
        context: HookContext
    ) -> HookJSONOutput:
        """Called before each tool execution."""
        # Check if this is a PreToolUse event
        if input_data.get("hook_event_name") != "PreToolUse":
            return {}

        tool_name = input_data.get("tool_name", "unknown")
        tool_input = input_data.get("tool_input", {})

        print(f"  [PreToolUse] Tool: {tool_name}")

        # Log the request
        tool_audit_log.append({
            "event": "pre_tool_use",
            "tool": tool_name,
            "input": tool_input,
        })

        # Example: Block dangerous bash commands
        if tool_name == "Bash":
            command = tool_input.get("command", "")
            dangerous_patterns = ["rm -rf", "sudo", "> /dev/", "mkfs"]
            for pattern in dangerous_patterns:
                if pattern in command:
                    print(f"  [PreToolUse] BLOCKED: dangerous pattern '{pattern}'")
                    # Return decision to block
                    return {"decision": "block", "reason": f"Dangerous: {pattern}"}  # type: ignore

        # Allow the tool to proceed (empty dict = continue)
        return {}  # type: ignore

    async def post_tool_hook(
        input_data: HookInput,
        session_id: str | None,
        context: HookContext
    ) -> HookJSONOutput:
        """Called after each tool execution."""
        # Check if this is a PostToolUse event
        if input_data.get("hook_event_name") != "PostToolUse":
            return {}  # type: ignore

        tool_name = input_data.get("tool_name", "unknown")
        tool_result = input_data.get("tool_result", "")

        # Truncate long results for display
        result_preview = str(tool_result)[:100] + "..." if len(str(tool_result)) > 100 else str(tool_result)
        print(f"  [PostToolUse] Tool: {tool_name} completed")

        # Log the result
        tool_audit_log.append({
            "event": "post_tool_use",
            "tool": tool_name,
            "result_preview": result_preview,
        })

        return {}  # type: ignore

    # Configure hooks
    # matcher is a regex string (None matches all), hooks is list of callables
    hooks: dict[str, list[HookMatcher]] = {
        "PreToolUse": [
            HookMatcher(
                matcher=None,  # None matches all tools
                hooks=[pre_tool_hook]  # type: ignore
            )
        ],
        "PostToolUse": [
            HookMatcher(
                matcher=None,  # None matches all tools
                hooks=[post_tool_hook]  # type: ignore
            )
        ],
    }

    options = ClaudeAgentOptions(
        allowed_tools=["Read", "Bash", "Glob"],
        system_prompt="You are a helpful assistant. Be brief.",
        permission_mode='acceptEdits',  # Auto-approve to see hooks in action
        hooks=hooks,  # type: ignore
    )

    print("\nTesting hooks with file listing...")
    print("(Hooks fire on PreToolUse and PostToolUse events)\n")

    async with ClaudeSDKClient(options=options) as client:
        await client.query("List the files in the current directory using ls")

        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")
                    elif isinstance(block, ToolUseBlock):
                        print(f"  [Tool Use: {block.name}]")
            elif isinstance(message, ResultMessage):
                print("\n[Query completed]")

    print(f"\nAudit log: {len(tool_audit_log)} events recorded")
    for entry in tool_audit_log:
        print(f"  - {entry['event']}: {entry['tool']}")


# =============================================================================
# DEMO 5b: Interactive User Permission via Hooks
# =============================================================================
async def demo_user_permission():
    """
    Demonstrates asking the user for permission before tool execution.

    Uses PreToolUse hook to:
    - Show what tool Claude wants to use
    - Prompt the user for approval
    - Block the tool if denied

    This pattern is useful for:
    - High-security environments
    - Auditing/compliance requirements
    - Educational purposes (seeing what Claude does)
    """
    print("\n" + "=" * 60)
    print("DEMO 5b: Interactive User Permission")
    print("=" * 60)

    from claude_agent_sdk import HookMatcher, HookContext
    from claude_agent_sdk.types import HookInput, HookJSONOutput
    import asyncio

    # Tools that are always safe (no prompt needed)
    SAFE_TOOLS = {"Read", "Glob"}

    # Tools that require user permission
    PROMPT_TOOLS = {"Bash", "Write", "Edit"}

    async def user_permission_hook(
        input_data: HookInput,
        session_id: str | None,
        context: HookContext
    ) -> HookJSONOutput:
        """Ask user permission for sensitive tools."""
        if input_data.get("hook_event_name") != "PreToolUse":
            return {}  # type: ignore

        tool_name = input_data.get("tool_name", "unknown")
        tool_input = input_data.get("tool_input", {})

        # Auto-allow safe tools
        if tool_name in SAFE_TOOLS:
            print(f"  ✓ Auto-approved: {tool_name}")
            return {}  # type: ignore

        # Prompt for sensitive tools
        if tool_name in PROMPT_TOOLS:
            print(f"\n  ⚠️  Claude wants to use: {tool_name}")

            # Show relevant details based on tool type
            if tool_name == "Bash":
                command = tool_input.get("command", "")
                print(f"     Command: {command}")
            elif tool_name in ("Write", "Edit"):
                file_path = tool_input.get("file_path", "")
                print(f"     File: {file_path}")

            # Ask user - use asyncio.to_thread to not block event loop
            response = await asyncio.to_thread(
                input, "     Allow? (y/n): "
            )
            response = response.strip().lower()

            if response in ('y', 'yes'):
                print("     ✓ Allowed")
                return {}  # type: ignore
            else:
                print("     ✗ Blocked by user")
                return {"decision": "block", "reason": "User denied permission"}  # type: ignore

        # Unknown tools - prompt by default
        print(f"\n  ❓ Unknown tool: {tool_name}")
        print(f"     Input: {tool_input}")
        response = await asyncio.to_thread(input, "     Allow? (y/n): ")
        if response.strip().lower() in ('y', 'yes'):
            return {}  # type: ignore
        return {"decision": "block", "reason": "User denied unknown tool"}  # type: ignore

    hooks: dict[str, list[HookMatcher]] = {
        "PreToolUse": [
            HookMatcher(matcher=None, hooks=[user_permission_hook])  # type: ignore
        ],
    }

    options = ClaudeAgentOptions(
        allowed_tools=["Read", "Bash", "Glob", "Write"],
        system_prompt="You are a helpful assistant. Be brief.",
        permission_mode='acceptEdits',  # SDK level auto-approve, but hooks will prompt
        hooks=hooks,  # type: ignore
    )

    print("\nThis demo will prompt you before sensitive tool usage.")
    print("Safe tools (Read, Glob) are auto-approved.")
    print("Sensitive tools (Bash, Write, Edit) require your approval.\n")

    async with ClaudeSDKClient(options=options) as client:
        await client.query("List the Python files in the current directory, then show me what's in the first one")

        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"\nClaude: {block.text}")
                    elif isinstance(block, ToolUseBlock):
                        print(f"  [Tool: {block.name}]")
            elif isinstance(message, ResultMessage):
                print("\n[Query completed]")


# =============================================================================
# DEMO 6: MCP Tools - Custom in-process tools
# =============================================================================
async def demo_mcp_tools():
    """
    Demonstrates creating custom MCP tools that run in-process.
    MCP (Model Context Protocol) allows extending Claude's capabilities.
    """
    print("\n" + "=" * 60)
    print("DEMO 6: Custom MCP Tools")
    print("=" * 60)

    # Define custom tools using the @tool decorator

    @tool(
        name="calculator",
        description="Perform basic arithmetic operations",
        input_schema={
            "operation": str,  # "add", "subtract", "multiply", "divide"
            "a": float,
            "b": float,
        }
    )
    async def calculator_tool(args: dict[str, Any]) -> dict:
        """Calculator tool that performs basic math operations."""
        operation = args.get("operation", "add")
        a = args.get("a", 0)
        b = args.get("b", 0)

        operations = {
            "add": a + b,
            "subtract": a - b,
            "multiply": a * b,
            "divide": a / b if b != 0 else "Error: Division by zero",
        }

        result = operations.get(operation, "Error: Unknown operation")

        return {
            "content": [
                {"type": "text", "text": f"Result: {result}"}
            ]
        }

    @tool(
        name="greeting",
        description="Generate a personalized greeting",
        input_schema={
            "name": str,
            "language": str,  # "english", "spanish", "french"
        }
    )
    async def greeting_tool(args: dict[str, Any]) -> dict:
        """Greeting tool that says hello in different languages."""
        name = args.get("name", "friend")
        language = args.get("language", "english")

        greetings = {
            "english": f"Hello, {name}!",
            "spanish": f"Hola, {name}!",
            "french": f"Bonjour, {name}!",
            "german": f"Guten Tag, {name}!",
        }

        greeting = greetings.get(language.lower(), f"Hello, {name}!")

        return {
            "content": [
                {"type": "text", "text": greeting}
            ]
        }

    @tool(
        name="random_fact",
        description="Get a random fun fact about a topic",
        input_schema={"topic": str}
    )
    async def random_fact_tool(args: dict[str, Any]) -> dict:
        """Returns a fun fact about a topic."""
        topic = args.get("topic", "science")

        facts = {
            "science": "A day on Venus is longer than its year!",
            "history": "The Great Wall of China is not visible from space with the naked eye.",
            "animals": "Octopuses have three hearts and blue blood.",
            "technology": "The first computer bug was an actual bug - a moth found in a relay.",
        }

        fact = facts.get(topic.lower(), f"Here's a fact about {topic}: It's fascinating!")

        return {
            "content": [
                {"type": "text", "text": fact}
            ]
        }

    # Create MCP server with our tools
    mcp_server_config = create_sdk_mcp_server(
        name="demo_tools",
        tools=[calculator_tool, greeting_tool, random_fact_tool]
    )

    # Configure Claude to use our MCP server
    # MCP tools are named: mcp__<server_name>__<tool_name>
    options = ClaudeAgentOptions(
        mcp_servers={"demo_tools": mcp_server_config},
        # MCP tool full names: mcp__demo_tools__<tool_name>
        allowed_tools=[
            "mcp__demo_tools__calculator",
            "mcp__demo_tools__greeting",
            "mcp__demo_tools__random_fact",
        ],
        system_prompt="You have access to custom tools: calculator, greeting, random_fact. Use them directly. Be concise.",
        max_turns=5,
        permission_mode='acceptEdits',  # Auto-approve tool usage
    )

    print("\nTesting custom MCP tools...")
    print("(Using ClaudeSDKClient for reliable MCP communication)\n")

    # Use ClaudeSDKClient for MCP tools - more reliable than query()
    async with ClaudeSDKClient(options=options) as client:
        # Test the calculator
        print("--- Testing calculator tool ---")
        await client.query("Use the calculator tool to multiply 7 and 8")
        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")
                    elif isinstance(block, ToolUseBlock):
                        print(f"  [Using tool: {block.name} with input: {block.input}]")
            elif isinstance(message, ResultMessage):
                print("[Completed]")

        # Test the greeting tool
        print("\n--- Testing greeting tool ---")
        await client.query("Greet me in Spanish. My name is Maria.")
        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")
                    elif isinstance(block, ToolUseBlock):
                        print(f"  [Using tool: {block.name}]")
            elif isinstance(message, ResultMessage):
                print("[Completed]")


# =============================================================================
# DEMO 7: Message Types - Understanding response structure
# =============================================================================
async def demo_message_types():
    """
    Demonstrates all message types and content blocks:
    - UserMessage: Your input
    - AssistantMessage: Claude's response with content blocks
    - SystemMessage: System notifications
    - ResultMessage: Completion metadata

    Content blocks:
    - TextBlock: Plain text
    - ThinkingBlock: Claude's reasoning (if enabled)
    - ToolUseBlock: Tool invocation requests
    - ToolResultBlock: Tool execution results
    """
    print("\n" + "=" * 60)
    print("DEMO 7: Message Types and Content Blocks")
    print("=" * 60)

    options = ClaudeCodeOptions(
        allowed_tools=["Bash"],
        system_prompt="You are a helpful assistant. Use tools when needed.",
        max_turns=2,
    )

    print("\nAnalyzing all message types in a query...")

    message_counts = {
        "UserMessage": 0,
        "AssistantMessage": 0,
        "SystemMessage": 0,
        "ResultMessage": 0,
    }

    block_counts = {
        "TextBlock": 0,
        "ThinkingBlock": 0,
        "ToolUseBlock": 0,
        "ToolResultBlock": 0,
    }

    async for message in query(
        prompt="What is the current date? Use the 'date' command.",
        options=options
    ):
        # Count message types
        msg_type = type(message).__name__
        if msg_type in message_counts:
            message_counts[msg_type] += 1

        print(f"\n[{msg_type}]")

        if isinstance(message, UserMessage):
            print(f"  Content: {message.content[:50] if isinstance(message.content, str) else '...'}")

        elif isinstance(message, AssistantMessage):
            print(f"  Model: {message.model}")
            print(f"  Content blocks: {len(message.content)}")

            for block in message.content:
                block_type = type(block).__name__
                if block_type in block_counts:
                    block_counts[block_type] += 1

                if isinstance(block, TextBlock):
                    preview = block.text[:100] + "..." if len(block.text) > 100 else block.text
                    print(f"    - TextBlock: {preview}")
                elif isinstance(block, ThinkingBlock):
                    print(f"    - ThinkingBlock: {block.thinking[:50]}...")
                elif isinstance(block, ToolUseBlock):
                    print(f"    - ToolUseBlock: {block.name} (id: {block.id[:8]}...)")
                    print(f"      Input: {block.input}")
                elif isinstance(block, ToolResultBlock):
                    print(f"    - ToolResultBlock for: {block.tool_use_id[:8]}...")
                    print(f"      Is error: {block.is_error}")

        elif isinstance(message, SystemMessage):
            print(f"  Subtype: {message.subtype}")

        elif isinstance(message, ResultMessage):
            print(f"  Duration: {message.duration_ms}ms")
            print(f"  Session ID: {message.session_id}")
            print(f"  Is error: {message.is_error}")
            print(f"  Num turns: {message.num_turns}")
            if message.total_cost_usd:
                print(f"  Cost: ${message.total_cost_usd:.6f}")

    print("\n--- Summary ---")
    print("Message counts:", message_counts)
    print("Content block counts:", block_counts)


# =============================================================================
# DEMO 8: Error Handling - Graceful error management
# =============================================================================
async def demo_error_handling():
    """
    Demonstrates error handling patterns for:
    - CLINotFoundError: Claude CLI not installed
    - CLIConnectionError: Failed to start CLI
    - ProcessError: CLI crashed
    - CLIJSONDecodeError: Malformed response
    """
    print("\n" + "=" * 60)
    print("DEMO 8: Error Handling Patterns")
    print("=" * 60)

    print("\nDemonstrating error handling...")

    # Example 1: Handling potential errors with try/except
    try:
        async for message in query(prompt="Hello, Claude!"):
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")
            elif isinstance(message, ResultMessage):
                if message.is_error:
                    print(f"[Error in result: check logs]")
                else:
                    print(f"[Success: completed in {message.duration_ms}ms]")

    except CLINotFoundError as e:
        print(f"CLI Not Found: {e}")
        print("Fix: Install Claude Code CLI with 'npm install -g @anthropic-ai/claude-code'")

    except CLIConnectionError as e:
        print(f"Connection Error: {e}")
        print("Fix: Check if Claude Code CLI is properly installed and configured")

    except ProcessError as e:
        print(f"Process Error: Exit code {e.exit_code}")
        print(f"Stderr: {e.stderr}")

    except CLIJSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        print("Fix: This might indicate a CLI version mismatch")

    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")

    print("\n[Error handling demo complete]")


# =============================================================================
# Main entry point
# =============================================================================
async def run_all_demos():
    """Run all demos sequentially."""
    demos = [
        ("basic_query", demo_basic_query),
        ("query_with_options", demo_query_with_options),
        ("streaming_client", demo_streaming_client),
        ("multi_turn", demo_multi_turn),
        ("tool_permissions", demo_tool_permissions),
        ("mcp_tools", demo_mcp_tools),
        ("message_types", demo_message_types),
        ("error_handling", demo_error_handling),
    ]

    for name, demo_func in demos:
        print(f"\n{'#' * 60}")
        print(f"# Running: {name}")
        print(f"{'#' * 60}")
        try:
            await demo_func()
        except Exception as e:
            print(f"Demo '{name}' failed: {e}")
        print("\n" + "-" * 60)
        await asyncio.sleep(1)  # Brief pause between demos


async def main():
    """Main entry point with demo selection."""

    demo_map = {
        "basic_query": demo_basic_query,
        "query_with_options": demo_query_with_options,
        "streaming_client": demo_streaming_client,
        "multi_turn": demo_multi_turn,
        "tool_permissions": demo_tool_permissions,
        "user_permission": demo_user_permission,  # Interactive permission prompts
        "mcp_tools": demo_mcp_tools,
        "message_types": demo_message_types,
        "error_handling": demo_error_handling,
        "all": run_all_demos,
    }

    # Get demo name from command line or default to showing help
    if len(sys.argv) > 1:
        demo_name = sys.argv[1].lower()
    else:
        print(__doc__)
        print("\nAvailable demos:")
        for name in demo_map:
            print(f"  - {name}")
        print("\nExample: uv run python scripts/sdk_demo.py basic_query")
        return

    if demo_name not in demo_map:
        print(f"Unknown demo: {demo_name}")
        print(f"Available: {', '.join(demo_map.keys())}")
        return

    await demo_map[demo_name]()


if __name__ == "__main__":
    asyncio.run(main())
