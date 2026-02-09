"""
FastAPI endpoint for Claude Agent SDK with Databricks integration.

This API exposes all SDK demo examples as REST endpoints:
- /demo/basic-query         : Simple one-shot query
- /demo/query-with-options  : Query with configuration
- /demo/streaming-client    : Interactive streaming client
- /demo/multi-turn          : Multi-turn conversation
- /demo/tool-permissions    : Hooks for tool monitoring
- /demo/mcp-tools           : Custom MCP server with tools
- /demo/message-types       : Demonstrate message type handling
- /demo/error-handling      : Error handling patterns
"""

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

# Databricks FM API configuration - must be set before importing SDK
databricks_host = os.getenv("DATABRICKS_HOST", "")
# Handle if host already includes https:// or /serving-endpoints
if databricks_host.startswith("https://"):
    base_url = databricks_host.rstrip("/")
else:
    base_url = f"https://{databricks_host}"
if not base_url.endswith("/serving-endpoints"):
    base_url = f"{base_url}/serving-endpoints"
os.environ["ANTHROPIC_BASE_URL"] = f"{base_url}/anthropic"
os.environ["ANTHROPIC_AUTH_TOKEN"] = os.getenv("DATABRICKS_TOKEN", "")
os.environ["ANTHROPIC_API_KEY"] = ""
os.environ["CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS"] = "1"

# Default model for Databricks
DEFAULT_MODEL = "databricks-claude-sonnet-4"

from fastapi import FastAPI
from pydantic import BaseModel, Field

# Import SDK components
from claude_agent_sdk import (
    # Core functions
    query,
    ClaudeSDKClient,
    # Configuration
    ClaudeAgentOptions,
    # Message types
    AssistantMessage,
    ResultMessage,
    UserMessage,
    SystemMessage,
    # Content blocks
    TextBlock,
    ToolUseBlock,
    # Hooks
    HookMatcher,
    HookContext,
    # MCP integration
    tool,
    create_sdk_mcp_server,
    # Errors
    CLINotFoundError,
    CLIConnectionError,
    ProcessError,
    CLIJSONDecodeError,
)
from claude_agent_sdk.types import HookInput, HookJSONOutput

app = FastAPI(
    title="Claude Agent SDK API",
    description="REST API exposing Claude Agent SDK demo examples",
    version="1.0.0",
)


# =============================================================================
# Pydantic Models
# =============================================================================

class ChatRequest(BaseModel):
    prompt: str
    model: str = DEFAULT_MODEL


class ChatResponse(BaseModel):
    response: str


# Basic Query
class BasicQueryRequest(BaseModel):
    prompt: str = Field(..., description="The prompt to send to Claude")


class BasicQueryResponse(BaseModel):
    response: str
    duration_ms: int | None = None
    cost_usd: float | None = None


# Query with Options
class QueryWithOptionsRequest(BaseModel):
    prompt: str
    system_prompt: str | None = Field(None, description="Custom system prompt")
    max_turns: int = Field(1, description="Maximum conversation turns")
    allowed_tools: list[str] = Field(default_factory=list, description="List of allowed tools")


class QueryWithOptionsResponse(BaseModel):
    response: str
    duration_ms: int | None = None


# Streaming Client
class StreamingClientRequest(BaseModel):
    prompt: str
    system_prompt: str | None = Field(
        "You are a helpful coding assistant. Be concise.",
        description="Custom system prompt"
    )
    follow_up: str | None = Field(None, description="Optional follow-up question")


class StreamingClientResponse(BaseModel):
    initial_response: str
    follow_up_response: str | None = None


# Multi-turn
class MultiTurnRequest(BaseModel):
    questions: list[str] = Field(..., description="List of questions for multi-turn conversation")


class TurnResult(BaseModel):
    question: str
    response: str


class MultiTurnResponse(BaseModel):
    turns: list[TurnResult]


# Tool Permissions (Hooks)
class ToolPermissionsRequest(BaseModel):
    prompt: str = Field(
        "List the files in the current directory using ls",
        description="Prompt that will trigger tool usage"
    )


class AuditEntry(BaseModel):
    event: str
    tool: str
    input: dict | None = None
    result_preview: str | None = None


class ToolPermissionsResponse(BaseModel):
    response: str
    audit_log: list[AuditEntry]


# MCP Tools
class MCPToolsRequest(BaseModel):
    prompt: str = Field(..., description="Prompt to use custom MCP tools")
    tool_to_use: str = Field(
        "calculator",
        description="Which tool to demonstrate: calculator, greeting, or random_fact"
    )


class MCPToolsResponse(BaseModel):
    response: str
    tool_used: str | None = None
    tool_input: dict | None = None


# Message Types
class MessageTypesRequest(BaseModel):
    prompt: str = Field(
        "What is the current date? Use the 'date' command.",
        description="Prompt that triggers tool usage to show message types"
    )


class MessageTypesResponse(BaseModel):
    response: str
    message_counts: dict[str, int]
    block_counts: dict[str, int]
    duration_ms: int | None = None


# Error Handling
class ErrorHandlingRequest(BaseModel):
    prompt: str = Field("Hello, Claude!", description="Prompt to send")


class ErrorHandlingResponse(BaseModel):
    response: str | None = None
    error: str | None = None
    error_type: str | None = None
    duration_ms: int | None = None


# =============================================================================
# MCP Tool Definitions (for /demo/mcp-tools)
# =============================================================================

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


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Original chat endpoint for backward compatibility."""
    options = ClaudeAgentOptions(
        model=request.model,
        system_prompt="You are a helpful assistant. Be concise."
    )

    async with ClaudeSDKClient(options=options) as client:
        await client.query(request.prompt)

        response = ""
        async for message in client.receive_response():
            response += str(message)

    return ChatResponse(response=response)


# =============================================================================
# Demo Endpoints
# =============================================================================

@app.post("/demo/basic-query", response_model=BasicQueryResponse)
async def demo_basic_query(request: BasicQueryRequest):
    """
    DEMO 1: Basic Query - Simple one-shot interaction.

    The simplest way to interact with Claude Code.
    query() is ideal for fire-and-forget operations.
    """
    response_text = ""
    duration_ms = None
    cost_usd = None

    options = ClaudeAgentOptions(model=DEFAULT_MODEL)
    async for message in query(prompt=request.prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    response_text += block.text
        elif isinstance(message, ResultMessage):
            duration_ms = message.duration_ms
            cost_usd = message.total_cost_usd

    return BasicQueryResponse(
        response=response_text,
        duration_ms=duration_ms,
        cost_usd=cost_usd,
    )


@app.post("/demo/query-with-options", response_model=QueryWithOptionsResponse)
async def demo_query_with_options(request: QueryWithOptionsRequest):
    """
    DEMO 2: Query with Options - Customize Claude's behavior.

    Demonstrates ClaudeAgentOptions for customizing:
    - System prompts
    - Allowed tools
    - Max turns
    - Working directory
    """
    options = ClaudeAgentOptions(
        model=DEFAULT_MODEL,
        system_prompt=request.system_prompt or "You are a helpful assistant. Be concise.",
        max_turns=request.max_turns,
        allowed_tools=request.allowed_tools if request.allowed_tools else ["Read"],
        cwd=Path.cwd(),
    )

    response_text = ""
    duration_ms = None

    async for message in query(prompt=request.prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    response_text += block.text
        elif isinstance(message, ResultMessage):
            duration_ms = message.duration_ms

    return QueryWithOptionsResponse(
        response=response_text,
        duration_ms=duration_ms,
    )


@app.post("/demo/streaming-client", response_model=StreamingClientResponse)
async def demo_streaming_client(request: StreamingClientRequest):
    """
    DEMO 3: Streaming Client - Interactive conversations.

    ClaudeSDKClient provides:
    - Bidirectional communication
    - Multi-turn conversations with context
    - Interrupt capabilities
    - Session management
    """
    options = ClaudeAgentOptions(
        model=DEFAULT_MODEL,
        system_prompt=request.system_prompt or "You are a helpful coding assistant. Be concise.",
        max_turns=3,
    )

    initial_response = ""
    follow_up_response = None

    async with ClaudeSDKClient(options=options) as client:
        # First query
        await client.query(request.prompt)
        async for msg in client.receive_response():
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    if isinstance(block, TextBlock):
                        initial_response += block.text

        # Follow-up if provided
        if request.follow_up:
            follow_up_response = ""
            await client.query(request.follow_up)
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            follow_up_response += block.text

    return StreamingClientResponse(
        initial_response=initial_response,
        follow_up_response=follow_up_response,
    )


@app.post("/demo/multi-turn", response_model=MultiTurnResponse)
async def demo_multi_turn(request: MultiTurnRequest):
    """
    DEMO 4: Multi-turn Conversation - Full dialogue example.

    Demonstrates maintaining conversation context across multiple turns.
    """
    turns: list[TurnResult] = []

    async with ClaudeSDKClient(options=ClaudeAgentOptions(model=DEFAULT_MODEL, max_turns=5)) as client:
        for question in request.questions:
            await client.query(question)
            response_text = ""
            async for msg in client.receive_response():
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            response_text += block.text

            turns.append(TurnResult(question=question, response=response_text))

    return MultiTurnResponse(turns=turns)


@app.post("/demo/tool-permissions", response_model=ToolPermissionsResponse)
async def demo_tool_permissions(request: ToolPermissionsRequest):
    """
    DEMO 5: Hooks - Event-driven tool monitoring.

    Demonstrates hooks for event-driven tool monitoring:
    - PreToolUse: Called before a tool executes (can block/modify)
    - PostToolUse: Called after a tool executes (for logging/auditing)
    """
    tool_audit_log: list[dict[str, Any]] = []

    async def pre_tool_hook(
        input_data: HookInput,
        session_id: str | None,
        context: HookContext
    ) -> HookJSONOutput:
        """Called before each tool execution."""
        if input_data.get("hook_event_name") != "PreToolUse":
            return {}  # type: ignore

        tool_name = input_data.get("tool_name", "unknown")
        tool_input = input_data.get("tool_input", {})

        tool_audit_log.append({
            "event": "pre_tool_use",
            "tool": tool_name,
            "input": tool_input,
        })

        # Block dangerous bash commands
        if tool_name == "Bash":
            command = tool_input.get("command", "")
            dangerous_patterns = ["rm -rf", "sudo", "> /dev/", "mkfs"]
            for pattern in dangerous_patterns:
                if pattern in command:
                    return {"decision": "block", "reason": f"Dangerous: {pattern}"}  # type: ignore

        return {}  # type: ignore

    async def post_tool_hook(
        input_data: HookInput,
        session_id: str | None,
        context: HookContext
    ) -> HookJSONOutput:
        """Called after each tool execution."""
        if input_data.get("hook_event_name") != "PostToolUse":
            return {}  # type: ignore

        tool_name = input_data.get("tool_name", "unknown")
        tool_result = input_data.get("tool_result", "")

        result_preview = str(tool_result)[:100] + "..." if len(str(tool_result)) > 100 else str(tool_result)

        tool_audit_log.append({
            "event": "post_tool_use",
            "tool": tool_name,
            "result_preview": result_preview,
        })

        return {}  # type: ignore

    hooks: dict[str, list[HookMatcher]] = {
        "PreToolUse": [
            HookMatcher(matcher=None, hooks=[pre_tool_hook])  # type: ignore
        ],
        "PostToolUse": [
            HookMatcher(matcher=None, hooks=[post_tool_hook])  # type: ignore
        ],
    }

    options = ClaudeAgentOptions(
        model=DEFAULT_MODEL,
        allowed_tools=["Read", "Bash", "Glob"],
        system_prompt="You are a helpful assistant. Be brief.",
        permission_mode='acceptEdits',
        hooks=hooks,  # type: ignore
    )

    response_text = ""

    async with ClaudeSDKClient(options=options) as client:
        await client.query(request.prompt)

        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        response_text += block.text

    audit_entries = [
        AuditEntry(
            event=entry["event"],
            tool=entry["tool"],
            input=entry.get("input"),
            result_preview=entry.get("result_preview"),
        )
        for entry in tool_audit_log
    ]

    return ToolPermissionsResponse(
        response=response_text,
        audit_log=audit_entries,
    )


@app.post("/demo/mcp-tools", response_model=MCPToolsResponse)
async def demo_mcp_tools(request: MCPToolsRequest):
    """
    DEMO 6: MCP Tools - Custom in-process tools.

    Demonstrates creating custom MCP tools that run in-process.
    Available tools: calculator, greeting, random_fact
    """
    mcp_server_config = create_sdk_mcp_server(
        name="demo_tools",
        tools=[calculator_tool, greeting_tool, random_fact_tool]
    )

    options = ClaudeAgentOptions(
        model=DEFAULT_MODEL,
        mcp_servers={"demo_tools": mcp_server_config},
        allowed_tools=[
            "mcp__demo_tools__calculator",
            "mcp__demo_tools__greeting",
            "mcp__demo_tools__random_fact",
        ],
        system_prompt="You have access to custom tools: calculator, greeting, random_fact. Use them directly. Be concise.",
        max_turns=5,
        permission_mode='acceptEdits',
    )

    response_text = ""
    tool_used = None
    tool_input = None

    async with ClaudeSDKClient(options=options) as client:
        await client.query(request.prompt)
        async for message in client.receive_response():
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        response_text += block.text
                    elif isinstance(block, ToolUseBlock):
                        tool_used = block.name
                        tool_input = block.input

    return MCPToolsResponse(
        response=response_text,
        tool_used=tool_used,
        tool_input=tool_input,
    )


@app.post("/demo/message-types", response_model=MessageTypesResponse)
async def demo_message_types(request: MessageTypesRequest):
    """
    DEMO 7: Message Types - Understanding response structure.

    Demonstrates all message types and content blocks:
    - UserMessage, AssistantMessage, SystemMessage, ResultMessage
    - TextBlock, ThinkingBlock, ToolUseBlock, ToolResultBlock
    """
    options = ClaudeAgentOptions(
        model=DEFAULT_MODEL,
        allowed_tools=["Bash"],
        system_prompt="You are a helpful assistant. Use tools when needed.",
        max_turns=2,
    )

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

    response_text = ""
    duration_ms = None

    async for message in query(prompt=request.prompt, options=options):
        msg_type = type(message).__name__
        if msg_type in message_counts:
            message_counts[msg_type] += 1

        if isinstance(message, UserMessage):
            pass  # Just count

        elif isinstance(message, AssistantMessage):
            for block in message.content:
                block_type = type(block).__name__
                if block_type in block_counts:
                    block_counts[block_type] += 1

                if isinstance(block, TextBlock):
                    response_text += block.text

        elif isinstance(message, SystemMessage):
            pass  # Just count

        elif isinstance(message, ResultMessage):
            duration_ms = message.duration_ms

    return MessageTypesResponse(
        response=response_text,
        message_counts=message_counts,
        block_counts=block_counts,
        duration_ms=duration_ms,
    )


@app.post("/demo/error-handling", response_model=ErrorHandlingResponse)
async def demo_error_handling(request: ErrorHandlingRequest):
    """
    DEMO 8: Error Handling - Graceful error management.

    Demonstrates error handling patterns for:
    - CLINotFoundError: Claude CLI not installed
    - CLIConnectionError: Failed to start CLI
    - ProcessError: CLI crashed
    - CLIJSONDecodeError: Malformed response
    """
    response_text = None
    error_msg = None
    error_type = None
    duration_ms = None

    try:
        options = ClaudeAgentOptions(model=DEFAULT_MODEL)
        async for message in query(prompt=request.prompt, options=options):
            if isinstance(message, AssistantMessage):
                if response_text is None:
                    response_text = ""
                for block in message.content:
                    if isinstance(block, TextBlock):
                        response_text += block.text
            elif isinstance(message, ResultMessage):
                duration_ms = message.duration_ms
                if message.is_error:
                    error_msg = "Error in result"
                    error_type = "ResultError"

    except CLINotFoundError as e:
        error_msg = str(e)
        error_type = "CLINotFoundError"

    except CLIConnectionError as e:
        error_msg = str(e)
        error_type = "CLIConnectionError"

    except ProcessError as e:
        error_msg = f"Exit code {e.exit_code}: {e.stderr}"
        error_type = "ProcessError"

    except CLIJSONDecodeError as e:
        error_msg = str(e)
        error_type = "CLIJSONDecodeError"

    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__

    return ErrorHandlingResponse(
        response=response_text,
        error=error_msg,
        error_type=error_type,
        duration_ms=duration_ms,
    )


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
