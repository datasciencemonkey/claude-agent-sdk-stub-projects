"""Dynamic tool loader for Databricks tools.

Scans FastMCP tools from databricks-mcp-server and creates
in-process SDK tools for the Claude Code Agent SDK.
"""

import asyncio
import json
import logging
from contextvars import copy_context
from typing import Any

from claude_agent_sdk import tool, create_sdk_mcp_server

logger = logging.getLogger(__name__)


def load_databricks_tools():
    """Dynamically scan FastMCP tools and create in-process SDK MCP server.

    Returns:
        Tuple of (server_config, tool_names) where:
        - server_config: McpSdkServerConfig for ClaudeAgentOptions.mcp_servers
        - tool_names: List of tool names in mcp__databricks__* format
    """
    # Import triggers @mcp.tool registration
    from databricks_mcp_server.server import mcp
    from databricks_mcp_server.tools import sql, compute, file, pipelines  # noqa: F401

    sdk_tools = []
    tool_names = []

    for name, mcp_tool in mcp._tool_manager._tools.items():
        input_schema = _convert_schema(mcp_tool.parameters)
        sdk_tool = _make_wrapper(name, mcp_tool.description, input_schema, mcp_tool.fn)
        sdk_tools.append(sdk_tool)
        tool_names.append(f'mcp__databricks__{name}')

    logger.info(f'Loaded {len(sdk_tools)} Databricks tools: {[n.split("__")[-1] for n in tool_names]}')

    server = create_sdk_mcp_server(name='databricks', tools=sdk_tools)
    return server, tool_names


def _convert_schema(json_schema: dict) -> dict[str, type]:
    """Convert JSON schema to SDK simple format: {"param": type}"""
    type_map = {
        'string': str,
        'integer': int,
        'number': float,
        'boolean': bool,
        'array': list,
        'object': dict,
    }
    result = {}

    for param, spec in json_schema.get('properties', {}).items():
        # Handle anyOf (optional types like "string | null")
        if 'anyOf' in spec:
            for opt in spec['anyOf']:
                if opt.get('type') != 'null':
                    result[param] = type_map.get(opt.get('type'), str)
                    break
        else:
            result[param] = type_map.get(spec.get('type'), str)

    return result


def _make_wrapper(name: str, description: str, schema: dict, fn):
    """Create SDK tool wrapper for a FastMCP function.

    The wrapper runs the sync function in a thread pool to avoid
    blocking the async event loop. It also handles JSON string parsing
    for complex types (lists, dicts) that the Claude agent may pass as strings.

    Includes a heartbeat mechanism that prints periodic status updates to stderr
    during long-running operations to keep the MCP connection alive.
    """

    @tool(name, description, schema)
    async def wrapper(args: dict[str, Any]) -> dict[str, Any]:
        import sys
        import traceback
        import time
        import concurrent.futures

        start_time = time.time()
        print(f'[MCP TOOL] {name} called with args: {args}', file=sys.stderr, flush=True)
        logger.info(f'[MCP] Tool {name} called with args: {args}')
        try:
            # Parse JSON strings for complex types (Claude agent sometimes sends these as strings)
            parsed_args = {}
            for key, value in args.items():
                if isinstance(value, str) and value.strip().startswith(('[', '{')):
                    # Try to parse as JSON if it looks like a list or dict
                    try:
                        parsed_args[key] = json.loads(value)
                        print(f'[MCP TOOL] Parsed {key} from JSON string', file=sys.stderr, flush=True)
                    except json.JSONDecodeError:
                        # Not valid JSON, keep as string
                        parsed_args[key] = value
                else:
                    parsed_args[key] = value

            # FastMCP tools are sync - run in thread pool with heartbeat
            print(f'[MCP TOOL] Running {name} in thread pool with heartbeat...', file=sys.stderr, flush=True)

            # Copy context to propagate Databricks auth contextvars to the thread
            ctx = copy_context()

            def run_in_context():
                """Run the tool function within the copied context."""
                return ctx.run(fn, **parsed_args)

            # Run tool in executor so we can poll for completion with heartbeat
            loop = asyncio.get_event_loop()
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            future = loop.run_in_executor(executor, run_in_context)

            # Heartbeat every 10 seconds while waiting for the tool to complete
            HEARTBEAT_INTERVAL = 10
            heartbeat_count = 0
            while True:
                try:
                    # Wait for result with timeout
                    result = await asyncio.wait_for(
                        asyncio.shield(future),
                        timeout=HEARTBEAT_INTERVAL
                    )
                    # Tool completed successfully
                    break
                except asyncio.TimeoutError:
                    # Tool still running - emit heartbeat
                    heartbeat_count += 1
                    elapsed = time.time() - start_time
                    print(f'[MCP HEARTBEAT] {name} still running... ({elapsed:.0f}s elapsed, heartbeat #{heartbeat_count})', file=sys.stderr, flush=True)
                    logger.debug(f'[MCP] Heartbeat for {name}: {elapsed:.0f}s elapsed')
                    # Continue waiting
                    continue

            elapsed = time.time() - start_time
            result_str = json.dumps(result, default=str)
            print(f'[MCP TOOL] {name} completed in {elapsed:.2f}s, result length: {len(result_str)}', file=sys.stderr, flush=True)
            logger.info(f'[MCP] Tool {name} completed in {elapsed:.2f}s')
            return {'content': [{'type': 'text', 'text': result_str}]}
        except asyncio.CancelledError:
            elapsed = time.time() - start_time
            error_msg = f'Tool execution cancelled after {elapsed:.2f}s (likely due to stream timeout)'
            print(f'[MCP TOOL] {name} CANCELLED: {error_msg}', file=sys.stderr, flush=True)
            logger.error(f'[MCP] Tool {name} cancelled: {error_msg}')
            return {'content': [{'type': 'text', 'text': f'Error: {error_msg}'}], 'is_error': True}
        except TimeoutError as e:
            elapsed = time.time() - start_time
            error_msg = f'Tool execution timed out after {elapsed:.2f}s: {e}'
            print(f'[MCP TOOL] {name} TIMEOUT: {error_msg}', file=sys.stderr, flush=True)
            logger.error(f'[MCP] Tool {name} timeout: {error_msg}')
            return {'content': [{'type': 'text', 'text': f'Error: {error_msg}'}], 'is_error': True}
        except Exception as e:
            elapsed = time.time() - start_time
            error_details = traceback.format_exc()
            error_msg = f'{type(e).__name__}: {str(e)}'
            print(f'[MCP TOOL] {name} FAILED after {elapsed:.2f}s: {error_msg}', file=sys.stderr, flush=True)
            print(f'[MCP TOOL] Stack trace:\n{error_details}', file=sys.stderr, flush=True)
            logger.exception(f'[MCP] Tool {name} failed after {elapsed:.2f}s: {error_msg}')
            return {'content': [{'type': 'text', 'text': f'Error ({type(e).__name__}): {str(e)}\n\nThis error occurred after {elapsed:.2f}s. If this is a long-running operation, it may have exceeded the stream timeout (50s).'}], 'is_error': True}

    return wrapper
