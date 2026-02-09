# Claude Agent SDK Stub Projects

A stub folder containing many example Claude Agent SDK projects.


## Projects

### `claude-agent-sdk-tutorial/`

A comprehensive tutorial for the Claude Agent SDK with Databricks integration.

- **`scripts/`** - Step-by-step tutorial scripts:
  - `query.py` - Simple one-shot query
  - `sdk-client.py` - Streaming client with multi-turn conversation
  - `sdk_demo.py` - Comprehensive demo covering all SDK features (basic query, options, streaming, multi-turn, hooks, MCP tools, message types, error handling)
  - `interactive_chat.py` - Interactive chat loop
  - `mcp_with_permission.py` - MCP tools with risk-based permission hooks
- **`app.py`** - FastAPI app deployed to Databricks Apps, exposing all SDK patterns as REST endpoints
- **`app.yaml`** - Databricks Apps deployment configuration

#### Getting Started

1. Copy `.env.example` to `.env` and fill in your Databricks credentials
2. Install dependencies: `uv sync`
3. Run a tutorial script: `uv run python scripts/sdk_demo.py basic_query`
4. Or start the FastAPI app: `uv run uvicorn app:app --reload`

### `workflows-with-claude-agent-sdk/`

End-to-end workflows built with the Claude Agent SDK on Databricks.

- **`scripts/`** - Utility scripts for data generation, chart creation, report generation, and Databricks job management
- **`jobs/`** - Databricks job definitions for the BJs executive reporting pipeline
- **`data/`** - Sample datasets (customer golden view, store sales monthly)
- **`reports/`** - Generated report charts
