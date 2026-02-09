#!/bin/bash
#
# Setup script for databricks-mcp-server
# Creates virtual environment and installs dependencies
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_CORE_DIR="../databricks-tools-core"

echo "======================================"
echo "Setting up Databricks MCP Server"
echo "======================================"
echo ""

# Check for uv
if ! command -v uv &> /dev/null; then
    echo "Error: 'uv' is not installed."
    echo "Install it with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi
echo "✓ uv is installed"

# Check if tools-core directory exists
if [ ! -d "$SCRIPT_DIR/$TOOLS_CORE_DIR" ]; then
    echo "Error: databricks-tools-core not found at $SCRIPT_DIR/$TOOLS_CORE_DIR"
    exit 1
fi
echo "✓ databricks-tools-core found"

cd "$SCRIPT_DIR"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
uv venv --python 3.11
echo "✓ Virtual environment created"

# Install packages
echo ""
echo "Installing databricks-tools-core (editable)..."
uv pip install --python .venv/bin/python -e "$SCRIPT_DIR/$TOOLS_CORE_DIR" --quiet
echo "✓ databricks-tools-core installed"

echo ""
echo "Installing databricks-mcp-server (editable)..."
uv pip install --python .venv/bin/python -e . --quiet
echo "✓ databricks-mcp-server installed"

# Verify
echo ""
echo "Verifying installation..."
if .venv/bin/python -c "import databricks_mcp_server; print('✓ MCP server can be imported')"; then
    echo ""
    echo "======================================"
    echo "Setup complete!"
    echo "======================================"
    echo ""
    echo "To run the MCP server:"
    echo "  .venv/bin/python run_server.py"
    echo ""
    echo "To setup in the project, paste this into .mcp.json (Claude) or .cursor/mcp.json (Cursor):"
    cat <<EOF
    {
      "mcpServers": {
        "databricks": {
          "command": "${SCRIPT_DIR}/.venv/bin/python",
          "args": ["${SCRIPT_DIR}/run_server.py"]
        }
      }
    }
EOF
    echo ""
    echo "To setup with Claude Code CLI:"
    echo "  claude mcp add-json databricks '{\"command\":\"$SCRIPT_DIR/.venv/bin/python\",\"args\":[\"$SCRIPT_DIR/run_server.py\"]}'"
    echo ""
else
    echo "Error: Failed to import databricks_mcp_server"
    exit 1
fi
