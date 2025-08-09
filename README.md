# BigQuery MCP Server

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI-green.svg)](https://fastapi.tiangolo.com/)

This is a Python-based MCP (Model Context Protocol) server that retrieves dataset, table, and schema information from Google Cloud BigQuery, caches it locally, and serves it via MCP. Its primary purpose is to enable generative AI systems to quickly understand BigQuery's structure and execute queries securely.

## Key Features

- **Metadata Management**: Retrieves and caches information about BigQuery datasets, tables, and columns
- **Keyword Search**: Supports keyword search of cached metadata
- **Secure Query Execution**: Provides SQL execution capabilities with automatic LIMIT clause insertion and cost control
- **MCP Compliance**: Offers tools via the Model Context Protocol

## MCP Server Tools

Available tools:

1. `get_datasets` - Retrieves a list of all datasets
2. `get_tables` - Retrieves all tables within a specified dataset (requires dataset_id, optionally accepts project_id)
3. `search_metadata` - Searches metadata for datasets, tables, and columns
4. `execute_query` - Safely executes BigQuery SQL queries with automatic LIMIT clause insertion and cost control
5. `check_query_scan_amount` - Retrieves the scan amount for BigQuery SQL queries

## Installation and Environment Setup
### Prerequisites

- Python 3.11 or later
- Google Cloud Platform account
- GCP project with BigQuery API enabled

### Install
uv

```bash
uv add bq_mcp_server
```

pip

```bash
pip install bq_mcp_server
```

### Installing Dependencies

This project uses `uv` for package management:

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

### Configuring Option

For a list of configuration values, see:

[docs/settings.md](./docs/settings.md)


## MCP Setting

Claude Code

```shell
claude mcp add bq_mcp_server -- uvx --from git+https://github.com/takada-at/bq_mcp_server bq_mcp_server --project-ids <your project ids>
```

JSON

```json
{
    "mcpServers": {
        "bq_mcp_server": {
            "command": "uvx",
            "args": [
                "--from git+https://github.com/takada-at/bq_mcp_server",
                "bq_mcp_server",
                "--project-ids",
                "<your project ids>"
            ]
        }
    }
}
```

## Running Tests

### Running All Tests

```bash
pytest
```

### Running Specific Test Files

```bash
pytest tests/test_logic.py
```

### Running Specific Test Functions

```bash
pytest -k test_function_name
```

### Checking Test Coverage

```bash
pytest --cov=bq_mcp_server
```

## Local Development

### Starting the MCP Server

```bash
uv run bq_mcp_server
```

### Starting the FastAPI REST API Server

```bash
uvicorn bq_mcp_server.adapters.web:app --reload
```

### Development Commands

#### Code Formatting and Linting

```bash
# Code formatting
ruff format

# Linting checks
ruff check

# Automatic fixes
ruff check --fix
```

#### Dependency Management

```bash
# Adding new dependencies
uv add <package>

# Adding development dependencies
uv add --dev <package>

# Updating dependencies
uv sync
```
