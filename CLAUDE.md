# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Testing
- `pytest` - Run all tests
- `pytest tests/test_logic.py` - Run specific test file
- `pytest -k test_function_name` - Run specific test

### Code Quality
- `ruff check` - Run linting checks
- `ruff format` - Format code
- `lizard` - Run cyclomatic complexity analysis

### Package Management
- Uses `uv` for dependency management
- `uv sync` - Install dependencies (including dev dependencies)
- `uv add <package>` - Add new dependency
- `uv add --dev <package>` - Add development dependency

### Scripts
- `python scripts/generate_env_example.py` - Generate .env.example from config.py

## High-Level Architecture

This is a BigQuery metadata API server that provides access to BigQuery dataset, table, and schema information through both REST API and MCP (Model Context Protocol) server interfaces.

### Core Components

**Domain Layer (`bq_mcp/core/`)**:
- `entities.py` - Pydantic models for data structures (DatasetMetadata, TableMetadata, CachedData, QueryExecutionRequest, etc.)
- `logic.py` - Business logic for retrieving datasets and tables with caching
- `converter.py` - Converts data structures to markdown format for API responses
- `query_parser.py` - SQL query parsing and LIMIT clause manipulation with safety checks

**Repository Layer (`bq_mcp/repositories/`)**:
- `bigquery_client.py` - Google Cloud BigQuery API client wrapper
- `cache_manager.py` - Manages local caching of BigQuery metadata with TTL
- `search_engine.py` - Full-text search functionality across cached metadata
- `query_executor.py` - BigQuery query execution with cost control and safety measures
- `config.py` - Application configuration management (environment variables, project IDs)
- `log.py` - Centralized logging configuration

**Adapter Layer (`bq_mcp/adapters/`)**:
- `mcp_server.py` - MCP server implementation with tools: `get_datasets`, `get_tables`, `search_metadata`, `execute_query`
- `web.py` - FastAPI REST endpoints including `/query/execute`
- `bq_agent_gradio.py` - Gradio web interface
- `agent.py` - AI agent integration using pydantic-ai

### Key Patterns

**Caching Strategy**: The application maintains a hierarchical cache structure:
- Project → Dataset → Tables with metadata
- Cache validation based on TTL and automatic refresh
- Uses both in-memory and persistent storage

**Error Handling**: Consistent HTTP exception handling with Japanese error messages in the business logic layer.

**Async Architecture**: All data retrieval operations are async to handle BigQuery API calls efficiently.

**Configuration**: Uses environment variables for GCP authentication and project configuration. Check `repositories/config.py` for available settings.

### MCP Server Usage
The MCP server (`mcp_server.py`) is the primary interface, providing four main tools for BigQuery metadata access and query execution:
1. `get_datasets` - Lists all available datasets across configured projects
2. `get_tables` - Lists tables in a specific dataset (requires dataset_id, optional project_id)
3. `search_metadata` - Searches across datasets, tables, and columns using keywords
4. `execute_query` - Executes BigQuery SQL with automatic safety checks and LIMIT clause management

**Query Execution Features:**
- Automatic LIMIT clause addition/modification (default: 10 rows)
- Automatic dry-run validation to check scan amount before execution
- Cost control with configurable scan limits (default: 1GB)
- Safety checks to prevent dangerous SQL operations (DELETE, DROP, etc.)
- Detailed execution results with resource usage information

**Configuration Environment Variables:**
- `MAX_SCAN_BYTES` - Maximum allowed scan bytes for queries (default: 1GB)
- `DEFAULT_QUERY_LIMIT` - Default LIMIT value added to queries (default: 10)
- `QUERY_TIMEOUT_SECONDS` - Query execution timeout (default: 300 seconds)

Run the MCP server with: `python -m bq_mcp.adapters.mcp_server`