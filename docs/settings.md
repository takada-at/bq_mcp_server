# BQ MCP Server Settings

This document describes all available settings for the BQ MCP server.
Settings can be configured via command-line arguments or environment variables.

## Priority

When both command-line arguments and environment variables are set, command-line arguments take priority.

## Settings

### --gcp-service-account-key-path

**Description**: Path to GCP service account JSON key file (uses Application Default Credentials by default)

**Environment Variable**: `GCP_SERVICE_ACCOUNT_KEY_PATH`

**Usage**:
```bash
python -m bq_mcp.adapters.mcp_server --gcp-service-account-key-path "value"
```

### --project-ids

**Description**: Comma-separated list of GCP project IDs (e.g., 'project1,project2')

**Environment Variable**: `PROJECT_IDS`
  - Default: Required (no default value)

**Usage**:
```bash
python -m bq_mcp.adapters.mcp_server --project-ids "value"
```

### --dataset-filters

**Description**: Comma-separated list of dataset filters (e.g., 'project1.*,project2.dataset1')

**Environment Variable**: `DATASET_FILTERS`

**Usage**:
```bash
python -m bq_mcp.adapters.mcp_server --dataset-filters "value"
```

### --cache-ttl-seconds

**Description**: Cache TTL in seconds (default: 3600)

**Type**: `int`

**Environment Variable**: `CACHE_TTL_SECONDS`
  - Default: `3600 seconds`

**Usage**:
```bash
python -m bq_mcp.adapters.mcp_server --cache-ttl-seconds 3600
```

### --cache-file-base-dir

**Description**: Base directory for cache files

**Environment Variable**: `CACHE_FILE_BASE_DIR`
  - Default: `.bq_metadata_cache`

**Usage**:
```bash
python -m bq_mcp.adapters.mcp_server --cache-file-base-dir "value"
```

### --query-execution-project-id

**Description**: Project ID to use for query execution (defaults to first project in project-ids)

**Environment Variable**: `QUERY_EXECUTION_PROJECT_ID`

**Usage**:
```bash
python -m bq_mcp.adapters.mcp_server --query-execution-project-id "value"
```

## Environment Variables Reference

The following environment variables are used by the BQ MCP server:

| Variable | Description | Type | Default |
| --- | --- | --- | --- |
| `API_HOST` | API server hostname | str | `127.0.0.1` |
| `API_PORT` | API server port number | int | `8000` |
| `CACHE_FILE_BASE_DIR` | Base directory for cache files | str | `.bq_metadata_cache` |
| `CACHE_TTL_SECONDS` | Cache TTL in seconds | int | `3600 seconds` |
| `DATASET_FILTERS` | Comma-separated list of dataset filters (e.g., 'project1.*,project2.dataset1') | list[str] | `None` |
| `DEFAULT_QUERY_LIMIT` | Default query result limit | int | `100` |
| `ENABLE_FILE_LOGGING` | Whether to enable file logging | bool | `False` |
| `GCP_SERVICE_ACCOUNT_KEY_PATH` | Path to GCP service account JSON key file (uses Application Default Credentials by default) | str | `None` |
| `MAX_SCAN_BYTES` | Maximum scan bytes for queries | int | `1GB (1,073,741,824 bytes)` |
| `PROJECT_IDS` | Comma-separated list of GCP project IDs (e.g., 'project1,project2') | list[str] | `Required` |
| `QUERY_EXECUTION_PROJECT_ID` | Project ID to use for query execution (defaults to first project in project-ids) | str | `None` |
| `QUERY_TIMEOUT_SECONDS` | Query timeout in seconds | int | `300 seconds` |
