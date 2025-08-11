## 0.2.0 - 2025-08-11
### Added
- **New MCP tool: `save_query_result`** - Execute BigQuery SQL queries and save results to local files
  - Support for CSV and JSONL output formats
  - No automatic LIMIT clause insertion (unlike `execute_query`)
  - Path validation for security (prevents directory traversal attacks)
  - Maintains cost control with scan amount limits (default: 1GB)
  - Configurable CSV header inclusion

## 0.1.4 - 2025-08-09
### Fix
- Supports mcp library version 1.12.3 and above. Removes the description parameter.

## 0.1.3 - 2025-08-01
### Changed
- Change the default cache storage location

## 0.1.2 - 2025-07-22
### Changed
- Change Script: mcp_server -> bq_mcp_server

## 0.1.1 - 2025-07-22
### Changed
- Organized dependency libraries

## 0.1.0 - 2025-07-21
### Added
- First Published Version