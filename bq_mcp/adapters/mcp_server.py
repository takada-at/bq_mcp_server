# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "gcloud-aio-bigquery>=7.1.0",
#     "google-cloud-bigquery>=3.31.0",
#     "pydantic>=2.11.3",
#     "python-dotenv>=1.1.0",
#     "mcp[cli]>=1.6.0",
# ]
# ///
import argparse
import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Optional

from mcp.server.fastmcp import FastMCP

from bq_mcp.core import converter
from bq_mcp.core.entities import ApplicationContext, CachedData
from bq_mcp.repositories import cache_manager, config, log, logic, search_engine


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[ApplicationContext]:
    """Manage application lifecycle with type-safe context"""
    log_setting = log.init_logger(log_to_console=False)
    setting = config.init_setting()
    logger = log.get_logger()

    # Load existing cache without blocking startup
    cache_data = cache_manager.load_cache()

    # If cache is invalid or doesn't exist, start background update
    if not cache_data or not cache_manager.is_cache_valid(cache_data):
        logger.info("Starting background cache update...")
        # Create a background task for cache update
        asyncio.create_task(_background_cache_update())
        # Initialize with empty cache to allow immediate startup
        cache_data = CachedData(last_updated=None)
    else:
        logger.info("Using existing valid cache")

    context = ApplicationContext(
        settings=setting,
        log_setting=log_setting,
        cache_data=cache_data,
    )

    yield context


async def _background_cache_update():
    """Background task to update cache without blocking server startup"""
    logger = log.get_logger()
    try:
        logger.info("Background cache update started")
        updated_cache = await cache_manager.update_cache()
        if updated_cache:
            logger.info("Background cache update completed successfully")
            # Save the updated cache
            cache_manager.save_cache(updated_cache)
        else:
            logger.error("Background cache update failed")
    except Exception:
        logger.exception("Error in background cache update")


mcp = FastMCP(
    "BigQuery MCP",
    description="Provides access to BigQuery dataset, table, and schema information, and allows safe query execution.",
    instructions="""Use search_metadata to search for metadata.
Use get_datasets to retrieve a list of datasets and get_tables to retrieve a list of tables.
Use execute_query to run BigQuery SQL with automatic safety checks and LIMIT clause management.""",
    lifespan=app_lifespan,
    version="0.1.0",
)


@mcp.tool("get_datasets")
async def get_datasets():
    """
    Get list of all datasets
    """
    datasets = await logic.get_datasets()
    markdown_content = converter.convert_datasets_to_markdown(datasets.datasets)
    return markdown_content


@mcp.tool("get_tables")
async def get_tables(dataset_id: str, project_id: Optional[str] = None):
    """
    Get list of all tables in a dataset
    """
    tables = await logic.get_tables(dataset_id, project_id)
    markdown_content = converter.convert_tables_to_markdown(tables)
    return markdown_content


@mcp.tool("search_metadata")
async def search_metadata(key: str):
    """
    Search metadata for datasets, tables, and columns
    """
    results = await search_engine.search_metadata(key)
    markdown_content = converter.convert_search_results_to_markdown(key, results)
    return markdown_content


@mcp.tool("check_query_scan_amount")
async def check_query_scan_amount(sql: str, project_id: Optional[str] = None):
    """
    Check the scan amount of a BigQuery SQL query using dry-run without executing it.

    Args:
        sql: The SQL query to check
        project_id: Optional project ID to use for the query (defaults to first configured project)
    """
    result = await logic.check_query_scan_amount(sql, project_id)
    return converter.convert_dry_run_result_to_markdown(result, project_id)


@mcp.tool("execute_query")
async def execute_query(sql: str, project_id: Optional[str] = None):
    """
    Execute BigQuery SQL with automatic safety checks and LIMIT clause management.

    Args:
        sql: The SQL query to execute
        project_id: Optional project ID to use for the query (defaults to first configured project)
    """
    # Force execution flag is always set to False. No workarounds allowed in MCP.
    result = await logic.execute_query(sql, project_id, force=False)
    return converter.convert_query_result_to_markdown(result, project_id)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="BigQuery MCP Server - Provides access to BigQuery metadata and query execution"
    )
    parser.add_argument(
        "--gcp-service-account-key-path",
        help="Path to GCP service account JSON key file",
    )
    parser.add_argument(
        "--project-ids",
        help="Comma-separated list of GCP project IDs (e.g., 'project1,project2')",
    )
    parser.add_argument(
        "--dataset-filters",
        help="Comma-separated list of dataset filters (e.g., 'project1.*,project2.dataset1')",
    )
    parser.add_argument(
        "--cache-ttl-seconds",
        type=int,
        help="Cache TTL in seconds (default: 3600)",
    )
    parser.add_argument(
        "--cache-file-base-dir",
        help="Base directory for cache files",
    )
    parser.add_argument(
        "--query-execution-project-id",
        help="Project ID to use for query execution (defaults to first project in project-ids)",
    )
    return parser.parse_args()


def apply_args_to_env(args):
    """Apply command line arguments to environment variables (args take priority)"""
    if args.gcp_service_account_key_path:
        os.environ["GCP_SERVICE_ACCOUNT_KEY_PATH"] = args.gcp_service_account_key_path
    if args.project_ids:
        os.environ["PROJECT_IDS"] = args.project_ids
    if args.dataset_filters:
        os.environ["DATASET_FILTERS"] = args.dataset_filters
    if args.cache_ttl_seconds is not None:
        os.environ["CACHE_TTL_SECONDS"] = str(args.cache_ttl_seconds)
    if args.cache_file_base_dir:
        os.environ["CACHE_FILE_BASE_DIR"] = args.cache_file_base_dir
    if args.query_execution_project_id:
        os.environ["QUERY_EXECUTION_PROJECT_ID"] = args.query_execution_project_id


def main():
    """
    Main entry point to run the MCP server.
    """
    # Parse command line arguments and apply them to environment
    args = parse_args()
    apply_args_to_env(args)

    # Run the MCP server
    mcp.run()


if __name__ == "__main__":
    main()
