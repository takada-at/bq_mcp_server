from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from mcp.server.fastmcp import FastMCP
from typing import Optional


from bq_meta_api.core import converter, logic
from bq_meta_api.core.entities import ApplicationContext
from bq_meta_api.repositories import cache_manager, config, log, search_engine


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[ApplicationContext]:
    """Manage application lifecycle with type-safe context"""
    log_setting = log.init_logger(log_to_console=False)
    setting = config.init_setting()
    cache_data = await cache_manager.get_cached_data()
    context = ApplicationContext(
        settings=setting,
        log_setting=log_setting,
        cache_data=cache_data,
    )

    yield context


mcp = FastMCP(
    "BigQuery Metadata API",
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


@mcp.tool("execute_query")
async def execute_query(
    sql: str, project_id: Optional[str] = None, force: bool = False
):
    """
    Execute BigQuery SQL with automatic safety checks and LIMIT clause management.

    Args:
        sql: The SQL query to execute
        project_id: Optional project ID to use for the query (defaults to first configured project)
        force: Force execution even if scan amount exceeds limits (use carefully)
    """
    result = await logic.execute_query(sql, project_id, force)
    return converter.convert_query_result_to_markdown(result, project_id)


if __name__ == "__main__":
    mcp.run()
