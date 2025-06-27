import traceback
from typing import List, Optional

from fastapi import HTTPException

from bq_mcp.core.entities import (
    CachedData,
    DatasetListResponse,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    TableMetadata,
)
from bq_mcp.repositories import cache_manager, config, log
from bq_mcp.repositories.query_executor import QueryExecutor


# --- Helper Functions ---
async def get_current_cache() -> CachedData:
    """Get current valid cache data. Raise error if not available."""
    logger = log.get_logger()
    cache = cache_manager.load_cache()  # First try from memory/file
    if cache and cache_manager.is_cache_valid(cache):
        return cache
    # Try to update if cache is invalid or doesn't exist
    logger.info("Cache is invalid or doesn't exist, attempting to update...")
    # cache_manager.update_cache() is now async
    updated_cache = await cache_manager.update_cache()
    if not updated_cache:
        logger.error("Failed to update cache.")
        raise HTTPException(
            status_code=503,
            detail="Failed to retrieve cache data. Server is unavailable.",
        )
    return updated_cache


async def get_datasets() -> DatasetListResponse:
    """Return list of datasets from all projects"""
    # Get dataset list from global cache
    logger = log.get_logger()
    try:
        cache = await get_current_cache()
        all_datasets: List[DatasetMetadata] = []
        for project_datasets in cache.datasets.values():
            all_datasets.extend(project_datasets)
        return DatasetListResponse(datasets=all_datasets)
    except HTTPException:  # Specific HTTPException should be re-raised
        raise
    except Exception as e:
        logger.error(f"Error occurred while retrieving dataset list: {e}")
        logger.error(traceback.format_exception(e))
        raise HTTPException(
            status_code=503,
            detail="Failed to retrieve dataset list. Server is unavailable.",
        )


async def get_datasets_by_project(project_id: str) -> DatasetListResponse:
    """Return list of datasets for the specified project"""
    cache = await get_current_cache()
    if project_id not in cache.datasets:
        raise HTTPException(
            status_code=404,
            detail=f"Project '{project_id}' not found.",
        )
    return DatasetListResponse(datasets=cache.datasets[project_id])


async def get_tables(
    dataset_id: str, project_id: Optional[str] = None
) -> List[TableMetadata]:
    """Return list of tables for the specified dataset
    Args:
        dataset_id: Dataset ID
        project_id: Project ID (optional)
    Returns:
        List[TableMetadata]: List of table metadata
    Raises:
        HTTPException: When project or dataset is not found
    """
    found_tables: List[TableMetadata] = []
    if project_id:
        # If project ID is specified, get cache for that dataset directly
        # cache_manager.get_cached_dataset_data is now async
        dataset, tables = await cache_manager.get_cached_dataset_data(
            project_id, dataset_id
        )
        if dataset is None:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{project_id}.{dataset_id}' not found.",
            )
        return tables
    else:
        # If project ID is not specified, search across all projects
        cache = await get_current_cache()
        found_dataset = False
        settings = config.get_settings()

        for proj_id in settings.project_ids:
            if proj_id in cache.tables and dataset_id in cache.tables[proj_id]:
                found_dataset = True
                # cache_manager.get_cached_dataset_data is now async
                dataset, tables = await cache_manager.get_cached_dataset_data(
                    proj_id, dataset_id
                )
                if dataset is not None and tables:
                    found_tables.extend(tables)

        if not found_dataset:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{dataset_id}' not found.",
            )

        return found_tables


async def check_query_scan_amount(
    sql: str, project_id: Optional[str] = None
) -> QueryDryRunResult:
    """Check BigQuery scan amount in advance (original query as-is)"""
    logger = log.get_logger()
    settings = config.get_settings()

    query_executor = QueryExecutor(settings)
    result = await query_executor.check_scan_amount(sql, project_id)

    logger.info(f"Scan amount check completed: {result.total_bytes_processed:,} bytes")
    return result


async def execute_query(
    sql: str, project_id: Optional[str] = None, force: bool = False
) -> QueryExecutionResult:
    """Execute BigQuery query safely"""

    logger = log.get_logger()
    settings = config.get_settings()

    query_executor = QueryExecutor(settings)
    result = await query_executor.execute_query(sql, project_id, force_execute=force)

    if result.success:
        logger.info(f"Query execution successful - result rows: {result.total_rows}")
    else:
        logger.warning(f"Query execution failed: {result.error_message}")

    return result
