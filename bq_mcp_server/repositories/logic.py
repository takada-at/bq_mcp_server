"""Repository layer logic with dependency injection"""

import asyncio
import traceback
from typing import List, Optional

from fastapi import HTTPException

from bq_mcp_server.core import logic_base
from bq_mcp_server.core.entities import CachedData, DatasetListResponse, TableMetadata
from bq_mcp_server.repositories import cache_manager, config, log
from bq_mcp_server.repositories.query_executor import QueryExecutor


# --- Private implementation functions ---
async def _get_current_cache_impl() -> CachedData:
    """Get current valid cache data. Raise error if not available."""
    logger = log.get_logger()
    cache = cache_manager.load_cache()  # First try from memory/file
    if cache and cache_manager.is_cache_valid(cache):
        return cache

    # If cache is invalid or doesn't exist, try to return stale cache
    # and trigger background update
    if cache:
        logger.warning(
            "Cache is expired, using stale cache and triggering background update"
        )
        # Start background update without waiting
        asyncio.create_task(_trigger_background_update())
        return cache

    # If no cache exists at all, we need to wait for initial update
    logger.info("No cache exists, performing initial cache load...")
    updated_cache = await cache_manager.update_cache()
    if not updated_cache:
        logger.error("Failed to update cache.")
        raise HTTPException(
            status_code=503,
            detail="Failed to retrieve cache data. Server is unavailable.",
        )
    return updated_cache


async def _trigger_background_update():
    """Trigger background cache update without blocking"""
    logger = log.get_logger()
    try:
        logger.info("Starting background cache update from logic layer")
        updated_cache = await cache_manager.update_cache()
        if updated_cache:
            cache_manager.save_cache(updated_cache)
            logger.info("Background cache update completed")
    except Exception:
        logger.exception("Background cache update failed")


# --- QueryExecutor wrapper functions ---
async def _check_scan_amount_impl(sql: str, project_id: Optional[str] = None):
    """Check query scan amount using QueryExecutor"""
    settings = config.get_settings()
    query_executor = QueryExecutor(settings)
    return await query_executor.check_scan_amount(sql, project_id)


async def _execute_query_impl(sql: str, project_id: Optional[str] = None):
    """Execute query using QueryExecutor"""
    settings = config.get_settings()
    query_executor = QueryExecutor(settings)
    return await query_executor.execute_query(sql, project_id, force_execute=False)


# --- Logger functions ---
def _logger_info(message: str):
    """Log info message"""
    log.get_logger().info(message)


def _logger_warning(message: str):
    """Log warning message"""
    log.get_logger().warning(message)


# --- Create public functions with dependency injection ---
_get_datasets_impl = logic_base.create_get_datasets(
    get_current_cache=_get_current_cache_impl
)

_get_datasets_by_project_impl = logic_base.create_get_datasets_by_project(
    get_current_cache=_get_current_cache_impl
)

_get_tables_impl = logic_base.create_get_tables(
    get_cached_dataset_data=cache_manager.get_cached_dataset_data,
    get_current_cache=_get_current_cache_impl,
    get_project_ids=lambda: config.get_settings().project_ids,
)

check_query_scan_amount = logic_base.create_check_query_scan_amount(
    check_scan_amount=_check_scan_amount_impl, logger=_logger_info
)

execute_query = logic_base.create_execute_query(
    execute_query_impl=_execute_query_impl, logger=_logger_info
)


# --- Public API with error handling ---
async def get_datasets() -> DatasetListResponse:
    """Return list of datasets from all projects with error handling"""
    logger = log.get_logger()
    try:
        return await _get_datasets_impl()
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
    """Return list of datasets for the specified project with error handling"""
    result = await _get_datasets_by_project_impl(project_id)
    if not result.datasets:
        raise HTTPException(
            status_code=404,
            detail=f"Project '{project_id}' not found.",
        )
    return result


async def get_tables(
    dataset_id: str, project_id: Optional[str] = None
) -> List[TableMetadata]:
    """Return list of tables for the specified dataset with error handling

    Args:
        dataset_id: Dataset ID
        project_id: Project ID (optional)

    Returns:
        List[TableMetadata]: List of table metadata

    Raises:
        HTTPException: When project or dataset is not found
    """
    tables = await _get_tables_impl(dataset_id, project_id)

    if not tables:
        if project_id:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{project_id}.{dataset_id}' not found.",
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{dataset_id}' not found.",
            )

    return tables


# Helper function for backward compatibility
async def get_current_cache() -> CachedData:
    """Get current valid cache data. For backward compatibility."""
    return await _get_current_cache_impl()
