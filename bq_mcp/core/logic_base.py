"""Pure business logic layer with dependency injection via higher-order functions"""

from typing import Awaitable, Callable, List, Optional, Tuple

from bq_mcp.core.entities import (
    CachedData,
    DatasetListResponse,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    TableMetadata,
)

# Type aliases for dependency functions
GetCurrentCacheFunc = Callable[[], Awaitable[CachedData]]
GetCachedDatasetFunc = Callable[
    [str, str], Awaitable[Tuple[Optional[DatasetMetadata], List[TableMetadata]]]
]
GetProjectIdsFunc = Callable[[], List[str]]
CheckScanAmountFunc = Callable[[str, Optional[str]], Awaitable[QueryDryRunResult]]
ExecuteQueryFunc = Callable[[str, Optional[str]], Awaitable[QueryExecutionResult]]
LoggerFunc = Callable[[str], None]


def create_get_datasets(
    get_current_cache: GetCurrentCacheFunc,
) -> Callable[[], Awaitable[DatasetListResponse]]:
    """Create get_datasets function with injected dependencies"""

    async def get_datasets() -> DatasetListResponse:
        """Return list of datasets from all projects"""
        cache = await get_current_cache()
        all_datasets: List[DatasetMetadata] = []
        for project_datasets in cache.datasets.values():
            all_datasets.extend(project_datasets)
        return DatasetListResponse(datasets=all_datasets)

    return get_datasets


def create_get_datasets_by_project(
    get_current_cache: GetCurrentCacheFunc,
) -> Callable[[str], Awaitable[DatasetListResponse]]:
    """Create get_datasets_by_project function with injected dependencies"""

    async def get_datasets_by_project(project_id: str) -> DatasetListResponse:
        """Return list of datasets for the specified project"""
        cache = await get_current_cache()
        if project_id not in cache.datasets:
            return DatasetListResponse(datasets=[])
        return DatasetListResponse(datasets=cache.datasets[project_id])

    return get_datasets_by_project


def create_get_tables(
    get_cached_dataset_data: GetCachedDatasetFunc,
    get_current_cache: GetCurrentCacheFunc,
    get_project_ids: GetProjectIdsFunc,
) -> Callable[[str, Optional[str]], Awaitable[List[TableMetadata]]]:
    """Create get_tables function with injected dependencies"""

    async def get_tables(
        dataset_id: str, project_id: Optional[str] = None
    ) -> List[TableMetadata]:
        """Return list of tables for the specified dataset

        Args:
            dataset_id: Dataset ID
            project_id: Project ID (optional)

        Returns:
            List[TableMetadata]: List of table metadata
        """
        if project_id:
            # If project ID is specified, get cache for that dataset directly
            dataset, tables = await get_cached_dataset_data(project_id, dataset_id)
            if dataset is None:
                return []
            return tables
        else:
            # If project ID is not specified, search across all projects
            cache = await get_current_cache()
            found_tables: List[TableMetadata] = []
            project_ids = get_project_ids()

            for proj_id in project_ids:
                if proj_id in cache.tables and dataset_id in cache.tables[proj_id]:
                    dataset, tables = await get_cached_dataset_data(proj_id, dataset_id)
                    if dataset is not None and tables:
                        found_tables.extend(tables)

            return found_tables

    return get_tables


def create_check_query_scan_amount(
    check_scan_amount: CheckScanAmountFunc, logger: LoggerFunc
) -> Callable[[str, Optional[str]], Awaitable[QueryDryRunResult]]:
    """Create check_query_scan_amount function with injected dependencies"""

    async def check_query_scan_amount(
        sql: str, project_id: Optional[str] = None
    ) -> QueryDryRunResult:
        """Check BigQuery scan amount in advance (original query as-is)"""
        result = await check_scan_amount(sql, project_id)
        logger(f"Scan amount check completed: {result.total_bytes_processed:,} bytes")
        return result

    return check_query_scan_amount


def create_execute_query(
    execute_query_impl: ExecuteQueryFunc, logger: LoggerFunc
) -> Callable[[str, Optional[str]], Awaitable[QueryExecutionResult]]:
    """Create execute_query function with injected dependencies"""

    async def execute_query(
        sql: str,
        project_id: Optional[str] = None,
    ) -> QueryExecutionResult:
        """Execute BigQuery query safely"""
        result = await execute_query_impl(sql, project_id)

        if result.success:
            logger(f"Query execution successful - result rows: {result.total_rows}")
        else:
            logger(f"Query execution failed: {result.error_message}")

        return result

    return execute_query
