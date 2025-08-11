"""Pure business logic layer with dependency injection via higher-order functions"""

from typing import Awaitable, Callable, List, Optional, Tuple

from bq_mcp_server.core.entities import (
    CachedData,
    DatasetListResponse,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    QuerySaveResult,
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
ExportToCsvFunc = Callable[[List[dict], str, bool], Awaitable[int]]
ExportToJsonlFunc = Callable[[List[dict], str], Awaitable[int]]
ValidateOutputPathFunc = Callable[[str], str]
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


def create_save_query_result(
    execute_query: ExecuteQueryFunc,
    export_to_csv: ExportToCsvFunc,
    export_to_jsonl: ExportToJsonlFunc,
    validate_output_path: ValidateOutputPathFunc,
    logger: LoggerFunc,
) -> Callable[[str, str, str, Optional[str], bool], Awaitable[QuerySaveResult]]:
    """Create save_query_result function with injected dependencies"""

    async def save_query_result(
        sql: str,
        output_path: str,
        format: str,
        project_id: Optional[str] = None,
        include_header: bool = True,
    ) -> QuerySaveResult:
        """Execute query and save results to file"""
        import time

        start_time = time.time()

        try:
            # Validate output path first
            validated_path = validate_output_path(output_path)
            logger(f"Validated output path: {validated_path}")

            # Validate format
            if format not in ["csv", "jsonl"]:
                return QuerySaveResult(
                    success=False,
                    output_path=output_path,
                    format=format,
                    total_rows=0,
                    file_size_bytes=0,
                    execution_time_ms=int((time.time() - start_time) * 1000),
                    query_bytes_processed=None,
                    error_message=f"Unsupported format: {format}. Supported formats: csv, jsonl",
                )

            # Execute query
            logger(f"Executing query for save operation: {sql[:100]}...")
            query_result = await execute_query(sql, project_id)

            # Check if query execution failed
            if not query_result.success:
                return QuerySaveResult(
                    success=False,
                    output_path=validated_path,
                    format=format,
                    total_rows=0,
                    file_size_bytes=0,
                    execution_time_ms=int((time.time() - start_time) * 1000),
                    query_bytes_processed=query_result.total_bytes_processed,
                    error_message=query_result.error_message,
                )

            # Export to file
            try:
                if format == "csv":
                    file_size = await export_to_csv(
                        query_result.rows or [], validated_path, include_header
                    )
                else:  # jsonl
                    file_size = await export_to_jsonl(
                        query_result.rows or [], validated_path
                    )

                logger(
                    f"Successfully saved {query_result.total_rows} rows to {validated_path}"
                )

                return QuerySaveResult(
                    success=True,
                    output_path=validated_path,
                    format=format,
                    total_rows=query_result.total_rows or 0,
                    file_size_bytes=file_size,
                    execution_time_ms=int((time.time() - start_time) * 1000),
                    query_bytes_processed=query_result.total_bytes_processed,
                    error_message=None,
                )

            except Exception as e:
                logger(f"File export failed: {str(e)}")
                return QuerySaveResult(
                    success=False,
                    output_path=validated_path,
                    format=format,
                    total_rows=query_result.total_rows or 0,
                    file_size_bytes=0,
                    execution_time_ms=int((time.time() - start_time) * 1000),
                    query_bytes_processed=query_result.total_bytes_processed,
                    error_message=f"Failed to export to file: {str(e)}",
                )

        except Exception as e:
            logger(f"Save query result failed: {str(e)}")
            return QuerySaveResult(
                success=False,
                output_path=output_path,
                format=format,
                total_rows=0,
                file_size_bytes=0,
                execution_time_ms=int((time.time() - start_time) * 1000),
                query_bytes_processed=None,
                error_message=str(e),
            )

    return save_query_result
