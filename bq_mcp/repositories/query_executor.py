"""
BigQuery query execution with cost control and safety measures
"""

import time
from typing import Optional

from fastapi import HTTPException
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig

from bq_mcp.core.entities import QueryDryRunResult, QueryExecutionResult, Settings
from bq_mcp.core.query_parser import QueryParser
from bq_mcp.repositories import log


class QueryExecutor:
    """Class for managing safe execution of BigQuery queries"""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.logger = log.get_logger()
        self.client = None

    def _get_client(self, project_id: Optional[str] = None) -> bigquery.Client:
        """Get BigQuery client"""
        if self.client is None:
            if self.settings.gcp_service_account_key_path:
                self.client = bigquery.Client.from_service_account_json(
                    self.settings.gcp_service_account_key_path,
                    project=project_id or self.settings.project_ids[0],
                )
            else:
                self.client = bigquery.Client(
                    project=project_id or self.settings.project_ids[0]
                )
        return self.client

    def _validate_and_prepare_query(self, sql: str) -> str:
        """
        Perform query safety checks and add/modify LIMIT clause

        Args:
            sql: SQL query to execute

        Returns:
            Modified SQL query
        """
        # Query safety check
        is_safe, error_msg = QueryParser.is_safe_query(sql)
        if not is_safe:
            raise HTTPException(status_code=400, detail=error_msg)

        # Add/modify LIMIT clause
        modified_sql = QueryParser.add_or_modify_limit(
            sql, self.settings.default_query_limit
        )
        return modified_sql

    async def check_scan_amount(
        self, sql: str, project_id: Optional[str] = None
    ) -> QueryDryRunResult:
        """
        Check query scan amount with dry run

        Args:
            sql: SQL query to execute (already prepared)
            project_id: Target project ID for execution

        Returns:
            Dry run result
        """
        self.logger.info(f"Starting scan amount check: {sql[:100]}...")

        try:
            client = self._get_client(project_id)

            # Configuration for dry run
            job_config = QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )

            # Execute dry run query
            query_job = client.query(sql, job_config=job_config)

            # Get results
            total_bytes_processed = query_job.total_bytes_processed or 0
            total_bytes_billed = query_job.total_bytes_billed or 0

            # Safety assessment
            is_safe_to_run = total_bytes_processed <= self.settings.max_scan_bytes

            self.logger.info(
                f"Scan amount check completed - Expected processed bytes: {total_bytes_processed:,}, "
                f"Expected billed bytes: {total_bytes_billed:,}, "
                f"Safe: {is_safe_to_run}"
            )

            return QueryDryRunResult(
                total_bytes_processed=total_bytes_processed,
                total_bytes_billed=total_bytes_billed,
                is_safe=is_safe_to_run,
                modified_sql=sql,
            )

        except Exception as e:
            self.logger.error(f"Scan amount check error: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Error occurred during scan amount check: {str(e)}",
            )

    async def execute_query(
        self, sql: str, project_id: Optional[str] = None, force_execute: bool = False
    ) -> QueryExecutionResult:
        """
        Execute query safely

        Args:
            sql: SQL query to execute
            project_id: Target project ID for execution
            force_execute: Whether to skip dry run check and force execution

        Returns:
            Query execution result
        """
        start_time = time.time()
        self.logger.info(f"Starting query execution: {sql[:100]}...")

        try:
            # Query preparation (safety check + LIMIT clause modification)
            modified_sql = self._validate_and_prepare_query(sql)

            # Dry run check (only when force_execute is False)
            if not force_execute:
                dry_run_result = await self.check_scan_amount(modified_sql, project_id)
                if not dry_run_result.is_safe:
                    return QueryExecutionResult(
                        success=False,
                        error_message=(
                            f"Query scan amount exceeds limit. "
                            f"Expected scan amount: {dry_run_result.total_bytes_processed:,} bytes, "
                            f"Limit: {self.settings.max_scan_bytes:,} bytes"
                        ),
                    )

            client = self._get_client(project_id)

            # Configuration for query execution
            job_config = QueryJobConfig(
                use_query_cache=True,
            )

            # Execute query
            query_job = client.query(modified_sql, job_config=job_config)
            results = query_job.result(
                timeout=self.settings.query_timeout_seconds
            )  # Wait for completion

            # Convert results to list
            rows = []
            for row in results:
                row_dict = {}
                for key, value in row.items():
                    # Handle BigQuery special types
                    if hasattr(value, "isoformat"):  # datetime objects
                        row_dict[key] = value.isoformat()
                    elif isinstance(value, (bytes, bytearray)):
                        row_dict[key] = value.decode("utf-8", errors="replace")
                    else:
                        row_dict[key] = value
                rows.append(row_dict)

            execution_time_ms = int((time.time() - start_time) * 1000)

            self.logger.info(
                f"Query execution completed - Rows: {len(rows)}, "
                f"Execution time: {execution_time_ms}ms, "
                f"Processed bytes: {query_job.total_bytes_processed or 0:,}"
            )

            return QueryExecutionResult(
                success=True,
                rows=rows,
                total_rows=len(rows),
                total_bytes_processed=query_job.total_bytes_processed,
                total_bytes_billed=query_job.total_bytes_billed,
                execution_time_ms=execution_time_ms,
                job_id=query_job.job_id,
            )

        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)
            error_msg = str(e)

            self.logger.error(f"Query execution error: {error_msg}")

            return QueryExecutionResult(
                success=False,
                execution_time_ms=execution_time_ms,
                error_message=error_msg,
            )

    def format_bytes(self, bytes_count: int) -> str:
        """Format byte count into human-readable format"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"
