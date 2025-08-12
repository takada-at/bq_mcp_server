"""
Tests for QueryExecutor with LIMIT information tracking
"""

from unittest.mock import Mock, patch

import pytest

from bq_mcp_server.core.entities import (
    Settings,
)
from bq_mcp_server.repositories.query_executor import QueryExecutor


@pytest.fixture
def mock_settings():
    """Create mock settings for testing"""
    return Settings(
        gcp_service_account_key_path=None,
        project_ids=["test-project"],
        cache_ttl_seconds=3600,
        cache_file_base_dir="/tmp/test_cache",
        api_host="127.0.0.1",
        api_port=8000,
        max_scan_bytes=1024 * 1024 * 1024,  # 1GB
        default_query_limit=100,  # Default LIMIT is 100
        query_timeout_seconds=300,
    )


@pytest.fixture
def query_executor(mock_settings):
    """Create QueryExecutor instance for testing"""
    return QueryExecutor(mock_settings)


class TestQueryExecutorLimitInfo:
    """Test cases for QueryExecutor LIMIT information tracking"""

    @pytest.mark.asyncio
    async def test_execute_query_adds_limit_and_tracks_info(self, query_executor):
        """Test that execute_query adds LIMIT clause and tracks the modification"""
        sql = "SELECT * FROM test_table"

        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and job
            mock_client = Mock()
            mock_job = Mock()
            mock_job.total_bytes_processed = 1000
            mock_job.total_bytes_billed = 1000
            mock_job.job_id = "test-job-id"

            # Mock result with exactly 100 rows (matching the LIMIT)
            mock_results = [{"id": i} for i in range(100)]
            mock_job.result.return_value = mock_results

            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            result = await query_executor.execute_query(sql)

            # Verify LIMIT information is tracked
            assert result.success is True
            assert result.original_limit is None  # Original query had no LIMIT
            assert result.applied_limit == 100  # Default LIMIT was applied
            assert result.limit_was_modified is True  # LIMIT was added
            assert result.total_rows == 100

            # Verify the query was modified with LIMIT
            called_sql = mock_client.query.call_args[0][0]
            assert "LIMIT 100" in called_sql

    @pytest.mark.asyncio
    async def test_execute_query_modifies_existing_limit(self, query_executor):
        """Test that execute_query modifies existing LIMIT and tracks the change"""
        sql = "SELECT * FROM test_table LIMIT 1000"

        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and job
            mock_client = Mock()
            mock_job = Mock()
            mock_job.total_bytes_processed = 1000
            mock_job.total_bytes_billed = 1000
            mock_job.job_id = "test-job-id"

            # Mock result with exactly 100 rows (matching the modified LIMIT)
            mock_results = [{"id": i} for i in range(100)]
            mock_job.result.return_value = mock_results

            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            result = await query_executor.execute_query(sql)

            # Verify LIMIT information is tracked
            assert result.success is True
            assert result.original_limit == 1000  # Original query had LIMIT 1000
            assert result.applied_limit == 100  # Modified to default LIMIT
            assert result.limit_was_modified is True  # LIMIT was modified
            assert result.total_rows == 100

            # Verify the query was modified with new LIMIT
            called_sql = mock_client.query.call_args[0][0]
            assert "LIMIT 100" in called_sql
            assert "LIMIT 1000" not in called_sql

    @pytest.mark.asyncio
    async def test_execute_query_keeps_smaller_limit(self, query_executor):
        """Test that execute_query keeps existing LIMIT if it's smaller than default"""
        sql = "SELECT * FROM test_table LIMIT 10"

        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and job
            mock_client = Mock()
            mock_job = Mock()
            mock_job.total_bytes_processed = 1000
            mock_job.total_bytes_billed = 1000
            mock_job.job_id = "test-job-id"

            # Mock result with 10 rows
            mock_results = [{"id": i} for i in range(10)]
            mock_job.result.return_value = mock_results

            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            result = await query_executor.execute_query(sql)

            # Verify LIMIT information is tracked (NEW BEHAVIOR: keep smaller limits)
            assert result.success is True
            assert result.original_limit == 10  # Original query had LIMIT 10
            assert result.applied_limit == 10  # Keep the smaller original limit
            assert result.limit_was_modified is False  # LIMIT was NOT modified
            assert result.total_rows == 10

    @pytest.mark.asyncio
    async def test_execute_query_with_skip_limit_modification(self, query_executor):
        """Test execute_query with skip_limit_modification flag"""
        sql = "SELECT * FROM test_table"

        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and job
            mock_client = Mock()
            mock_job = Mock()
            mock_job.total_bytes_processed = 1000
            mock_job.total_bytes_billed = 1000
            mock_job.job_id = "test-job-id"

            # Mock result with many rows
            mock_results = [{"id": i} for i in range(500)]
            mock_job.result.return_value = mock_results

            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            result = await query_executor.execute_query(
                sql, skip_limit_modification=True
            )

            # Verify LIMIT information when skipping modification
            assert result.success is True
            assert result.original_limit is None  # Original query had no LIMIT
            assert result.applied_limit is None  # No LIMIT was applied
            assert result.limit_was_modified is False  # LIMIT was not modified
            assert result.total_rows == 500

            # Verify the query was not modified
            called_sql = mock_client.query.call_args[0][0]
            assert "LIMIT" not in called_sql

    @pytest.mark.asyncio
    async def test_execute_query_with_fewer_results_than_limit(self, query_executor):
        """Test execute_query when results are fewer than the LIMIT"""
        sql = "SELECT * FROM test_table"

        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and job
            mock_client = Mock()
            mock_job = Mock()
            mock_job.total_bytes_processed = 1000
            mock_job.total_bytes_billed = 1000
            mock_job.job_id = "test-job-id"

            # Mock result with only 50 rows (less than LIMIT 100)
            mock_results = [{"id": i} for i in range(50)]
            mock_job.result.return_value = mock_results

            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            result = await query_executor.execute_query(sql)

            # Verify LIMIT information is tracked
            assert result.success is True
            assert result.original_limit is None  # Original query had no LIMIT
            assert result.applied_limit == 100  # Default LIMIT was applied
            assert result.limit_was_modified is True  # LIMIT was added
            assert result.total_rows == 50  # But only 50 rows were returned
