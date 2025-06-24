"""
Tests for BigQuery query executor functionality
"""

import pytest
from unittest.mock import Mock, patch
from bq_mcp.repositories.query_executor import QueryExecutor
from bq_mcp.core.entities import Settings, QueryDryRunResult, QueryExecutionResult
from fastapi import HTTPException


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
        max_scan_bytes=1024 * 1024,  # 1MB for testing
        default_query_limit=10,
        query_timeout_seconds=300,
    )


@pytest.fixture
def query_executor(mock_settings):
    """Create QueryExecutor instance for testing"""
    return QueryExecutor(mock_settings)


class TestQueryExecutor:
    """Test cases for QueryExecutor class"""

    @pytest.mark.asyncio
    async def test_check_scan_amount_safe(self, query_executor):
        """Test scan amount check with safe query"""
        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and job
            mock_client = Mock()
            mock_job = Mock()
            mock_job.total_bytes_processed = 1000  # Within limit
            mock_job.total_bytes_billed = 1000
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM dataset.table LIMIT 10"  # Already prepared
            result = await query_executor.check_scan_amount(sql)

            assert isinstance(result, QueryDryRunResult)
            assert result.total_bytes_processed == 1000
            assert result.total_bytes_billed == 1000
            assert result.is_safe is True

    @pytest.mark.asyncio
    async def test_check_scan_amount_unsafe(self, query_executor):
        """Test scan amount check with unsafe query (exceeds scan limit)"""
        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and job
            mock_client = Mock()
            mock_job = Mock()
            mock_job.total_bytes_processed = 2 * 1024 * 1024  # Exceeds 1MB limit
            mock_job.total_bytes_billed = 2 * 1024 * 1024
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM dataset.table LIMIT 10"  # Already prepared
            result = await query_executor.check_scan_amount(sql)

            assert isinstance(result, QueryDryRunResult)
            assert result.total_bytes_processed == 2 * 1024 * 1024
            assert result.is_safe is False

    def test_validate_and_prepare_query_dangerous_sql(self, query_executor):
        """Test query validation with dangerous SQL"""
        sql = "DELETE FROM dataset.table WHERE id = 1"

        with pytest.raises(HTTPException) as exc_info:
            query_executor._validate_and_prepare_query(sql)

        assert exc_info.value.status_code == 400
        assert "危険なSQL操作" in exc_info.value.detail

    def test_validate_and_prepare_query_adds_limit(self, query_executor):
        """Test that query preparation adds LIMIT clause"""
        sql = "SELECT * FROM dataset.table"
        result = query_executor._validate_and_prepare_query(sql)

        assert "LIMIT 10" in result

    @pytest.mark.asyncio
    async def test_execute_query_success(self, query_executor):
        """Test successful query execution"""
        with (
            patch.object(query_executor, "_get_client") as mock_get_client,
            patch.object(query_executor, "check_scan_amount") as mock_check_scan,
        ):
            # Mock scan amount check result (safe)
            mock_check_scan.return_value = QueryDryRunResult(
                total_bytes_processed=1000,
                total_bytes_billed=1000,
                is_safe=True,
                modified_sql="SELECT * FROM dataset.table LIMIT 10",
            )

            # Mock BigQuery client and results
            mock_client = Mock()
            mock_job = Mock()
            mock_job.result.return_value = [
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"},
            ]
            mock_job.total_bytes_processed = 1000
            mock_job.total_bytes_billed = 1000
            mock_job.job_id = "test-job-id"
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM dataset.table"
            result = await query_executor.execute_query(sql)

            assert isinstance(result, QueryExecutionResult)
            assert result.success is True
            assert len(result.rows) == 2
            assert result.total_rows == 2
            assert result.job_id == "test-job-id"

    @pytest.mark.asyncio
    async def test_execute_query_unsafe_blocked(self, query_executor):
        """Test query execution blocked due to unsafe scan amount"""
        with patch.object(query_executor, "check_scan_amount") as mock_check_scan:
            # Mock scan amount check result (unsafe)
            mock_check_scan.return_value = QueryDryRunResult(
                total_bytes_processed=2 * 1024 * 1024,  # Exceeds limit
                total_bytes_billed=2 * 1024 * 1024,
                is_safe=False,
                modified_sql="SELECT * FROM dataset.table LIMIT 10",
            )

            sql = "SELECT * FROM dataset.table"
            result = await query_executor.execute_query(sql)

            assert isinstance(result, QueryExecutionResult)
            assert result.success is False
            assert "スキャン量が制限を超えています" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_query_force_execution(self, query_executor):
        """Test forced query execution bypassing safety checks"""
        with patch.object(query_executor, "_get_client") as mock_get_client:
            # Mock BigQuery client and results
            mock_client = Mock()
            mock_job = Mock()
            mock_job.result.return_value = [{"id": 1, "name": "test"}]
            mock_job.total_bytes_processed = 2 * 1024 * 1024  # Would normally be unsafe
            mock_job.total_bytes_billed = 2 * 1024 * 1024
            mock_job.job_id = "test-job-id"
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM dataset.table"
            result = await query_executor.execute_query(sql, force_execute=True)

            assert isinstance(result, QueryExecutionResult)
            assert result.success is True
            assert len(result.rows) == 1

    @pytest.mark.asyncio
    async def test_execute_query_error(self, query_executor):
        """Test query execution with error"""
        with (
            patch.object(query_executor, "_get_client") as mock_get_client,
            patch.object(query_executor, "check_scan_amount") as mock_check_scan,
        ):
            # Mock scan amount check result (safe)
            mock_check_scan.return_value = QueryDryRunResult(
                total_bytes_processed=1000,
                total_bytes_billed=1000,
                is_safe=True,
                modified_sql="SELECT * FROM dataset.table LIMIT 10",
            )

            # Mock BigQuery client to raise exception
            mock_client = Mock()
            mock_client.query.side_effect = Exception("BigQuery error")
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM dataset.table"
            result = await query_executor.execute_query(sql)

            assert isinstance(result, QueryExecutionResult)
            assert result.success is False
            assert "BigQuery error" in result.error_message

    def test_format_bytes(self, query_executor):
        """Test byte formatting utility"""
        assert query_executor.format_bytes(500) == "500.0 B"
        assert query_executor.format_bytes(1024) == "1.0 KB"
        assert query_executor.format_bytes(1024 * 1024) == "1.0 MB"
        assert query_executor.format_bytes(1024 * 1024 * 1024) == "1.0 GB"
