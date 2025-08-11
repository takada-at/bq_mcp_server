"""Tests for QueryExecutor skip_limit_modification parameter"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from bq_mcp_server.core.entities import Settings
from bq_mcp_server.repositories.query_executor import QueryExecutor


@pytest.fixture
def mock_settings():
    """Mock settings for testing"""
    return Settings(
        project_ids=["test-project"],
        default_query_limit=10,
        max_scan_bytes=1073741824,  # 1GB
        query_timeout_seconds=300,
        query_execution_project_id="test-project",
    )


@pytest.fixture
def query_executor(mock_settings):
    """QueryExecutor instance for testing"""
    return QueryExecutor(mock_settings)


class TestSkipLimitModification:
    """Tests for skip_limit_modification parameter"""

    @pytest.mark.asyncio
    async def test_skip_limit_modification_false_adds_limit(self, query_executor):
        """Test that skip_limit_modification=False adds LIMIT clause (default behavior)"""
        with (
            patch.object(query_executor, "_get_client") as mock_get_client,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.is_safe_query",
                return_value=(True, None),
            ),
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.add_or_modify_limit",
                return_value="SELECT * FROM table LIMIT 10",
            ) as mock_add_limit,
        ):
            # Mock BigQuery client
            mock_client = MagicMock()
            mock_job = MagicMock()
            mock_job.total_bytes_processed = 100
            mock_job.total_bytes_billed = 200
            mock_job.job_id = "test-job-id"
            mock_result = MagicMock()
            mock_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
            mock_job.result.return_value = mock_result
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM table"
            result = await query_executor.execute_query(
                sql, skip_limit_modification=False
            )

            # Verify LIMIT was added
            mock_add_limit.assert_called_once_with(sql, 10)
            assert result.success is True
            assert len(result.rows) == 1

    @pytest.mark.asyncio
    async def test_skip_limit_modification_true_no_limit(self, query_executor):
        """Test that skip_limit_modification=True does NOT add LIMIT clause"""
        with (
            patch.object(query_executor, "_get_client") as mock_get_client,
            patch.object(query_executor, "check_scan_amount") as mock_check_scan,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.is_safe_query",
                return_value=(True, None),
            ) as mock_is_safe,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.add_or_modify_limit"
            ) as mock_add_limit,
        ):
            from bq_mcp_server.core.entities import QueryDryRunResult

            # Mock successful dry run
            mock_check_scan.return_value = QueryDryRunResult(
                total_bytes_processed=100,
                total_bytes_billed=200,
                is_safe=True,
                modified_sql="SELECT * FROM table",
                error_message=None,
            )

            # Mock BigQuery client
            mock_client = MagicMock()
            mock_job = MagicMock()
            mock_job.total_bytes_processed = 100
            mock_job.total_bytes_billed = 200
            mock_job.job_id = "test-job-id"
            mock_result = MagicMock()
            mock_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
            mock_job.result.return_value = mock_result
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM table"
            result = await query_executor.execute_query(
                sql, skip_limit_modification=True
            )

            # Verify safety check was called
            mock_is_safe.assert_called_once_with(sql)
            # Verify LIMIT was NOT added
            mock_add_limit.assert_not_called()
            # Verify dry run was called with original SQL
            mock_check_scan.assert_called_once_with(sql, None)
            # Verify original SQL was used for execution
            mock_client.query.assert_called_once()
            call_args = mock_client.query.call_args[0]
            assert call_args[0] == sql  # Original SQL without modification

            assert result.success is True

    @pytest.mark.asyncio
    async def test_skip_limit_modification_true_rejects_unsafe_query(
        self, query_executor
    ):
        """Test that skip_limit_modification=True still rejects unsafe queries"""
        with patch(
            "bq_mcp_server.core.query_parser.QueryParser.is_safe_query",
            return_value=(False, "Dangerous query detected"),
        ):
            sql = "DROP TABLE test"

            with pytest.raises(HTTPException) as exc_info:
                await query_executor.execute_query(sql, skip_limit_modification=True)

            assert exc_info.value.status_code == 400
            assert "Dangerous query detected" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_skip_limit_modification_true_with_force_execute(
        self, query_executor
    ):
        """Test skip_limit_modification=True with force_execute=True"""
        with (
            patch.object(query_executor, "_get_client") as mock_get_client,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.is_safe_query",
                return_value=(True, None),
            ) as mock_is_safe,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.add_or_modify_limit"
            ) as mock_add_limit,
        ):
            # Mock BigQuery client
            mock_client = MagicMock()
            mock_job = MagicMock()
            mock_job.total_bytes_processed = 100
            mock_job.total_bytes_billed = 200
            mock_job.job_id = "test-job-id"
            mock_result = MagicMock()
            mock_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
            mock_job.result.return_value = mock_result
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM table"
            result = await query_executor.execute_query(
                sql, force_execute=True, skip_limit_modification=True
            )

            # Verify safety check was called
            mock_is_safe.assert_called_once_with(sql)
            # Verify LIMIT was NOT added
            mock_add_limit.assert_not_called()
            # Verify original SQL was used
            mock_client.query.assert_called_once()
            call_args = mock_client.query.call_args[0]
            assert call_args[0] == sql

            assert result.success is True

    @pytest.mark.asyncio
    async def test_skip_limit_modification_false_with_force_execute(
        self, query_executor
    ):
        """Test skip_limit_modification=False with force_execute=True (should still add LIMIT)"""
        with (
            patch.object(query_executor, "_get_client") as mock_get_client,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.is_safe_query",
                return_value=(True, None),
            ),
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.add_or_modify_limit",
                return_value="SELECT * FROM table LIMIT 10",
            ) as mock_add_limit,
        ):
            # Mock BigQuery client
            mock_client = MagicMock()
            mock_job = MagicMock()
            mock_job.total_bytes_processed = 100
            mock_job.total_bytes_billed = 200
            mock_job.job_id = "test-job-id"
            mock_result = MagicMock()
            mock_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
            mock_job.result.return_value = mock_result
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM table"
            result = await query_executor.execute_query(
                sql, force_execute=True, skip_limit_modification=False
            )

            # Verify LIMIT was added even with force_execute=True
            mock_add_limit.assert_called_once_with(sql, 10)
            assert result.success is True

    @pytest.mark.asyncio
    async def test_default_behavior_unchanged(self, query_executor):
        """Test that default behavior (no skip_limit_modification parameter) still adds LIMIT"""
        with (
            patch.object(query_executor, "_get_client") as mock_get_client,
            patch.object(query_executor, "check_scan_amount") as mock_check_scan,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.is_safe_query",
                return_value=(True, None),
            ),
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.add_or_modify_limit",
                return_value="SELECT * FROM table LIMIT 10",
            ) as mock_add_limit,
        ):
            from bq_mcp_server.core.entities import QueryDryRunResult

            # Mock successful dry run
            mock_check_scan.return_value = QueryDryRunResult(
                total_bytes_processed=100,
                total_bytes_billed=200,
                is_safe=True,
                modified_sql="SELECT * FROM table LIMIT 10",
                error_message=None,
            )

            # Mock BigQuery client
            mock_client = MagicMock()
            mock_job = MagicMock()
            mock_job.total_bytes_processed = 100
            mock_job.total_bytes_billed = 200
            mock_job.job_id = "test-job-id"
            mock_result = MagicMock()
            mock_result.__iter__ = lambda self: iter([{"id": 1, "name": "test"}])
            mock_job.result.return_value = mock_result
            mock_client.query.return_value = mock_job
            mock_get_client.return_value = mock_client

            sql = "SELECT * FROM table"
            # Call without skip_limit_modification parameter
            result = await query_executor.execute_query(sql)

            # Verify LIMIT was added (default behavior)
            mock_add_limit.assert_called_once_with(sql, 10)
            assert result.success is True

    @pytest.mark.asyncio
    async def test_skip_limit_modification_true_with_dry_run_failure(
        self, query_executor
    ):
        """Test skip_limit_modification=True with dry run failure"""
        with (
            patch.object(query_executor, "_get_client"),
            patch.object(query_executor, "check_scan_amount") as mock_check_scan,
            patch(
                "bq_mcp_server.core.query_parser.QueryParser.is_safe_query",
                return_value=(True, None),
            ),
        ):
            from bq_mcp_server.core.entities import QueryDryRunResult

            # Mock dry run failure
            mock_check_scan.return_value = QueryDryRunResult(
                total_bytes_processed=0,
                total_bytes_billed=0,
                is_safe=False,
                modified_sql="SELECT * FROM table",
                error_message="Dry run failed",
            )

            sql = "SELECT * FROM table"
            result = await query_executor.execute_query(
                sql, skip_limit_modification=True
            )

            # Verify dry run was called with original SQL
            mock_check_scan.assert_called_once_with(sql, None)
            # Verify failure result
            assert result.success is False
            assert result.error_message == "Dry run failed"
