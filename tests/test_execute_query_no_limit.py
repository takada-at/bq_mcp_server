"""Tests for execute_query_no_limit function in repositories/logic.py"""

from unittest.mock import MagicMock, patch

import pytest

from bq_mcp_server.core.entities import QueryExecutionResult


@pytest.mark.asyncio
async def test_execute_query_no_limit_impl_calls_query_executor():
    """Test that _execute_query_no_limit_impl calls QueryExecutor with skip_limit_modification=True"""
    from bq_mcp_server.repositories.logic import _execute_query_no_limit_impl

    with (
        patch("bq_mcp_server.repositories.config.get_settings") as mock_get_settings,
        patch(
            "bq_mcp_server.repositories.query_executor.QueryExecutor"
        ) as mock_query_executor_class,
    ):
        # Mock settings
        mock_settings = MagicMock()
        mock_get_settings.return_value = mock_settings

        # Mock QueryExecutor instance
        mock_query_executor = MagicMock()
        mock_query_executor_class.return_value = mock_query_executor

        # Mock successful execution result
        mock_result = QueryExecutionResult(
            success=True,
            rows=[{"id": 1, "name": "test"}],
            total_rows=1,
            total_bytes_processed=100,
            total_bytes_billed=200,
            execution_time_ms=500,
            job_id="test-job-id",
            error_message=None,
        )
        mock_query_executor.execute_query.return_value = mock_result

        sql = "SELECT * FROM table"
        project_id = "test-project"

        result = await _execute_query_no_limit_impl(sql, project_id)

        # Verify QueryExecutor was initialized with settings
        mock_query_executor_class.assert_called_once_with(mock_settings)

        # Verify execute_query was called with skip_limit_modification=True
        mock_query_executor.execute_query.assert_called_once_with(
            sql, project_id, skip_limit_modification=True
        )

        # Verify the result
        assert result.success is True
        assert result.rows == [{"id": 1, "name": "test"}]


@pytest.mark.asyncio
async def test_execute_query_no_limit_impl_with_no_project_id():
    """Test that _execute_query_no_limit_impl works with None project_id"""
    from bq_mcp_server.repositories.logic import _execute_query_no_limit_impl

    with (
        patch("bq_mcp_server.repositories.config.get_settings") as mock_get_settings,
        patch(
            "bq_mcp_server.repositories.query_executor.QueryExecutor"
        ) as mock_query_executor_class,
    ):
        # Mock settings
        mock_settings = MagicMock()
        mock_get_settings.return_value = mock_settings

        # Mock QueryExecutor instance
        mock_query_executor = MagicMock()
        mock_query_executor_class.return_value = mock_query_executor

        # Mock successful execution result
        mock_result = QueryExecutionResult(
            success=True,
            rows=[{"id": 1, "name": "test"}],
            total_rows=1,
            total_bytes_processed=100,
            total_bytes_billed=200,
            execution_time_ms=500,
            job_id="test-job-id",
            error_message=None,
        )
        mock_query_executor.execute_query.return_value = mock_result

        sql = "SELECT * FROM table"

        result = await _execute_query_no_limit_impl(sql, None)

        # Verify execute_query was called with skip_limit_modification=True and None project_id
        mock_query_executor.execute_query.assert_called_once_with(
            sql, None, skip_limit_modification=True
        )

        assert result.success is True


@pytest.mark.asyncio
async def test_execute_query_no_limit_impl_handles_failure():
    """Test that _execute_query_no_limit_impl handles query execution failure"""
    from bq_mcp_server.repositories.logic import _execute_query_no_limit_impl

    with (
        patch("bq_mcp_server.repositories.config.get_settings") as mock_get_settings,
        patch(
            "bq_mcp_server.repositories.query_executor.QueryExecutor"
        ) as mock_query_executor_class,
    ):
        # Mock settings
        mock_settings = MagicMock()
        mock_get_settings.return_value = mock_settings

        # Mock QueryExecutor instance
        mock_query_executor = MagicMock()
        mock_query_executor_class.return_value = mock_query_executor

        # Mock failed execution result
        mock_result = QueryExecutionResult(
            success=False,
            rows=None,
            total_rows=None,
            total_bytes_processed=None,
            total_bytes_billed=None,
            execution_time_ms=100,
            job_id=None,
            error_message="Query failed",
        )
        mock_query_executor.execute_query.return_value = mock_result

        sql = "SELECT * FROM nonexistent_table"

        result = await _execute_query_no_limit_impl(sql, None)

        # Verify failure result
        assert result.success is False
        assert result.error_message == "Query failed"
        assert result.rows is None
