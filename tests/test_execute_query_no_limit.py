"""Tests for execute_query_no_limit function in repositories/logic.py"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bq_mcp_server.core.entities import QueryExecutionResult


@pytest.mark.asyncio
async def test_execute_query_no_limit_impl_calls_query_executor():
    """Test that _execute_query_no_limit_impl calls QueryExecutor with skip_limit_modification=True"""

    mock_settings = MagicMock()
    mock_query_executor = MagicMock()

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

    # Mock the async execute_query method
    mock_query_executor.execute_query = AsyncMock(return_value=mock_result)

    with (
        patch("bq_mcp_server.repositories.logic.config") as mock_config,
        patch(
            "bq_mcp_server.repositories.logic.QueryExecutor",
            return_value=mock_query_executor,
        ) as mock_query_executor_class,
    ):
        mock_config.get_settings.return_value = mock_settings

        # Import after patching
        from bq_mcp_server.repositories.logic import _execute_query_no_limit_impl

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

    mock_settings = MagicMock()
    mock_query_executor = MagicMock()

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

    # Mock the async execute_query method
    mock_query_executor.execute_query = AsyncMock(return_value=mock_result)

    with (
        patch("bq_mcp_server.repositories.logic.config") as mock_config,
        patch(
            "bq_mcp_server.repositories.logic.QueryExecutor",
            return_value=mock_query_executor,
        ),
    ):
        mock_config.get_settings.return_value = mock_settings

        # Import after patching
        from bq_mcp_server.repositories.logic import _execute_query_no_limit_impl

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

    mock_settings = MagicMock()
    mock_query_executor = MagicMock()

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

    # Mock the async execute_query method
    mock_query_executor.execute_query = AsyncMock(return_value=mock_result)

    with (
        patch("bq_mcp_server.repositories.logic.config") as mock_config,
        patch(
            "bq_mcp_server.repositories.logic.QueryExecutor",
            return_value=mock_query_executor,
        ),
    ):
        mock_config.get_settings.return_value = mock_settings

        # Import after patching
        from bq_mcp_server.repositories.logic import _execute_query_no_limit_impl

        sql = "SELECT * FROM nonexistent_table"

        result = await _execute_query_no_limit_impl(sql, None)

        # Verify failure result
        assert result.success is False
        assert result.error_message == "Query failed"
        assert result.rows is None
