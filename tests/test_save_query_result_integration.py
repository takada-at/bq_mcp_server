"""Integration tests for save_query_result without LIMIT clause"""

import importlib
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bq_mcp_server.core.entities import QueryExecutionResult


def reload_logic_module():
    """Reload the logic module to ensure fresh imports with mocks"""
    if "bq_mcp_server.repositories.logic" in sys.modules:
        importlib.reload(sys.modules["bq_mcp_server.repositories.logic"])


@pytest.mark.asyncio
async def test_save_query_result_uses_execute_query_no_limit():
    """Test that save_query_result uses execute_query_no_limit internally"""

    # Mock successful query execution result
    mock_result = QueryExecutionResult(
        success=True,
        rows=[
            {"project": "test-project-1", "count": 5},
            {"project": "test-project-2", "count": 10},
            {"project": "test-project-3", "count": 15},
        ],
        total_rows=3,
        total_bytes_processed=1000,
        total_bytes_billed=2000,
        execution_time_ms=500,
        job_id="test-job-id",
        error_message=None,
    )

    temp_path = "/tmp/test.csv"

    # Mock at the QueryExecutor level - this is the most reliable approach
    with (
        patch(
            "bq_mcp_server.repositories.query_executor.QueryExecutor"
        ) as MockQueryExecutor,
        patch(
            "bq_mcp_server.core.file_exporter.validate_output_path",
            return_value=temp_path,
        ) as mock_validate,
        patch(
            "bq_mcp_server.core.file_exporter.export_to_csv",
            new_callable=AsyncMock,
            return_value=100,
        ) as mock_export,
    ):
        # Set up QueryExecutor mock
        mock_query_executor = MagicMock()
        mock_query_executor.execute_query = AsyncMock(return_value=mock_result)
        MockQueryExecutor.return_value = mock_query_executor

        # Reload and import after mocking
        reload_logic_module()
        from bq_mcp_server.repositories.logic import save_query_result

        sql = "SELECT project, COUNT(*) as count FROM my_table GROUP BY project"
        result = await save_query_result(sql, temp_path, "csv")

        # Verify QueryExecutor.execute_query was called with skip_limit_modification=True
        mock_query_executor.execute_query.assert_called_with(
            sql, None, skip_limit_modification=True
        )

        # Verify path validation was called
        mock_validate.assert_called_once_with(temp_path)

        # Verify export was called
        mock_export.assert_called_once()

        # Verify the result
        assert result.success is True
        assert result.total_rows == 3
        assert result.format == "csv"
        assert result.output_path == temp_path


@pytest.mark.asyncio
async def test_save_query_result_preserves_original_sql():
    """Test that save_query_result preserves the original SQL without modifications"""

    # Mock successful query execution result
    mock_result = QueryExecutionResult(
        success=True,
        rows=[{"id": 1, "name": "test"}],
        total_rows=1,
        total_bytes_processed=100,
        total_bytes_billed=200,
        execution_time_ms=300,
        job_id="test-job-id",
        error_message=None,
    )

    temp_path = "/tmp/test.jsonl"

    with (
        patch(
            "bq_mcp_server.repositories.query_executor.QueryExecutor"
        ) as MockQueryExecutor,
        patch(
            "bq_mcp_server.core.file_exporter.validate_output_path",
            return_value=temp_path,
        ),
        patch(
            "bq_mcp_server.core.file_exporter.export_to_jsonl",
            new_callable=AsyncMock,
            return_value=50,
        ),
    ):
        # Set up QueryExecutor mock
        mock_query_executor = MagicMock()
        mock_query_executor.execute_query = AsyncMock(return_value=mock_result)
        MockQueryExecutor.return_value = mock_query_executor

        # Reload and import after mocking
        reload_logic_module()
        from bq_mcp_server.repositories.logic import save_query_result

        # Test with a query that already has a LIMIT
        sql_with_limit = "SELECT * FROM large_table LIMIT 1000"
        await save_query_result(sql_with_limit, temp_path, "jsonl")

        # Verify that the original SQL (with its existing LIMIT) was used with skip_limit_modification=True
        mock_query_executor.execute_query.assert_called_with(
            sql_with_limit, None, skip_limit_modification=True
        )

        # Test with a query without LIMIT
        mock_query_executor.execute_query.reset_mock()
        sql_no_limit = "SELECT * FROM large_table WHERE status = 'active'"
        await save_query_result(sql_no_limit, temp_path, "jsonl")

        # Verify that the SQL was used unchanged (no LIMIT added) with skip_limit_modification=True
        mock_query_executor.execute_query.assert_called_with(
            sql_no_limit, None, skip_limit_modification=True
        )


@pytest.mark.asyncio
async def test_save_query_result_with_project_id():
    """Test that project_id is passed through correctly"""

    mock_result = QueryExecutionResult(
        success=True,
        rows=[{"id": 1}],
        total_rows=1,
        total_bytes_processed=100,
        total_bytes_billed=200,
        execution_time_ms=300,
        job_id="test-job-id",
        error_message=None,
    )

    temp_path = "/tmp/test.csv"

    with (
        patch(
            "bq_mcp_server.repositories.query_executor.QueryExecutor"
        ) as MockQueryExecutor,
        patch(
            "bq_mcp_server.core.file_exporter.validate_output_path",
            return_value=temp_path,
        ),
        patch(
            "bq_mcp_server.core.file_exporter.export_to_csv",
            new_callable=AsyncMock,
            return_value=50,
        ),
    ):
        # Set up QueryExecutor mock
        mock_query_executor = MagicMock()
        mock_query_executor.execute_query = AsyncMock(return_value=mock_result)
        MockQueryExecutor.return_value = mock_query_executor

        # Reload and import after mocking
        reload_logic_module()
        from bq_mcp_server.repositories.logic import save_query_result

        sql = "SELECT * FROM dataset.table"
        project_id = "my-custom-project"
        result = await save_query_result(sql, temp_path, "csv", project_id)

        # Verify that both SQL and project_id were passed correctly with skip_limit_modification=True
        mock_query_executor.execute_query.assert_called_once_with(
            sql, project_id, skip_limit_modification=True
        )

        assert result.success is True


@pytest.mark.asyncio
async def test_save_query_result_query_failure():
    """Test that save_query_result handles query execution failures properly"""

    # Mock failed query execution result
    mock_result = QueryExecutionResult(
        success=False,
        rows=None,
        total_rows=None,
        total_bytes_processed=None,
        total_bytes_billed=None,
        execution_time_ms=100,
        job_id=None,
        error_message="Query execution failed",
    )

    temp_path = "/tmp/test.csv"

    with (
        patch(
            "bq_mcp_server.repositories.query_executor.QueryExecutor"
        ) as MockQueryExecutor,
        patch(
            "bq_mcp_server.core.file_exporter.validate_output_path",
            return_value=temp_path,
        ),
    ):
        # Set up QueryExecutor mock for failure
        mock_query_executor = MagicMock()
        mock_query_executor.execute_query = AsyncMock(return_value=mock_result)
        MockQueryExecutor.return_value = mock_query_executor

        # Reload and import after mocking
        reload_logic_module()
        from bq_mcp_server.repositories.logic import save_query_result

        sql = "SELECT * FROM nonexistent_table"
        result = await save_query_result(sql, temp_path, "csv")

        # Verify query was attempted with skip_limit_modification=True
        mock_query_executor.execute_query.assert_called_once_with(
            sql, None, skip_limit_modification=True
        )

        # Verify failure is properly handled
        assert result.success is False
        assert result.error_message == "Query execution failed"
        assert result.total_rows == 0
