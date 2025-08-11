"""Integration tests for save_query_result without LIMIT clause"""

import os
import tempfile
from unittest.mock import patch

import pytest

from bq_mcp_server.core.entities import QueryExecutionResult


@pytest.mark.asyncio
async def test_save_query_result_uses_execute_query_no_limit():
    """Test that save_query_result no longer adds LIMIT clause to queries"""

    # Mock at the implementation level
    with patch(
        "bq_mcp_server.repositories.logic._execute_query_no_limit_impl"
    ) as mock_execute:
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
        mock_execute.return_value = mock_result

        # Import after mocking
        from bq_mcp_server.repositories.logic import save_query_result

        # Test with temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as temp_file:
            temp_path = temp_file.name

        try:
            sql = "SELECT project, COUNT(*) as count FROM my_table GROUP BY project"
            result = await save_query_result(sql, temp_path, "csv")

            # Verify that _execute_query_no_limit_impl was called with the original SQL
            mock_execute.assert_called_once_with(sql, None)

            # Verify the result
            assert result.success is True
            assert result.total_rows == 3
            assert result.format == "csv"
            assert result.output_path == temp_path

            # Verify file was created and has content
            assert os.path.exists(temp_path)
            with open(temp_path, "r") as f:
                content = f.read()
                assert "project,count" in content  # Header
                assert "test-project-1,5" in content
                assert "test-project-2,10" in content
                assert "test-project-3,15" in content

        finally:
            # Cleanup
            if os.path.exists(temp_path):
                os.unlink(temp_path)


@pytest.mark.asyncio
async def test_save_query_result_no_limit_original_sql_preserved():
    """Test that the original SQL is passed through unchanged"""

    with patch(
        "bq_mcp_server.repositories.logic._execute_query_no_limit_impl"
    ) as mock_execute:
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
        mock_execute.return_value = mock_result

        from bq_mcp_server.repositories.logic import save_query_result

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as temp_file:
            temp_path = temp_file.name

        try:
            # Test with a query that already has a LIMIT
            sql = "SELECT * FROM large_table LIMIT 1000"
            await save_query_result(sql, temp_path, "jsonl")

            # Verify that the original SQL (with its existing LIMIT) was used
            mock_execute.assert_called_once_with(sql, None)

            # Test with a query without LIMIT
            mock_execute.reset_mock()
            sql_no_limit = "SELECT * FROM large_table WHERE status = 'active'"
            await save_query_result(sql_no_limit, temp_path, "jsonl")

            # Verify that the SQL was used unchanged (no LIMIT added)
            mock_execute.assert_called_once_with(sql_no_limit, None)

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


@pytest.mark.asyncio
async def test_save_query_result_with_project_id():
    """Test that project_id is passed through correctly"""

    with patch(
        "bq_mcp_server.repositories.logic._execute_query_no_limit_impl"
    ) as mock_execute:
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
        mock_execute.return_value = mock_result

        from bq_mcp_server.repositories.logic import save_query_result

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as temp_file:
            temp_path = temp_file.name

        try:
            sql = "SELECT * FROM dataset.table"
            project_id = "my-custom-project"
            result = await save_query_result(sql, temp_path, "csv", project_id)

            # Verify that both SQL and project_id were passed correctly
            mock_execute.assert_called_once_with(sql, project_id)

            assert result.success is True

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
