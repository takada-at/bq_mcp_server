"""Tests for save_query_result functionality in logic_base"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from bq_mcp_server.core import logic_base
from bq_mcp_server.core.entities import (
    QueryExecutionResult,
    QuerySaveResult,
)


class TestSaveQueryResult:
    """Tests for save_query_result function"""

    @pytest.mark.asyncio
    async def test_save_query_result_csv_success(self):
        """Test successful query execution and CSV save"""
        # Mock query execution result
        mock_query_result = QueryExecutionResult(
            success=True,
            rows=[
                {"id": 1, "name": "Alice", "score": 95.5},
                {"id": 2, "name": "Bob", "score": 87.3},
            ],
            total_rows=2,
            total_bytes_processed=1024,
            total_bytes_billed=2048,
            execution_time_ms=123,
            job_id="job123",
            error_message=None,
        )

        # Mock dependencies
        mock_execute_query = AsyncMock(return_value=mock_query_result)
        mock_export_to_csv = AsyncMock(return_value=256)  # file size
        mock_logger = MagicMock()

        # Create the function with dependencies
        save_query_result = logic_base.create_save_query_result(
            execute_query=mock_execute_query,
            export_to_csv=mock_export_to_csv,
            export_to_jsonl=AsyncMock(),
            validate_output_path=lambda p: p,
            logger=mock_logger,
        )

        # Execute
        result = await save_query_result(
            sql="SELECT * FROM table",
            output_path="/tmp/output.csv",
            format="csv",
            project_id="test-project",
            include_header=True,
        )

        # Verify result
        assert isinstance(result, QuerySaveResult)
        assert result.success is True
        assert result.output_path == "/tmp/output.csv"
        assert result.format == "csv"
        assert result.total_rows == 2
        assert result.file_size_bytes == 256
        assert result.query_bytes_processed == 1024
        assert result.error_message is None

        # Verify mocks were called correctly
        mock_execute_query.assert_called_once_with(
            "SELECT * FROM table", "test-project"
        )
        mock_export_to_csv.assert_called_once_with(
            mock_query_result.rows, "/tmp/output.csv", True
        )

    @pytest.mark.asyncio
    async def test_save_query_result_jsonl_success(self):
        """Test successful query execution and JSONL save"""
        # Mock query execution result
        mock_query_result = QueryExecutionResult(
            success=True,
            rows=[
                {"id": 1, "data": {"key": "value1"}},
                {"id": 2, "data": {"key": "value2"}},
            ],
            total_rows=2,
            total_bytes_processed=2048,
            total_bytes_billed=4096,
            execution_time_ms=456,
            job_id="job456",
            error_message=None,
        )

        # Mock dependencies
        mock_execute_query = AsyncMock(return_value=mock_query_result)
        mock_export_to_jsonl = AsyncMock(return_value=512)  # file size
        mock_logger = MagicMock()

        # Create the function with dependencies
        save_query_result = logic_base.create_save_query_result(
            execute_query=mock_execute_query,
            export_to_csv=AsyncMock(),
            export_to_jsonl=mock_export_to_jsonl,
            validate_output_path=lambda p: p,
            logger=mock_logger,
        )

        # Execute
        result = await save_query_result(
            sql="SELECT * FROM table",
            output_path="/tmp/output.jsonl",
            format="jsonl",
            project_id=None,  # Use default project
            include_header=False,  # Should be ignored for JSONL
        )

        # Verify result
        assert result.success is True
        assert result.format == "jsonl"
        assert result.total_rows == 2
        assert result.file_size_bytes == 512
        assert result.query_bytes_processed == 2048

        # Verify mocks were called correctly
        mock_execute_query.assert_called_once_with("SELECT * FROM table", None)
        mock_export_to_jsonl.assert_called_once_with(
            mock_query_result.rows, "/tmp/output.jsonl"
        )

    @pytest.mark.asyncio
    async def test_save_query_result_query_failure(self):
        """Test handling of query execution failure"""
        # Mock failed query execution
        mock_query_result = QueryExecutionResult(
            success=False,
            rows=None,
            total_rows=None,
            total_bytes_processed=None,
            total_bytes_billed=None,
            execution_time_ms=100,
            job_id=None,
            error_message="Query syntax error",
        )

        # Mock dependencies
        mock_execute_query = AsyncMock(return_value=mock_query_result)
        mock_logger = MagicMock()

        # Create the function with dependencies
        save_query_result = logic_base.create_save_query_result(
            execute_query=mock_execute_query,
            export_to_csv=AsyncMock(),
            export_to_jsonl=AsyncMock(),
            validate_output_path=lambda p: p,
            logger=mock_logger,
        )

        # Execute
        result = await save_query_result(
            sql="INVALID SQL",
            output_path="/tmp/output.csv",
            format="csv",
        )

        # Verify result
        assert result.success is False
        assert result.error_message == "Query syntax error"
        assert result.total_rows == 0
        assert result.file_size_bytes == 0

    @pytest.mark.asyncio
    async def test_save_query_result_invalid_format(self):
        """Test handling of invalid output format"""
        # Mock dependencies
        mock_logger = MagicMock()

        # Create the function with dependencies
        save_query_result = logic_base.create_save_query_result(
            execute_query=AsyncMock(),
            export_to_csv=AsyncMock(),
            export_to_jsonl=AsyncMock(),
            validate_output_path=lambda p: p,
            logger=mock_logger,
        )

        # Execute
        result = await save_query_result(
            sql="SELECT * FROM table",
            output_path="/tmp/output.txt",
            format="txt",  # Invalid format
        )

        # Verify result
        assert result.success is False
        assert "Unsupported format" in result.error_message
        assert result.total_rows == 0

    @pytest.mark.asyncio
    async def test_save_query_result_file_write_error(self):
        """Test handling of file write errors"""
        # Mock successful query execution
        mock_query_result = QueryExecutionResult(
            success=True,
            rows=[{"id": 1}],
            total_rows=1,
            total_bytes_processed=100,
            total_bytes_billed=200,
            execution_time_ms=50,
            job_id="job789",
            error_message=None,
        )

        # Mock file export failure
        mock_execute_query = AsyncMock(return_value=mock_query_result)
        mock_export_to_csv = AsyncMock(side_effect=IOError("Permission denied"))
        mock_logger = MagicMock()

        # Create the function with dependencies
        save_query_result = logic_base.create_save_query_result(
            execute_query=mock_execute_query,
            export_to_csv=mock_export_to_csv,
            export_to_jsonl=AsyncMock(),
            validate_output_path=lambda p: p,
            logger=mock_logger,
        )

        # Execute
        result = await save_query_result(
            sql="SELECT * FROM table",
            output_path="/restricted/output.csv",
            format="csv",
        )

        # Verify result
        assert result.success is False
        assert "Permission denied" in result.error_message

    @pytest.mark.asyncio
    async def test_save_query_result_path_validation_error(self):
        """Test handling of path validation errors"""

        # Mock dependencies
        def validate_path_mock(path):
            if "../" in path:
                raise ValueError("Path traversal detected")
            return path

        mock_logger = MagicMock()

        # Create the function with dependencies
        save_query_result = logic_base.create_save_query_result(
            execute_query=AsyncMock(),
            export_to_csv=AsyncMock(),
            export_to_jsonl=AsyncMock(),
            validate_output_path=validate_path_mock,
            logger=mock_logger,
        )

        # Execute
        result = await save_query_result(
            sql="SELECT * FROM table",
            output_path="../../../etc/passwd",
            format="csv",
        )

        # Verify result
        assert result.success is False
        assert "Path traversal detected" in result.error_message

    @pytest.mark.asyncio
    async def test_save_query_result_empty_result_set(self):
        """Test handling of empty query results"""
        # Mock empty query result
        mock_query_result = QueryExecutionResult(
            success=True,
            rows=[],
            total_rows=0,
            total_bytes_processed=50,
            total_bytes_billed=100,
            execution_time_ms=25,
            job_id="job_empty",
            error_message=None,
        )

        # Mock dependencies
        mock_execute_query = AsyncMock(return_value=mock_query_result)
        mock_export_to_csv = AsyncMock(return_value=50)  # Small file with headers only
        mock_logger = MagicMock()

        # Create the function with dependencies
        save_query_result = logic_base.create_save_query_result(
            execute_query=mock_execute_query,
            export_to_csv=mock_export_to_csv,
            export_to_jsonl=AsyncMock(),
            validate_output_path=lambda p: p,
            logger=mock_logger,
        )

        # Execute
        result = await save_query_result(
            sql="SELECT * FROM table WHERE 1=0",
            output_path="/tmp/empty.csv",
            format="csv",
            include_header=True,
        )

        # Verify result
        assert result.success is True
        assert result.total_rows == 0
        assert result.file_size_bytes == 50  # Headers only
