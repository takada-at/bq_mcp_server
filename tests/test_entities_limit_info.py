"""
Tests for QueryExecutionResult entity with LIMIT information
"""

from bq_mcp_server.core.entities import QueryExecutionResult


class TestQueryExecutionResultLimitInfo:
    """Test cases for QueryExecutionResult with LIMIT information"""

    def test_query_execution_result_with_limit_info(self):
        """Test QueryExecutionResult can store LIMIT information"""
        result = QueryExecutionResult(
            success=True,
            rows=[{"col1": "value1"}, {"col2": "value2"}],
            total_rows=2,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
            original_limit=None,
            applied_limit=10,
            limit_was_modified=True,
        )

        assert result.original_limit is None
        assert result.applied_limit == 10
        assert result.limit_was_modified is True

    def test_query_execution_result_without_modification(self):
        """Test QueryExecutionResult when LIMIT was not modified"""
        result = QueryExecutionResult(
            success=True,
            rows=[{"col1": "value1"}],
            total_rows=1,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
            original_limit=5,
            applied_limit=5,
            limit_was_modified=False,
        )

        assert result.original_limit == 5
        assert result.applied_limit == 5
        assert result.limit_was_modified is False

    def test_query_execution_result_backwards_compatibility(self):
        """Test QueryExecutionResult maintains backwards compatibility"""
        # Should work without new fields
        result = QueryExecutionResult(
            success=True,
            rows=[],
            total_rows=0,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
        )

        # New fields should have default values
        assert hasattr(result, "original_limit")
        assert hasattr(result, "applied_limit")
        assert hasattr(result, "limit_was_modified")
        assert result.original_limit is None
        assert result.applied_limit is None
        assert result.limit_was_modified is False
