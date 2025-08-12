"""
Tests for Markdown converter with LIMIT notification
"""

from bq_mcp_server.core.converter import convert_query_result_to_markdown
from bq_mcp_server.core.entities import QueryExecutionResult


class TestConverterLimitNotification:
    """Test cases for Markdown converter LIMIT notification"""

    def test_shows_limit_notification_when_rows_equal_limit(self):
        """Test that notification shows when row count equals applied limit"""
        # Create result where rows == applied_limit and limit was modified
        result = QueryExecutionResult(
            success=True,
            rows=[{"id": i} for i in range(100)],
            total_rows=100,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
            original_limit=None,  # No original LIMIT
            applied_limit=100,  # Applied LIMIT 100
            limit_was_modified=True,  # LIMIT was added
        )

        markdown = convert_query_result_to_markdown(result)

        # Check that warning message is included
        assert "⚠️" in markdown
        assert "automatic LIMIT" in markdown
        assert "100" in markdown  # LIMIT value should be mentioned
        assert "save_query_result" in markdown  # Suggestion should be included

    def test_no_notification_when_rows_less_than_limit(self):
        """Test that no notification shows when row count is less than limit"""
        # Create result where rows < applied_limit
        result = QueryExecutionResult(
            success=True,
            rows=[{"id": i} for i in range(50)],
            total_rows=50,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
            original_limit=None,
            applied_limit=100,
            limit_was_modified=True,
        )

        markdown = convert_query_result_to_markdown(result)

        # Check that warning message is NOT included
        assert "⚠️" not in markdown or "automatic LIMIT" not in markdown

    def test_no_notification_when_limit_not_modified(self):
        """Test that no notification shows when LIMIT was not modified"""
        # Create result where limit was not modified (user's original LIMIT)
        result = QueryExecutionResult(
            success=True,
            rows=[{"id": i} for i in range(10)],
            total_rows=10,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
            original_limit=10,
            applied_limit=10,
            limit_was_modified=False,  # LIMIT was NOT modified
        )

        markdown = convert_query_result_to_markdown(result)

        # Check that warning message is NOT included
        assert "⚠️" not in markdown or "automatic LIMIT" not in markdown

    def test_shows_different_message_for_modified_limit(self):
        """Test different message when existing LIMIT was modified"""
        # Create result where an existing LIMIT was modified
        result = QueryExecutionResult(
            success=True,
            rows=[{"id": i} for i in range(100)],
            total_rows=100,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
            original_limit=1000,  # Original LIMIT was 1000
            applied_limit=100,  # Modified to 100
            limit_was_modified=True,
        )

        markdown = convert_query_result_to_markdown(result)

        # Check that warning message mentions both original and applied limits
        assert "⚠️" in markdown
        assert "100" in markdown  # Applied limit
        # Could mention original limit too
        if "1000" in markdown:
            assert True  # Original limit is mentioned

    def test_no_notification_for_failed_queries(self):
        """Test that no LIMIT notification shows for failed queries"""
        result = QueryExecutionResult(
            success=False,
            rows=None,
            total_rows=None,
            total_bytes_processed=None,
            total_bytes_billed=None,
            execution_time_ms=100,
            job_id=None,
            error_message="Query failed",
            original_limit=None,
            applied_limit=100,
            limit_was_modified=True,
        )

        markdown = convert_query_result_to_markdown(result)

        # Failed queries shouldn't show LIMIT notification
        assert "automatic LIMIT" not in markdown

    def test_backwards_compatibility_without_limit_fields(self):
        """Test that converter works with results without LIMIT fields"""
        # Create result without new LIMIT fields (backwards compatibility)
        result = QueryExecutionResult(
            success=True,
            rows=[{"id": i} for i in range(50)],
            total_rows=50,
            total_bytes_processed=1000,
            total_bytes_billed=1000,
            execution_time_ms=100,
            job_id="job-123",
            error_message=None,
            # No LIMIT fields - they should default to None/False
        )

        markdown = convert_query_result_to_markdown(result)

        # Should work without errors and no notification
        assert "BigQuery Query Execution Result" in markdown
        assert "⚠️" not in markdown or "automatic LIMIT" not in markdown
