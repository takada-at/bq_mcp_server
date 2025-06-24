"""
Tests for SQL query parser functionality
"""

from bq_mcp.core.query_parser import QueryParser


class TestQueryParser:
    """Test cases for QueryParser class"""

    def test_is_safe_query_safe_select(self):
        """Test that SELECT queries are considered safe"""
        sql = "SELECT * FROM dataset.table"
        is_safe, error_msg = QueryParser.is_safe_query(sql)
        assert is_safe is True
        assert error_msg is None

    def test_is_safe_query_unsafe_delete(self):
        """Test that DELETE queries are considered unsafe"""
        sql = "DELETE FROM dataset.table WHERE id = 1"
        is_safe, error_msg = QueryParser.is_safe_query(sql)
        assert is_safe is False
        assert "DELETE" in error_msg

    def test_is_safe_query_unsafe_drop(self):
        """Test that DROP queries are considered unsafe"""
        sql = "DROP TABLE dataset.table"
        is_safe, error_msg = QueryParser.is_safe_query(sql)
        assert is_safe is False
        assert "DROP" in error_msg

    def test_is_safe_query_case_insensitive(self):
        """Test that safety check is case insensitive"""
        sql = "drop table dataset.table"
        is_safe, error_msg = QueryParser.is_safe_query(sql)
        assert is_safe is False
        assert "DROP" in error_msg

    def test_has_limit_clause_with_limit(self):
        """Test detection of LIMIT clause"""
        sql = "SELECT * FROM dataset.table LIMIT 10"
        assert QueryParser.has_limit_clause(sql) is True

    def test_has_limit_clause_without_limit(self):
        """Test detection when no LIMIT clause"""
        sql = "SELECT * FROM dataset.table"
        assert QueryParser.has_limit_clause(sql) is False

    def test_has_limit_clause_case_insensitive(self):
        """Test LIMIT detection is case insensitive"""
        sql = "SELECT * FROM dataset.table limit 5"
        assert QueryParser.has_limit_clause(sql) is True

    def test_get_limit_value_existing(self):
        """Test getting LIMIT value when it exists"""
        sql = "SELECT * FROM dataset.table LIMIT 25"
        limit_value = QueryParser.get_limit_value(sql)
        assert limit_value == 25

    def test_get_limit_value_none(self):
        """Test getting LIMIT value when it doesn't exist"""
        sql = "SELECT * FROM dataset.table"
        limit_value = QueryParser.get_limit_value(sql)
        assert limit_value is None

    def test_add_limit_no_existing_limit(self):
        """Test adding LIMIT when none exists"""
        sql = "SELECT * FROM dataset.table"
        modified_sql = QueryParser.add_or_modify_limit(sql, 10)
        assert modified_sql == "SELECT * FROM dataset.table LIMIT 10"

    def test_add_limit_with_semicolon(self):
        """Test adding LIMIT when query ends with semicolon"""
        sql = "SELECT * FROM dataset.table;"
        modified_sql = QueryParser.add_or_modify_limit(sql, 10)
        assert modified_sql == "SELECT * FROM dataset.table LIMIT 10;"

    def test_modify_existing_limit(self):
        """Test modifying existing LIMIT clause"""
        sql = "SELECT * FROM dataset.table LIMIT 50"
        modified_sql = QueryParser.add_or_modify_limit(sql, 10)
        assert modified_sql == "SELECT * FROM dataset.table LIMIT 10"

    def test_modify_existing_limit_case_insensitive(self):
        """Test modifying existing LIMIT clause case insensitive"""
        sql = "SELECT * FROM dataset.table limit 50"
        modified_sql = QueryParser.add_or_modify_limit(sql, 10)
        assert modified_sql == "SELECT * FROM dataset.table LIMIT 10"

    def test_normalize_query_whitespace(self):
        """Test query normalization removes excess whitespace"""
        sql = "  SELECT   *   FROM   dataset.table  "
        normalized = QueryParser.normalize_query(sql)
        assert normalized == "SELECT * FROM dataset.table"

    def test_normalize_query_newlines(self):
        """Test query normalization handles newlines"""
        sql = "SELECT *\nFROM dataset.table\nWHERE id = 1"
        normalized = QueryParser.normalize_query(sql)
        assert normalized == "SELECT * FROM dataset.table WHERE id = 1"
