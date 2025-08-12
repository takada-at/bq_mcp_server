"""
Tests for QueryParser smart LIMIT handling (preserve smaller limits)
"""

from bq_mcp_server.core.query_parser import QueryParser


class TestQueryParserSmartLimit:
    """Test smart LIMIT handling in QueryParser"""

    def test_add_limit_to_query_without_limit(self):
        """Test adding LIMIT to query without existing LIMIT"""
        sql = "SELECT * FROM table"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 100"

    def test_preserve_smaller_limit(self):
        """Test preserving existing LIMIT when it's smaller than default"""
        sql = "SELECT * FROM table LIMIT 50"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 50"  # Should preserve 50

    def test_preserve_much_smaller_limit(self):
        """Test preserving very small LIMIT"""
        sql = "SELECT * FROM table LIMIT 5"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 5"  # Should preserve 5

    def test_replace_larger_limit(self):
        """Test replacing larger LIMIT with default"""
        sql = "SELECT * FROM table LIMIT 1000"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 100"  # Should replace with 100

    def test_preserve_equal_limit(self):
        """Test preserving LIMIT when it equals default"""
        sql = "SELECT * FROM table LIMIT 100"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 100"  # Should keep 100

    def test_preserve_smaller_limit_with_semicolon(self):
        """Test preserving smaller LIMIT in query with semicolon"""
        sql = "SELECT * FROM table LIMIT 10;"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 10;"  # Should preserve 10

    def test_replace_larger_limit_with_semicolon(self):
        """Test replacing larger LIMIT in query with semicolon"""
        sql = "SELECT * FROM table LIMIT 200;"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 100;"  # Should replace with 100

    def test_add_limit_with_semicolon(self):
        """Test adding LIMIT to query with semicolon"""
        sql = "SELECT * FROM table;"
        result = QueryParser.add_or_modify_limit(sql, 100)
        assert result == "SELECT * FROM table LIMIT 100;"
