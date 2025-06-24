"""
SQL query parser for LIMIT clause detection and modification
"""

import re
from typing import Tuple, Optional


class QueryParser:
    """Class for SQL query parsing and LIMIT clause operations"""

    # Dangerous query patterns
    DANGEROUS_PATTERNS = [
        r"\bDELETE\b",
        r"\bDROP\b",
        r"\bTRUNCATE\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bALTER\b",
        r"\bCREATE\b",
    ]

    # LIMIT clause pattern (case-insensitive)
    LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)

    @classmethod
    def is_safe_query(cls, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Check if the query is safe

        Args:
            sql: SQL query to check

        Returns:
            (is_safe, error_message): Whether it's safe and error message
        """
        sql_upper = sql.upper()

        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, sql_upper):
                clean_pattern = pattern.replace("\\b", "").replace("\\", "")
                return False, f"Dangerous SQL operation detected: {clean_pattern}"

        return True, None

    @classmethod
    def has_limit_clause(cls, sql: str) -> bool:
        """
        Check if the SQL query has a LIMIT clause

        Args:
            sql: SQL query to check

        Returns:
            Whether there is a LIMIT clause
        """
        return bool(cls.LIMIT_PATTERN.search(sql))

    @classmethod
    def get_limit_value(cls, sql: str) -> Optional[int]:
        """
        Get the LIMIT value from the SQL query

        Args:
            sql: SQL query to check

        Returns:
            LIMIT value (None if not found)
        """
        match = cls.LIMIT_PATTERN.search(sql)
        if match:
            return int(match.group(1))
        return None

    @classmethod
    def add_or_modify_limit(cls, sql: str, limit_value: int) -> str:
        """
        Add or modify LIMIT clause in SQL query

        Args:
            sql: SQL query to modify
            limit_value: LIMIT value to set

        Returns:
            SQL query with LIMIT clause added or modified
        """
        # Check if there is an existing LIMIT clause
        if cls.has_limit_clause(sql):
            # Replace existing LIMIT clause with new value
            return cls.LIMIT_PATTERN.sub(f"LIMIT {limit_value}", sql)
        else:
            # Add LIMIT clause at the end if there is no LIMIT clause
            # Add before semicolon if present, otherwise at the end
            sql = sql.strip()
            if sql.endswith(";"):
                return sql[:-1] + f" LIMIT {limit_value};"
            else:
                return sql + f" LIMIT {limit_value}"

    @classmethod
    def normalize_query(cls, sql: str) -> str:
        """
        Normalize SQL query (remove leading/trailing whitespace, organize line breaks)

        Args:
            sql: SQL query to normalize

        Returns:
            Normalized SQL query
        """
        # Remove leading and trailing whitespace
        sql = sql.strip()

        # Combine multiple consecutive spaces into one
        sql = re.sub(r"\s+", " ", sql)

        return sql
