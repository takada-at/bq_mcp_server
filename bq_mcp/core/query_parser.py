"""
SQL query parser for LIMIT clause detection and modification
"""

import re
from typing import Tuple, Optional


class QueryParser:
    """SQLクエリの解析とLIMIT句の操作を行うクラス"""

    # 危険なクエリパターン
    DANGEROUS_PATTERNS = [
        r"\bDELETE\b",
        r"\bDROP\b",
        r"\bTRUNCATE\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bALTER\b",
        r"\bCREATE\b",
    ]

    # LIMIT句のパターン（大文字小文字を無視）
    LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)

    @classmethod
    def is_safe_query(cls, sql: str) -> Tuple[bool, Optional[str]]:
        """
        クエリが安全かどうかをチェックする

        Args:
            sql: チェックするSQLクエリ

        Returns:
            (is_safe, error_message): 安全かどうかとエラーメッセージ
        """
        sql_upper = sql.upper()

        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, sql_upper):
                clean_pattern = pattern.replace("\\b", "").replace("\\", "")
                return False, f"危険なSQL操作が検出されました: {clean_pattern}"

        return True, None

    @classmethod
    def has_limit_clause(cls, sql: str) -> bool:
        """
        SQLクエリにLIMIT句があるかどうかをチェックする

        Args:
            sql: チェックするSQLクエリ

        Returns:
            LIMIT句があるかどうか
        """
        return bool(cls.LIMIT_PATTERN.search(sql))

    @classmethod
    def get_limit_value(cls, sql: str) -> Optional[int]:
        """
        SQLクエリからLIMIT値を取得する

        Args:
            sql: チェックするSQLクエリ

        Returns:
            LIMIT値（見つからない場合はNone）
        """
        match = cls.LIMIT_PATTERN.search(sql)
        if match:
            return int(match.group(1))
        return None

    @classmethod
    def add_or_modify_limit(cls, sql: str, limit_value: int) -> str:
        """
        SQLクエリにLIMIT句を追加または修正する

        Args:
            sql: 修正するSQLクエリ
            limit_value: 設定するLIMIT値

        Returns:
            LIMIT句が追加または修正されたSQLクエリ
        """
        # 既存のLIMIT句があるかチェック
        if cls.has_limit_clause(sql):
            # 既存のLIMIT句を新しい値に置き換え
            return cls.LIMIT_PATTERN.sub(f"LIMIT {limit_value}", sql)
        else:
            # LIMIT句がない場合は末尾に追加
            # セミコロンがある場合はその前に、ない場合は末尾に追加
            sql = sql.strip()
            if sql.endswith(";"):
                return sql[:-1] + f" LIMIT {limit_value};"
            else:
                return sql + f" LIMIT {limit_value}"

    @classmethod
    def normalize_query(cls, sql: str) -> str:
        """
        SQLクエリを正規化する（前後の空白を削除、改行を整理）

        Args:
            sql: 正規化するSQLクエリ

        Returns:
            正規化されたSQLクエリ
        """
        # 前後の空白を削除
        sql = sql.strip()

        # 複数の連続する空白を1つにまとめる
        sql = re.sub(r"\s+", " ", sql)

        return sql
