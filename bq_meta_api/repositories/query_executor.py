"""
BigQuery query execution with cost control and safety measures
"""

import time
from typing import Optional
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from fastapi import HTTPException

from bq_meta_api.core.entities import QueryDryRunResult, QueryExecutionResult, Settings
from bq_meta_api.core.query_parser import QueryParser
from bq_meta_api.repositories import log


class QueryExecutor:
    """BigQueryクエリの安全な実行を管理するクラス"""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.logger = log.get_logger()
        self.client = None

    def _get_client(self, project_id: Optional[str] = None) -> bigquery.Client:
        """BigQueryクライアントを取得する"""
        if self.client is None:
            if self.settings.gcp_service_account_key_path:
                self.client = bigquery.Client.from_service_account_json(
                    self.settings.gcp_service_account_key_path,
                    project=project_id or self.settings.project_ids[0],
                )
            else:
                self.client = bigquery.Client(
                    project=project_id or self.settings.project_ids[0]
                )
        return self.client

    def _validate_and_prepare_query(self, sql: str) -> str:
        """
        クエリの安全性チェックとLIMIT句の追加・修正を行う

        Args:
            sql: 実行するSQLクエリ

        Returns:
            修正されたSQLクエリ
        """
        # クエリの安全性チェック
        is_safe, error_msg = QueryParser.is_safe_query(sql)
        if not is_safe:
            raise HTTPException(status_code=400, detail=error_msg)

        # LIMIT句の追加・修正
        modified_sql = QueryParser.add_or_modify_limit(
            sql, self.settings.default_query_limit
        )
        return modified_sql

    async def check_scan_amount(
        self, sql: str, project_id: Optional[str] = None
    ) -> QueryDryRunResult:
        """
        クエリのスキャン量をドライランでチェックする

        Args:
            sql: 実行するSQLクエリ（既に準備済み）
            project_id: 実行対象のプロジェクトID

        Returns:
            ドライラン結果
        """
        self.logger.info(f"スキャン量チェック開始: {sql[:100]}...")

        try:
            client = self._get_client(project_id)

            # ドライラン用の設定
            job_config = QueryJobConfig(
                dry_run=True,
                use_query_cache=False,
            )

            # ドライランクエリを実行
            query_job = client.query(sql, job_config=job_config)

            # 結果を取得
            total_bytes_processed = query_job.total_bytes_processed or 0
            total_bytes_billed = query_job.total_bytes_billed or 0

            # 安全性の判定
            is_safe_to_run = total_bytes_processed <= self.settings.max_scan_bytes

            self.logger.info(
                f"スキャン量チェック完了 - 処理予定バイト数: {total_bytes_processed:,}, "
                f"課金予定バイト数: {total_bytes_billed:,}, "
                f"安全: {is_safe_to_run}"
            )

            return QueryDryRunResult(
                total_bytes_processed=total_bytes_processed,
                total_bytes_billed=total_bytes_billed,
                is_safe=is_safe_to_run,
                modified_sql=sql,
            )

        except Exception as e:
            self.logger.error(f"スキャン量チェックエラー: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"スキャン量チェック中にエラーが発生しました: {str(e)}",
            )

    async def execute_query(
        self, sql: str, project_id: Optional[str] = None, force_execute: bool = False
    ) -> QueryExecutionResult:
        """
        クエリを安全に実行する

        Args:
            sql: 実行するSQLクエリ
            project_id: 実行対象のプロジェクトID
            force_execute: ドライランチェックをスキップして強制実行するか

        Returns:
            クエリ実行結果
        """
        start_time = time.time()
        self.logger.info(f"クエリ実行開始: {sql[:100]}...")

        try:
            # クエリの準備（安全性チェック + LIMIT句修正）
            modified_sql = self._validate_and_prepare_query(sql)

            # ドライランチェック（force_executeがFalseの場合のみ）
            if not force_execute:
                dry_run_result = await self.check_scan_amount(modified_sql, project_id)
                if not dry_run_result.is_safe:
                    return QueryExecutionResult(
                        success=False,
                        error_message=(
                            f"クエリのスキャン量が制限を超えています。"
                            f"予想スキャン量: {dry_run_result.total_bytes_processed:,} bytes, "
                            f"制限: {self.settings.max_scan_bytes:,} bytes"
                        ),
                    )

            client = self._get_client(project_id)

            # クエリ実行用の設定
            job_config = QueryJobConfig(
                use_query_cache=True,
            )

            # クエリを実行
            query_job = client.query(modified_sql, job_config=job_config)
            results = query_job.result(
                timeout=self.settings.query_timeout_seconds
            )  # 完了を待機

            # 結果をリストに変換
            rows = []
            for row in results:
                row_dict = {}
                for key, value in row.items():
                    # BigQueryの特殊な型を処理
                    if hasattr(value, "isoformat"):  # datetime objects
                        row_dict[key] = value.isoformat()
                    elif isinstance(value, (bytes, bytearray)):
                        row_dict[key] = value.decode("utf-8", errors="replace")
                    else:
                        row_dict[key] = value
                rows.append(row_dict)

            execution_time_ms = int((time.time() - start_time) * 1000)

            self.logger.info(
                f"クエリ実行完了 - 行数: {len(rows)}, "
                f"実行時間: {execution_time_ms}ms, "
                f"処理バイト数: {query_job.total_bytes_processed or 0:,}"
            )

            return QueryExecutionResult(
                success=True,
                rows=rows,
                total_rows=len(rows),
                total_bytes_processed=query_job.total_bytes_processed,
                total_bytes_billed=query_job.total_bytes_billed,
                execution_time_ms=execution_time_ms,
                job_id=query_job.job_id,
            )

        except HTTPException as http_exc:
            self.logger.error(f"HTTPエラー: {http_exc.detail}")
            raise http_exc
        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)
            error_msg = str(e)

            self.logger.error(f"クエリ実行エラー: {error_msg}")

            return QueryExecutionResult(
                success=False,
                execution_time_ms=execution_time_ms,
                error_message=error_msg,
            )

    def format_bytes(self, bytes_count: int) -> str:
        """バイト数を人間が読みやすい形式にフォーマットする"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"
