from fastapi import HTTPException
from typing import List, Optional
import traceback
from bq_meta_api.repositories import config
from bq_meta_api.core.entities import (
    CachedData,
    DatasetListResponse,
    DatasetMetadata,
    TableMetadata,
    QueryExecutionResult,
    QueryDryRunResult,
)
from bq_meta_api.repositories import cache_manager, log
from bq_meta_api.repositories.query_executor import QueryExecutor


# --- ヘルパー関数 ---
async def get_current_cache() -> CachedData:
    """現在の有効なキャッシュデータを取得する。なければエラーを発生させる。"""
    logger = log.get_logger()
    cache = cache_manager.load_cache()  # まずメモリ/ファイルから試す
    if cache and cache_manager.is_cache_valid(cache):
        return cache
    # キャッシュが無効または存在しない場合は更新を試みる
    logger.info("キャッシュが無効または存在しないため、更新を試みます...")
    updated_cache = cache_manager.update_cache()
    if not updated_cache:
        logger.error("キャッシュの更新に失敗しました。")
        raise HTTPException(
            status_code=503,
            detail="キャッシュデータの取得に失敗しました。サーバーが利用できません。",
        )
    return updated_cache


async def get_datasets() -> DatasetListResponse:
    """全プロジェクトのデータセット一覧を返す"""
    # グローバルキャッシュからデータセット一覧を取得
    logger = log.get_logger()
    try:
        cache = await get_current_cache()
        all_datasets: List[DatasetMetadata] = []
        for project_datasets in cache.datasets.values():
            all_datasets.extend(project_datasets)
        return DatasetListResponse(datasets=all_datasets)
    except HTTPException:  # Specific HTTPException should be re-raised
        raise
    except Exception as e:
        logger.error(f"データセット一覧の取得中にエラーが発生: {e}")
        logger.error(traceback.format_exception(e))
        raise HTTPException(
            status_code=503,
            detail="データセット一覧の取得に失敗しました。サーバーが利用できません。",
        )


async def get_datasets_by_project(project_id: str) -> DatasetListResponse:
    """指定されたプロジェクトのデータセット一覧を返す"""
    logger = log.get_logger()
    try:
        cache = await get_current_cache()
        if project_id not in cache.datasets:
            raise HTTPException(
                status_code=404,
                detail=f"プロジェクト '{project_id}' は見つかりません。",
            )
        return DatasetListResponse(datasets=cache.datasets[project_id])
    except HTTPException:  # Specific HTTPException should be re-raised
        raise
    except Exception as e:
        logger.error(
            f"プロジェクト '{project_id}' のデータセット一覧の取得中にエラーが発生: {e}"
        )
        raise HTTPException(
            status_code=503,
            detail=f"プロジェクト '{project_id}' のデータセット一覧の取得に失敗しました。",
        )


async def get_tables(
    dataset_id: str, project_id: Optional[str] = None
) -> List[TableMetadata]:
    """指定されたデータセットのテーブル一覧を返す
    Args:
        dataset_id: データセットID
        project_id: プロジェクトID（オプション）
    Returns:
        List[TableMetadata]: テーブルメタデータのリスト
    Raises:
        HTTPException: プロジェクトまたはデータセットが見つからない場合
    """
    found_tables: List[TableMetadata] = []
    logger = log.get_logger()

    try:
        if project_id:
            # プロジェクトIDが指定されている場合、そのデータセットのキャッシュを直接取得
            dataset, tables = cache_manager.get_cached_dataset_data(
                project_id, dataset_id
            )
            if dataset is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"データセット '{project_id}.{dataset_id}' は見つかりません。",
                )
            return tables
        else:
            # プロジェクトIDが指定されていない場合、すべてのプロジェクトから検索
            cache = await get_current_cache()  # Added await
            found_dataset = False
            settings = config.get_settings()

            for proj_id in settings.project_ids:
                if proj_id in cache.tables and dataset_id in cache.tables[proj_id]:
                    found_dataset = True
                    dataset, tables = cache_manager.get_cached_dataset_data(
                        proj_id, dataset_id
                    )
                    if dataset is not None and tables:
                        found_tables.extend(tables)

            if not found_dataset:
                raise HTTPException(
                    status_code=404,
                    detail=f"データセット '{dataset_id}' は見つかりません。",
                )

            return found_tables
    except HTTPException:  # Specific HTTPException should be re-raised
        raise
    except Exception as e:
        project_info = f"{project_id}." if project_id else ""
        logger.error(
            f"テーブル一覧の取得中にエラーが発生: {project_info}{dataset_id}, {e}"
        )
        raise HTTPException(
            status_code=503,
            detail=f"データセット '{project_info}{dataset_id}' のテーブル一覧の取得に失敗しました。",
        )


async def check_query_scan_amount(
    sql: str, project_id: Optional[str] = None
) -> QueryDryRunResult:
    """BigQueryクエリのスキャン量を事前にチェックする（元のクエリのまま）"""
    logger = log.get_logger()
    settings = config.get_settings()

    try:
        query_executor = QueryExecutor(settings)
        result = await query_executor.check_scan_amount(sql, project_id)

        logger.info(f"スキャン量チェック完了: {result.total_bytes_processed:,} bytes")
        return result

    except HTTPException as http_exc:
        logger.error(f"スキャン量チェックでHTTPエラー: {http_exc.detail}")
        raise http_exc
    except Exception as e:
        logger.error(f"スキャン量チェックでエラーが発生しました: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"スキャン量チェック中にエラーが発生しました: {str(e)}",
        )


async def execute_query(
    sql: str, project_id: Optional[str] = None, force: bool = False
) -> QueryExecutionResult:
    """BigQueryクエリを安全に実行する"""

    logger = log.get_logger()
    settings = config.get_settings()

    try:
        query_executor = QueryExecutor(settings)
        result = await query_executor.execute_query(
            sql, project_id, force_execute=force
        )

        if result.success:
            logger.info(f"クエリ実行成功 - 結果行数: {result.total_rows}")
        else:
            logger.warning(f"クエリ実行失敗: {result.error_message}")

        return result
    except Exception as e:
        logger.error(f"クエリ実行中にエラーが発生: {e}")
        raise HTTPException(
            status_code=500, detail="クエリ実行中に内部エラーが発生しました。"
        )
