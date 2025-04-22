from fastapi import HTTPException
from typing import List, Optional
from bq_meta_api import cache_manager, log
from bq_meta_api.models import (
    CachedData,
    DatasetListResponse,
    DatasetMetadata,
    TableMetadata,
)


logger = log.logger


# --- ヘルパー関数 ---
def get_current_cache() -> CachedData:
    """現在の有効なキャッシュデータを取得する。なければエラーを発生させる。"""
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


def get_datasets() -> DatasetListResponse:
    """全プロジェクトのデータセット一覧を返す"""
    cache = get_current_cache()
    all_datasets: List[DatasetMetadata] = []
    for project_datasets in cache.datasets.values():
        all_datasets.extend(project_datasets)
    return DatasetListResponse(datasets=all_datasets)


def get_datasets_by_project(project_id: str) -> DatasetListResponse:
    """指定されたプロジェクトのデータセット一覧を返す"""
    cache = get_current_cache()
    if project_id not in cache.datasets:
        raise HTTPException(
            status_code=404, detail=f"プロジェクト '{project_id}' は見つかりません。"
        )
    return DatasetListResponse(datasets=cache.datasets[project_id])


def get_tables(
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
    cache = get_current_cache()
    found_tables: List[TableMetadata] = []
    if project_id:
        # プロジェクトIDが指定されている場合、そのプロジェクトのデータセットを検索
        if project_id not in cache.datasets:
            raise HTTPException(
                status_code=404,
                detail=f"プロジェクト '{project_id}' は見つかりません。",
            )
        datasets_tables = cache.tables.get(project_id, {})
        if dataset_id in datasets_tables:
            # テーブル一覧とスキーマ情報を取得
            for table_meta_with_schema in datasets_tables[dataset_id]:
                found_tables.append(table_meta_with_schema)
    else:
        found_dataset: bool = False
        for project_id, datasets_tables in cache.tables.items():
            if dataset_id in datasets_tables:
                found_dataset = True
                # テーブル一覧とスキーマ情報を取得
                for table_meta_with_schema in datasets_tables[dataset_id]:
                    found_tables.append(table_meta_with_schema)
        if not found_dataset:
            raise HTTPException(
                status_code=404,
                detail=f"データセット '{dataset_id}' は見つかりません。",
            )
    return found_tables
