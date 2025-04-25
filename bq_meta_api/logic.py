from fastapi import HTTPException
from typing import List, Optional, Dict, Tuple
from bq_meta_api import cache_manager, log, config
from bq_meta_api.models import (
    CachedData,
    DatasetListResponse,
    DatasetMetadata,
    TableMetadata,
)


# --- ヘルパー関数 ---
async def get_current_cache() -> CachedData:
    """現在の有効なキャッシュデータを取得する。なければエラーを発生させる。"""
    logger = log.get_logger()
    cache = await cache_manager.load_cache()  # まずメモリ/ファイルから試す
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
    except Exception as e:
        logger.error(f"データセット一覧の取得中にエラーが発生: {e}")
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
    except HTTPException:
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
            cache = get_current_cache()
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
    except HTTPException:
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
