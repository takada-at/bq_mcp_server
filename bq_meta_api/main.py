# main.py: FastAPI application entry point
import logging
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Path as FastApiPath
from fastapi.responses import JSONResponse

from . import cache_manager, search_engine
from .config import settings
from .models import (
    DatasetListResponse, TableListResponse, SearchResponse,
    DatasetMetadata, TableMetadata, SearchResultItem, CachedData
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="BigQuery Metadata API Server",
    description="Provides access to cached BigQuery dataset, table, and schema information.",
    version="0.1.0",
)

# --- アプリケーション起動時の処理 ---
@app.on_event("startup")
async def startup_event():
    """アプリケーション起動時にキャッシュを読み込むか、必要であれば更新する"""
    logger.info("アプリケーション起動処理を開始します...")
    # 初回起動時にキャッシュを準備
    cache_data = cache_manager.get_cached_data()
    if cache_data:
        logger.info("キャッシュの準備が完了しました。")
    else:
        logger.warning("キャッシュの準備に失敗しました。APIは機能しない可能性があります。")
    logger.info(f"APIサーバーを {settings.api_host}:{settings.api_port} で起動します。")
    logger.info(f"監視対象プロジェクト: {settings.project_ids}")
    logger.info(f"キャッシュTTL: {settings.cache_ttl_seconds}秒")
    logger.info(f"キャッシュファイル: {settings.cache_file_path}")


# --- ヘルパー関数 ---
def get_current_cache() -> CachedData:
    """現在の有効なキャッシュデータを取得する。なければエラーを発生させる。"""
    cache = cache_manager.load_cache() # まずメモリ/ファイルから試す
    if cache and cache_manager.is_cache_valid(cache):
        return cache
    # キャッシュが無効または存在しない場合は更新を試みる
    logger.info("キャッシュが無効または存在しないため、更新を試みます...")
    updated_cache = cache_manager.update_cache()
    if not updated_cache:
        logger.error("キャッシュの更新に失敗しました。")
        raise HTTPException(status_code=503, detail="キャッシュデータの取得に失敗しました。サーバーが利用できません。")
    return updated_cache

# --- API エンドポイント ---

@app.get(
    "/datasets",
    response_model=DatasetListResponse,
    summary="Get list of all datasets",
    description="Returns a list of all datasets across all configured projects from the cache.",
)
async def get_datasets():
    """全プロジェクトのデータセット一覧を返す"""
    try:
        cache = get_current_cache()
        all_datasets: List[DatasetMetadata] = []
        for project_datasets in cache.datasets.values():
            all_datasets.extend(project_datasets)
        return DatasetListResponse(datasets=all_datasets)
    except HTTPException as e:
        raise e # キャッシュ取得失敗時のHTTPExceptionを再送出
    except Exception as e:
        logger.error(f"/datasets エンドポイントでエラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="データセットリストの取得中に内部エラーが発生しました。")

@app.get(
    "/{dataset_id}/tables",
    response_model=TableListResponse,
    summary="Get list of tables in a dataset",
    description="Returns a list of tables (metadata only, without schema) for the specified dataset ID from the cache. Searches across all configured projects.",
)
async def get_tables_in_dataset(
    dataset_id: str = FastApiPath(..., description="The ID of the dataset to retrieve tables for.")
):
    """指定されたデータセットIDに属するテーブル一覧（スキーマなし）を返す"""
    try:
        cache = get_current_cache()
        found_tables: List[TableMetadata] = []
        found_dataset = False
        for project_id, datasets_tables in cache.tables.items():
            if dataset_id in datasets_tables:
                found_dataset = True
                # スキーマ情報を含まないTableMetadataを作成して返す
                for table_meta_with_schema in datasets_tables[dataset_id]:
                    # schemaを除外して新しいインスタンスを作成
                    table_meta_dict = table_meta_with_schema.model_dump(exclude={'schema_'}) # schemaエイリアスも考慮
                    # Pydantic v2では model_validate を使用
                    found_tables.append(TableMetadata.model_validate(table_meta_dict))
                break # 最初に見つかったプロジェクトのデータセットを使用

        if not found_dataset:
            raise HTTPException(status_code=404, detail=f"データセット '{dataset_id}' がキャッシュ内に見つかりません。")

        return TableListResponse(tables=found_tables)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"/{dataset_id}/tables エンドポイントでエラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="テーブルリストの取得中に内部エラーが発生しました。")


@app.get(
    "/search",
    response_model=SearchResponse,
    summary="Search metadata",
    description="Searches dataset names, table names, column names, and descriptions in the cache for the given keyword.",
)
async def search_items(
    key: str = Query(..., description="Keyword to search for in metadata.")
):
    """キーワードに基づいてメタデータを検索する"""
    if not key:
        raise HTTPException(status_code=400, detail="検索キーワード 'key' を指定してください。")
    try:
        # 検索自体はキャッシュ更新をトリガーしない（古いキャッシュでも検索は可能とする）
        # 必要ならここで get_current_cache() を呼ぶように変更も可能
        cached_data = cache_manager.load_cache()
        if not cached_data:
             # キャッシュがない場合は、検索前に更新を試みる
             logger.info("検索前にキャッシュを更新します。")
             cached_data = cache_manager.update_cache()
             if not cached_data:
                  raise HTTPException(status_code=503, detail="検索のためのキャッシュデータが利用できません。")

        results: List[SearchResultItem] = search_engine.search_metadata(key)
        return SearchResponse(query=key, results=results)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"/search エンドポイントでエラー (key={key}): {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="メタデータの検索中に内部エラーが発生しました。")

@app.post(
    "/cache/update",
    summary="Force update cache",
    description="Forces an update of the local metadata cache by fetching fresh data from BigQuery.",
    status_code=202, # Accepted
)
async def force_update_cache():
    """キャッシュの手動更新をトリガーするエンドポイント"""
    logger.info("キャッシュの手動更新リクエストを受け付けました。")
    # ここでは更新処理をバックグラウンドで実行せず、完了を待つ
    # 大規模な場合は BackgroundTasks を使う方が良い
    try:
        updated_cache = cache_manager.update_cache()
        if updated_cache:
            return JSONResponse(
                status_code=200,
                content={"message": "キャッシュの更新が正常に完了しました。", "last_updated": updated_cache.last_updated.isoformat()}
            )
        else:
            raise HTTPException(status_code=500, detail="キャッシュの更新に失敗しました。")
    except Exception as e:
        logger.error("/cache/update エンドポイントでエラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"キャッシュ更新中にエラーが発生しました: {e}")


# --- uvicorn で実行するための設定 ---
# このファイルが直接実行された場合にuvicornを起動
if __name__ == "__main__":
    import uvicorn
    # main:app を指定するため、このファイル自体を実行するのではなく、
    # コマンドラインから `uvicorn bq_meta_api.main:app --reload --host 0.0.0.0 --port 8000` のように実行する
    print("サーバーを起動するには、以下のコマンドを実行してください:")
    print(f"uvicorn bq_meta_api.main:app --host {settings.api_host} --port {settings.api_port} --reload")
    # uvicorn.run("main:app", host=settings.api_host, port=settings.api_port, reload=True) # この書き方はモジュール解決の問題でうまく動かないことが多い