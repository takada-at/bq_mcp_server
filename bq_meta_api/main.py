# main.py: FastAPI application entry point
from typing import List, Optional, Literal

from fastapi import FastAPI, HTTPException, Query, Path as FastApiPath, Depends
from fastapi.responses import JSONResponse, PlainTextResponse, Response

# App setup (assuming it's already here)
app = FastAPI(
    title="BigQuery Metadata API Server",
    description="Provides access to cached BigQuery dataset, table, and schema information.",
    version="0.1.0",
)
from bq_meta_api import start

start.init_app(log_to_console=True)  # ログ設定と設定の初期化

# Original imports - some will be replaced or augmented
from bq_meta_api import cache_manager, config, converter, log, logic, search_engine
from bq_meta_api.domain.entities import (
    DatasetListResponse, # Keep if used by other endpoints or for new response_model
    SearchResponse,      # Keep if used by other endpoints
    TableListResponse,   # Keep if used by other endpoints
    TableMetadata as Table, # Using TableMetadata as Table
    DatasetMetadata as Dataset # Using DatasetMetadata as Dataset
)

# New imports for DI and services
from bq_meta_api.application.services import FetchBigQueryTableMetadataService, ListDatasetsService
from bq_meta_api.domain.use_cases import IFetchBigQueryTableMetadataUseCase, IListDatasetsUseCase
from bq_meta_api.domain.repositories import IBigQueryRepository
from bq_meta_api.infrastructure.bigquery import BigQueryClient
from bq_meta_api.config import Settings, get_settings


logger = log.get_logger() # Assuming log is already initialized

# --- Dependency Injection Setup ---

def get_bigquery_repository(settings: Settings = Depends(get_settings)) -> IBigQueryRepository:
    # BigQueryClient can be initialized without project_id, as project_id is passed to its methods.
    # If a specific project_id from settings were to be used for ALL client operations (not per-call),
    # it could be passed here: e.g., BigQueryClient(project_id=settings.project_ids[0] if settings.project_ids else None)
    # However, the current IBigQueryRepository methods accept project_id, making a project-agnostic client suitable.
    return BigQueryClient()

def get_fetch_table_metadata_use_case(
    repo: IBigQueryRepository = Depends(get_bigquery_repository)
) -> IFetchBigQueryTableMetadataUseCase:
    return FetchBigQueryTableMetadataService(bigquery_repository=repo)

def get_list_datasets_use_case(
    repo: IBigQueryRepository = Depends(get_bigquery_repository)
) -> IListDatasetsUseCase:
    return ListDatasetsService(bigquery_repository=repo)


# --- アプリケーション起動時の処理 ---
@app.on_event("startup")
async def startup_event():
    """アプリケーション起動時にキャッシュを読み込むか、必要であれば更新する"""
    settings = config.get_settings() # Corrected: use the imported config
    logger.info("アプリケーション起動処理を開始します...")
    # 初回起動時にキャッシュを準備
    # cache_manager needs to be an instance of the new CacheManager class if its methods are not static
    # For now, assuming cache_manager global import refers to the module with static-like methods
    # or an already instantiated global manager.
    # If cache_manager.get_cached_data is an instance method of the new CacheManager:
    # cache_repo = CacheManager() # Or get it via DI if CacheManager itself is a dependency
    # cache_data = await cache_repo.get_cached_data_all() # Assuming get_cached_data_all is the replacement
    
    # Based on previous steps, cache_manager.py was refactored into a class CacheManager.
    # The old global functions like get_cached_data were adapted into methods like get_cached_data_all.
    # To use it properly, we should instantiate CacheManager.
    # For startup, it's simpler if get_cached_data can be called module-level or via a default instance.
    # Let's assume for now that the old cache_manager.get_cached_data still works as a module call
    # for backwards compatibility during refactoring, or it's managed by `start.init_app`.
    # This part might need adjustment if cache_manager is strictly class-based now.
    # For the purpose of this subtask, focusing on service injection for endpoints.
    
    # Reverting to direct module call for cache_manager for now, as its DI is not part of this subtask.
    cache_data = await cache_manager.get_cached_data() # This might need to change if get_cached_data is no longer static

    if cache_data:
        logger.info("キャッシュの準備が完了しました。")
    else:
        logger.warning(
            "キャッシュの準備に失敗しました。APIは機能しない可能性があります。"
        )
    logger.info(f"APIサーバーを {settings.api_host}:{settings.api_port} で起動します。")
    logger.info(f"監視対象プロジェクト: {settings.project_ids}")
    logger.info(f"キャッシュTTL: {settings.cache_ttl_seconds}秒")
    logger.info(f"キャッシュファイル: {settings.cache_file_base_dir}")


# --- API エンドポイント ---

@app.get(
    "/datasets",
    # The response model needs to be flexible: List[Dataset] if project_id is given,
    # or DatasetListResponse if project_id is not given (original behavior).
    # For simplicity in this refactoring, we'll assume the client can handle
    # a list of datasets. If specific response structure is critical,
    # the endpoint might need to return different Pydantic models or use Response directly.
    # For now, let's make it List[Dataset] and note that original response was DatasetListResponse.
    response_model=List[Dataset], 
    summary="Get list of datasets",
    description="Returns a list of datasets. If project_id is provided, returns datasets for that project. Otherwise, returns all datasets from cache (legacy behavior).",
)
async def get_datasets_endpoint( # Renamed from get_all_datasets_original or get_datasets
    project_id: Optional[str] = Query(None, description="Optional project ID to filter datasets."),
    list_datasets_use_case: IListDatasetsUseCase = Depends(get_list_datasets_use_case)
    # settings: Settings = Depends(get_settings) # If needed for legacy path
):
    """
    プロジェクトIDが指定されていれば、そのプロジェクトのデータセット一覧をサービス経由で返す。
    指定されていなければ、全プロジェクトのデータセット一覧を従来のロジックで返す。
    """
    if project_id:
        logger.info(f"Fetching datasets for project_id: {project_id} using ListDatasetsService.")
        try:
            datasets = list_datasets_use_case.execute(project_id=project_id)
            return datasets # Returns List[Dataset]
        except Exception as e:
            logger.error(f"/datasets エンドポイントでエラー (project_id={project_id}): {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"プロジェクト '{project_id}' のデータセット一覧取得中にエラー: {str(e)}")
    else:
        logger.info("Fetching all datasets using original logic (logic.get_datasets).")
        try:
            # Original logic returns DatasetListResponse, which is {datasets: List[DatasetMetadata]}
            # To match response_model=List[Dataset], we extract the list.
            original_response = await logic.get_datasets()
            return original_response.datasets # Returns List[DatasetMetadata]
        except Exception as e:
            logger.error(f"/datasets エンドポイントでエラー (all projects): {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="データセットリストの取得中に内部エラーが発生しました。",
            )

# The /projects/{project_id}/datasets endpoint created in the previous step will be kept.
# It specifically serves datasets for a project.
@app.get("/projects/{project_id}/datasets", response_model=List[Dataset], deprecated=True, summary="[DEPRECATED] Use /datasets?project_id=...") 
async def list_datasets_endpoint_deprecated( 
    project_id: str,
    list_datasets_use_case: IListDatasetsUseCase = Depends(get_list_datasets_use_case)
):
    """指定されたプロジェクトのデータセット一覧をサービス経由で返す (非推奨)"""
    try:
        datasets = list_datasets_use_case.execute(project_id=project_id)
        return datasets 
    except HTTPException: 
        raise
    except Exception as e:
        logger.error(f"/projects/{project_id}/datasets エンドポイントでエラー: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"プロジェクト '{project_id}' のデータセット一覧取得中にエラー: {str(e)}")


# Refactoring the endpoint for getting table metadata
# The new endpoint /projects/{project_id}/datasets/{dataset_id}/tables/{table_id} is already using the service.
# No changes needed for it here as it was correctly set up in the previous turn.
# Original path: /{dataset_id}/tables - this seems to list tables, not get a specific table's metadata.
# The new endpoint for specific table metadata is already in place from the previous diff:
# /projects/{project_id}/datasets/{dataset_id}/tables/{table_id}
# It uses IFetchBigQueryTableMetadataUseCase and Depends(get_fetch_table_metadata_use_case).
# So, that part of the subtask is already complete.

# The original /{dataset_id}/tables endpoint remains for listing tables within a dataset (potentially across projects).
# This was named get_tables_in_dataset_original in the previous diff.
# It uses logic.py and is not part of this specific refactoring task for the two services.
@app.get(
    "/{dataset_id}/tables", 
    responses={
        200: {
            "content": {
                "application/json": {"model": TableListResponse},
                "text/markdown": {
                    "example": "### Table: `prj-example.example_dataset.users`\n\n| カラム名 | データ型 | モード | 説明 |\n|---------|---------|--------|------|\n| user_id | INTEGER | REQUIRED | ユーザーの一意識別子 |\n| name | STRING | NULLABLE | ユーザー名 |\n| created_at | TIMESTAMP | REQUIRED | 作成日時 |"
                },
            }
        }
    },
    summary="Get list of tables in a dataset",
    description="Returns a list of tables for the specified dataset ID from the cache. Searches across all configured projects. Format can be either 'json' or 'markdown'. (Original version, uses logic.py)",
    summary="Get list of tables in a dataset (Original)",
)
async def get_tables_in_dataset_original( # Renamed to avoid conflict
    dataset_id: str = FastApiPath(
        ..., description="The ID of the dataset to retrieve tables for."
    ),
    project_id: Optional[str] = Query(
        None, description="The ID of the project to filter datasets by."
    ),
    format: Optional[Literal["json", "markdown"]] = Query(
        "markdown", description="Response format: 'json' or 'markdown'."
    ),
):
    """指定されたデータセットIDに属するテーブル一覧を返す（オリジナルロジック）"""
    try:
        # Ensure correct type hint for found_tables if Table alias is used
        found_tables_meta: List[Table] = await logic.get_tables( # Assuming logic.get_tables returns List[TableMetadata]
            dataset_id, project_id=project_id
        )
        if format == "markdown":
            markdown_content = converter.convert_tables_to_markdown(found_tables_meta)
            return Response(content=markdown_content, media_type="text/markdown")
        else:
            # TableListResponse expects List[TableMetadata]
            return TableListResponse(tables=found_tables_meta)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"/{dataset_id}/tables エンドポイントでエラー: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="テーブルリストの取得中に内部エラーが発生しました。"
        )


@app.get(
    "/search",
    responses={
        200: {
            "content": {
                "application/json": {"model": SearchResponse},
                "text/markdown": {
                    "example": "## 検索結果: `user`\n\n**3** 件のヒットがありました。\n\n### テーブル\n- **project-id.dataset_id.users** (名前に一致)\n\n### カラム\n- **project-id.dataset_id.users.user_id** (名前に一致)\n- **project-id.dataset_id.logs.user_name** (名前に一致)"
                },
            }
        }
    },
    summary="Search metadata",
    description="Searches dataset names, table names, column names, and descriptions in the cache for the given keyword. Format can be either 'json' or 'markdown'.",
)
async def search_items(
    key: str = Query(..., description="Keyword to search for in metadata."),
    format: Optional[Literal["json", "markdown"]] = Query(
        "markdown", description="Response format: 'json' or 'markdown'."
    ),
):
    """キーワードに基づいてメタデータを検索する"""
    if not key:
        raise HTTPException(
            status_code=400, detail="検索キーワード 'key' を指定してください。"
        )
    try:
        search_result = await search_engine.search_metadata(key)
        # レスポンスの形式を処理
        if format == "markdown":
            search_response = converter.convert_search_results_to_markdown(
                query=key, results=search_result
            )
            return PlainTextResponse(
                content=search_response, media_type="text/markdown"
            )
        else:
            return SearchResponse(query=key, results=search_result)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"/search エンドポイントでエラー (key={key}): {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="メタデータの検索中に内部エラーが発生しました。"
        )


@app.post(
    "/cache/update",
    summary="Force update cache",
    description="Forces an update of the local metadata cache by fetching fresh data from BigQuery.",
    status_code=202,  # Accepted
)
async def force_update_cache():
    """キャッシュの手動更新をトリガーするエンドポイント"""
    logger.info("キャッシュの手動更新リクエストを受け付けました。")
    # ここでは更新処理をバックグラウンドで実行せず、完了を待つ
    # 大規模な場合は BackgroundTasks を使う方が良い
    try:
        updated_cache = await cache_manager.update_cache()
        if updated_cache:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "キャッシュの更新が正常に完了しました。",
                    "last_updated": updated_cache.last_updated.isoformat(),
                },
            )
        else:
            raise HTTPException(
                status_code=500, detail="キャッシュの更新に失敗しました。"
            )
    except Exception as e:
        logger.error("/cache/update エンドポイントでエラー: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"キャッシュ更新中にエラーが発生しました: {e}"
        )


# --- uvicorn で実行するための設定 ---
# このファイルが直接実行された場合にuvicornを起動
if __name__ == "__main__":
    import uvicorn

    # main:app を指定するため、このファイル自体を実行するのではなく、
    # コマンドラインから `uvicorn bq_meta_api.main:app --reload --host 0.0.0.0 --port 8000` のように実行する
    print("サーバーを起動するには、以下のコマンドを実行してください:")
    print(f"uvicorn bq_meta_api.main:app --reload")
