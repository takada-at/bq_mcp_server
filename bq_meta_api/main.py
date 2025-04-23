# main.py: FastAPI application entry point
from typing import List, Optional, Literal

from fastapi import FastAPI, HTTPException, Query, Path as FastApiPath
from fastapi.responses import JSONResponse, PlainTextResponse, Response

app = FastAPI(
    title="BigQuery Metadata API Server",
    description="Provides access to cached BigQuery dataset, table, and schema information.",
    version="0.1.0",
)
from bq_meta_api import log

log.init_logger()
logger = log.logger

from bq_meta_api import cache_manager, converter, logic, search_engine
from bq_meta_api.config import settings
from bq_meta_api.models import (
    DatasetListResponse,
    SearchResponse,
    TableListResponse,
    TableMetadata,
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
    response_model=DatasetListResponse,
    summary="Get list of all datasets",
    description="Returns a list of all datasets across all configured projects from the cache.",
)
async def get_datasets():
    """全プロジェクトのデータセット一覧を返す"""
    try:
        return logic.get_datasets()
    except Exception as e:
        logger.error(f"/datasets エンドポイントでエラー: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="データセットリストの取得中に内部エラーが発生しました。",
        )


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
    description="Returns a list of tables for the specified dataset ID from the cache. Searches across all configured projects. Format can be either 'json' or 'markdown'.",
)
async def get_tables_in_dataset(
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
    """指定されたデータセットIDに属するテーブル一覧を返す"""
    try:
        found_tables: List[TableMetadata] = logic.get_tables(
            dataset_id, project_id=project_id
        )
        if format == "markdown":
            # マークダウン形式のレスポンスを生成
            markdown_content = converter.convert_tables_to_markdown(found_tables)
            return Response(content=markdown_content, media_type="text/markdown")
        else:
            # JSON形式のレスポンスを生成
            return TableListResponse(tables=found_tables)
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
        search_result = search_engine.search_metadata(key)
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
        updated_cache = cache_manager.update_cache()
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
    print(
        f"uvicorn bq_meta_api.main:app --host {settings.api_host} --port {settings.api_port} --reload"
    )
    # uvicorn.run("main:app", host=settings.api_host, port=settings.api_port, reload=True) # この書き方はモジュール解決の問題でうまく動かないことが多い
