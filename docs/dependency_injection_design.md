# 依存性注入による logic.py の分離設計

## 概要

現在の `core/logic.py` は repositories 層のコードに依存しているため、これを依存性注入パターンを使って分離します。
クラスベースのDIではなく、高階関数を使った関数型のアプローチで実装します。

## 現状の依存関係分析

`core/logic.py` が依存している repositories 層のモジュール：

- `cache_manager` - キャッシュの読み書き操作
- `config` - アプリケーション設定の取得
- `log` - ロギング機能
- `QueryExecutor` - BigQuery クエリの実行

## 新しいアーキテクチャ

### ディレクトリ構造

```
bq_mcp/
├── core/
│   ├── logic_base.py  # 純粋なビジネスロジック（新規）
│   └── entities.py    # エンティティ定義（既存）
└── repositories/
    ├── logic.py       # 依存性注入と公開API（新規）
    └── ...           # その他の既存モジュール
```

## 実装設計

### 1. core/logic_base.py - ビジネスロジック層

依存性を引数として受け取る高階関数として実装：

```python
from typing import Callable, List, Optional, Tuple, Awaitable
from bq_mcp.core.entities import (
    CachedData,
    DatasetListResponse,
    DatasetMetadata,
    TableMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
)

# 型エイリアスの定義
GetCurrentCacheFunc = Callable[[], Awaitable[CachedData]]
GetCachedDatasetFunc = Callable[[str, str], Awaitable[Tuple[Optional[DatasetMetadata], List[TableMetadata]]]]
GetProjectIdsFunc = Callable[[], List[str]]
CheckScanAmountFunc = Callable[[str, Optional[str]], Awaitable[QueryDryRunResult]]
ExecuteQueryFunc = Callable[[str, Optional[str], bool], Awaitable[QueryExecutionResult]]
LoggerFunc = Callable[[str], None]

def create_get_datasets(
    get_current_cache: GetCurrentCacheFunc
) -> Callable[[], Awaitable[DatasetListResponse]]:
    """データセット一覧取得関数を生成"""
    async def get_datasets() -> DatasetListResponse:
        cache = await get_current_cache()
        all_datasets: List[DatasetMetadata] = []
        for project_datasets in cache.datasets.values():
            all_datasets.extend(project_datasets)
        return DatasetListResponse(datasets=all_datasets)
    return get_datasets

def create_get_tables(
    get_cached_dataset_data: GetCachedDatasetFunc,
    get_current_cache: GetCurrentCacheFunc,
    get_project_ids: GetProjectIdsFunc
) -> Callable[[str, Optional[str]], Awaitable[List[TableMetadata]]]:
    """テーブル一覧取得関数を生成"""
    async def get_tables(
        dataset_id: str, 
        project_id: Optional[str] = None
    ) -> List[TableMetadata]:
        if project_id:
            # プロジェクトIDが指定されている場合
            dataset, tables = await get_cached_dataset_data(project_id, dataset_id)
            if dataset is None:
                return []  # エラーハンドリングは上位層で実施
            return tables
        else:
            # プロジェクトIDが未指定の場合、全プロジェクトを検索
            cache = await get_current_cache()
            found_tables: List[TableMetadata] = []
            project_ids = get_project_ids()
            
            for proj_id in project_ids:
                if proj_id in cache.tables and dataset_id in cache.tables[proj_id]:
                    dataset, tables = await get_cached_dataset_data(proj_id, dataset_id)
                    if dataset is not None and tables:
                        found_tables.extend(tables)
            
            return found_tables
    return get_tables

def create_check_query_scan_amount(
    check_scan_amount: CheckScanAmountFunc,
    logger: LoggerFunc
) -> Callable[[str, Optional[str]], Awaitable[QueryDryRunResult]]:
    """クエリスキャン量チェック関数を生成"""
    async def check_query_scan_amount(
        sql: str, 
        project_id: Optional[str] = None
    ) -> QueryDryRunResult:
        result = await check_scan_amount(sql, project_id)
        logger(f"Scan amount check completed: {result.total_bytes_processed:,} bytes")
        return result
    return check_query_scan_amount

def create_execute_query(
    execute_query_impl: ExecuteQueryFunc,
    logger: LoggerFunc
) -> Callable[[str, Optional[str], bool], Awaitable[QueryExecutionResult]]:
    """クエリ実行関数を生成"""
    async def execute_query(
        sql: str,
        project_id: Optional[str] = None,
        force: bool = False
    ) -> QueryExecutionResult:
        result = await execute_query_impl(sql, project_id, force)
        
        if result.success:
            logger(f"Query execution successful - result rows: {result.total_rows}")
        else:
            logger(f"Query execution failed: {result.error_message}")
        
        return result
    return execute_query
```

### 2. repositories/logic.py - インフラストラクチャ層

実際の依存性を注入して公開APIを提供：

```python
import asyncio
from typing import Optional
from fastapi import HTTPException

from bq_mcp.core import logic_base
from bq_mcp.core.entities import CachedData
from bq_mcp.repositories import cache_manager, config, log
from bq_mcp.repositories.query_executor import QueryExecutor

# プライベート実装関数
async def _get_current_cache_impl() -> CachedData:
    """キャッシュ取得の実装（HTTPException含む）"""
    logger = log.get_logger()
    cache = cache_manager.load_cache()
    if cache and cache_manager.is_cache_valid(cache):
        return cache

    if cache:
        logger.warning(
            "Cache is expired, using stale cache and triggering background update"
        )
        asyncio.create_task(_trigger_background_update())
        return cache

    logger.info("No cache exists, performing initial cache load...")
    updated_cache = await cache_manager.update_cache()
    if not updated_cache:
        logger.error("Failed to update cache.")
        raise HTTPException(
            status_code=503,
            detail="Failed to retrieve cache data. Server is unavailable.",
        )
    return updated_cache

async def _trigger_background_update():
    """バックグラウンドキャッシュ更新"""
    logger = log.get_logger()
    try:
        logger.info("Starting background cache update from logic layer")
        updated_cache = await cache_manager.update_cache()
        if updated_cache:
            cache_manager.save_cache(updated_cache)
            logger.info("Background cache update completed")
    except Exception:
        logger.exception("Background cache update failed")

# QueryExecutor のラッパー関数
async def _check_scan_amount_impl(sql: str, project_id: Optional[str] = None):
    settings = config.get_settings()
    query_executor = QueryExecutor(settings)
    return await query_executor.check_scan_amount(sql, project_id)

async def _execute_query_impl(
    sql: str, 
    project_id: Optional[str] = None, 
    force: bool = False
):
    settings = config.get_settings()
    query_executor = QueryExecutor(settings)
    return await query_executor.execute_query(sql, project_id, force_execute=force)

# ロガー関数
def _logger_info(message: str):
    log.get_logger().info(message)

def _logger_warning(message: str):
    log.get_logger().warning(message)

# 依存性を注入して公開関数を生成
get_datasets = logic_base.create_get_datasets(
    get_current_cache=_get_current_cache_impl
)

get_tables = logic_base.create_get_tables(
    get_cached_dataset_data=cache_manager.get_cached_dataset_data,
    get_current_cache=_get_current_cache_impl,
    get_project_ids=lambda: config.get_settings().project_ids
)

check_query_scan_amount = logic_base.create_check_query_scan_amount(
    check_scan_amount=_check_scan_amount_impl,
    logger=_logger_info
)

execute_query = logic_base.create_execute_query(
    execute_query_impl=_execute_query_impl,
    logger=_logger_info
)

# エラーハンドリングを追加したラッパー関数
async def get_datasets_with_error_handling():
    """HTTPException を含むエラーハンドリング付きデータセット取得"""
    try:
        return await get_datasets()
    except HTTPException:
        raise
    except Exception as e:
        logger = log.get_logger()
        logger.error(f"Error occurred while retrieving dataset list: {e}")
        raise HTTPException(
            status_code=503,
            detail="Failed to retrieve dataset list. Server is unavailable.",
        )

async def get_tables_with_error_handling(
    dataset_id: str, 
    project_id: Optional[str] = None
):
    """HTTPException を含むエラーハンドリング付きテーブル取得"""
    tables = await get_tables(dataset_id, project_id)
    
    if not tables and project_id:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{project_id}.{dataset_id}' not found.",
        )
    elif not tables:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset '{dataset_id}' not found.",
        )
    
    return tables
```

## メリット

1. **テスタビリティの向上**
   - 依存性をモックに差し替えて単体テストが容易に
   - ビジネスロジックを独立してテスト可能

2. **疎結合**
   - core層がrepositories層の実装詳細に依存しない
   - インターフェースが明確に定義される

3. **再利用性**
   - 異なる実装を注入することで別の環境での利用が可能
   - 例：テスト環境用のメモリキャッシュ実装など

4. **関数型アプローチ**
   - クラスベースのDIコンテナが不要
   - シンプルで理解しやすい実装

## 移行手順

1. `core/logic_base.py` を新規作成
2. `repositories/logic.py` を新規作成
3. 既存の `core/logic.py` から新しいモジュールへ段階的に移行
4. インポート元を更新（`web.py`, `mcp_server.py` など）
5. テストを追加・更新
6. 古い `core/logic.py` を削除

## テスト戦略

### core/logic_base.py のテスト

```python
import pytest
from unittest.mock import AsyncMock
from bq_mcp.core import logic_base

async def test_get_datasets():
    # モックの準備
    mock_cache = CachedData(
        datasets={"project1": [dataset1, dataset2]},
        tables={},
        last_updated=datetime.now()
    )
    mock_get_cache = AsyncMock(return_value=mock_cache)
    
    # 関数の生成とテスト
    get_datasets = logic_base.create_get_datasets(mock_get_cache)
    result = await get_datasets()
    
    assert len(result.datasets) == 2
    mock_get_cache.assert_called_once()
```

### repositories/logic.py のテスト

実際の依存性を使った統合テストや、HTTPExceptionの発生を確認するテストなど。

## 今後の拡張性

この設計により、以下のような拡張が容易になります：

1. **キャッシュ戦略の変更**
   - Redis キャッシュへの移行
   - 分散キャッシュの実装

2. **マルチテナント対応**
   - テナントごとに異なる設定を注入

3. **監査ログ機能**
   - ロガー関数を拡張して監査ログを追加

4. **パフォーマンス最適化**
   - 非同期処理の最適化
   - バッチ処理の追加