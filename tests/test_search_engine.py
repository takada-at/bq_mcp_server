import pytest
from unittest.mock import patch
import datetime
from bq_mcp.repositories.search_engine import search_metadata, _search_columns
from bq_mcp.core.entities import (
    CachedData,
    TableSchema,
    ColumnSchema,
    DatasetMetadata,
    TableMetadata,
)


@pytest.fixture
def test_timestamp():
    """テスト用のタイムスタンプを提供するフィクスチャ"""
    return datetime.datetime.now(datetime.timezone.utc)


@pytest.fixture
def test_cached_data(test_timestamp):
    """テスト用のキャッシュデータを提供するフィクスチャ"""
    # カラムスキーマの作成
    user_id_column = ColumnSchema(
        name="user_id",
        type="STRING",
        mode="REQUIRED",
        description="ユーザーの一意識別子",
    )

    created_at_column = ColumnSchema(
        name="created_at",
        type="TIMESTAMP",
        mode="REQUIRED",
        description="ユーザーが作成された時間",
    )

    address_column = ColumnSchema(
        name="address",
        type="RECORD",
        mode="NULLABLE",
        description="ユーザーの住所情報",
        fields=[
            ColumnSchema(
                name="postal_code",
                type="STRING",
                mode="NULLABLE",
                description="郵便番号",
            ),
            ColumnSchema(
                name="prefecture",
                type="STRING",
                mode="NULLABLE",
                description="都道府県",
            ),
        ],
    )

    # テーブルスキーマの作成
    user_schema = TableSchema(
        columns=[user_id_column, created_at_column, address_column]
    )

    product_id_column = ColumnSchema(
        name="product_id",
        type="STRING",
        mode="REQUIRED",
        description="商品の一意識別子",
    )

    product_name_column = ColumnSchema(
        name="product_name", type="STRING", mode="REQUIRED", description="商品名"
    )

    # テーブルスキーマの作成
    product_schema = TableSchema(columns=[product_id_column, product_name_column])

    # データセットメタデータの作成
    user_dataset = DatasetMetadata(
        project_id="test-project",
        dataset_id="user_data",
        description="ユーザー関連のデータセット",
    )

    product_dataset = DatasetMetadata(
        project_id="test-project",
        dataset_id="product_data",
        description="商品関連のデータセット",
    )

    # テーブルメタデータの作成
    users_table = TableMetadata(
        project_id="test-project",
        dataset_id="user_data",
        table_id="users",
        full_table_id="test-project.user_data.users",
        schema_=user_schema,
        description="ユーザー情報テーブル",
        num_rows=1000,
        created_time=test_timestamp,
    )

    products_table = TableMetadata(
        project_id="test-project",
        dataset_id="product_data",
        table_id="products",
        full_table_id="test-project.product_data.products",
        schema_=product_schema,
        description="商品情報テーブル",
        num_rows=500,
        created_time=test_timestamp,
    )

    # キャッシュデータの作成
    return CachedData(
        datasets={"test-project": [user_dataset, product_dataset]},
        tables={
            "test-project": {
                "user_data": [users_table],
                "product_data": [products_table],
            }
        },
        last_updated=test_timestamp,
    )


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_no_cache(mock_get_cached_data):
    """キャッシュデータがない場合は空のリストを返す"""
    # キャッシュデータがない状態をモック
    mock_get_cached_data.return_value = None

    # 検索実行
    results = await search_metadata("user")

    # 結果の検証
    assert len(results) == 0
    assert isinstance(results, list)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_dataset_name(mock_get_cached_data, test_cached_data):
    """データセット名でキーワードマッチする場合のテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 検索実行
    results = await search_metadata("user")

    # 結果の検証 - データセット名マッチが含まれているか
    dataset_results = [
        r for r in results if r.type == "dataset" and r.match_location == "name"
    ]
    assert any(r.dataset_id == "user_data" for r in dataset_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_dataset_description(
    mock_get_cached_data, test_cached_data
):
    """データセットの説明でキーワードマッチする場合のテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 検索実行
    results = await search_metadata("ユーザー関連")

    # 結果の検証 - データセット説明マッチが含まれているか
    dataset_results = [
        r for r in results if r.type == "dataset" and r.match_location == "description"
    ]
    assert any(r.dataset_id == "user_data" for r in dataset_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_table_name(mock_get_cached_data, test_cached_data):
    """テーブル名でキーワードマッチする場合のテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 検索実行
    results = await search_metadata("user")

    # 結果の検証 - テーブル名マッチが含まれているか
    table_results = [
        r for r in results if r.type == "table" and r.match_location == "name"
    ]
    assert any(r.table_id == "users" for r in table_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_table_description(
    mock_get_cached_data, test_cached_data
):
    """テーブルの説明でキーワードマッチする場合のテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 検索実行
    results = await search_metadata("ユーザー情報")

    # 結果の検証 - テーブル説明マッチが含まれているか
    table_results = [
        r for r in results if r.type == "table" and r.match_location == "description"
    ]
    assert any(r.table_id == "users" for r in table_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_column_name(mock_get_cached_data, test_cached_data):
    """カラム名でキーワードマッチする場合のテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 検索実行
    results = await search_metadata("user_id")

    # 結果の検証 - カラム名マッチが含まれているか
    column_results = [
        r for r in results if r.type == "column" and r.match_location == "name"
    ]
    assert any(r.column_name == "user_id" for r in column_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_column_description(
    mock_get_cached_data, test_cached_data
):
    """カラムの説明でキーワードマッチする場合のテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 検索実行
    results = await search_metadata("一意識別子")

    # 結果の検証 - カラム説明マッチが含まれているか
    column_results = [
        r for r in results if r.type == "column" and r.match_location == "description"
    ]
    assert any(r.column_name == "user_id" for r in column_results)
    assert any(r.column_name == "product_id" for r in column_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_nested_column(mock_get_cached_data, test_cached_data):
    """ネストしたカラムでキーワードマッチする場合のテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 検索実行
    results = await search_metadata("postal_code")

    # 結果の検証 - ネストしたカラム名マッチが含まれているか
    column_results = [
        r for r in results if r.type == "column" and r.match_location == "name"
    ]
    assert any(r.column_name == "postal_code" for r in column_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_case_insensitive(mock_get_cached_data, test_cached_data):
    """検索がケースインセンシティブであることを確認するテスト"""
    # キャッシュデータをモック
    mock_get_cached_data.return_value = test_cached_data

    # 大文字で検索実行
    results = await search_metadata("USER")

    # 結果の検証 - 大文字小文字関係なくマッチすること
    assert any(r.dataset_id == "user_data" for r in results if r.type == "dataset")
    assert any(r.table_id == "users" for r in results if r.type == "table")


def test_search_columns():
    """_search_columns関数の単体テスト"""
    # テスト用のカラムリストを作成
    columns = [
        ColumnSchema(
            name="test_column",
            type="STRING",
            mode="REQUIRED",
            description="テスト用カラム",
        ),
        ColumnSchema(
            name="nested_col",
            type="RECORD",
            mode="NULLABLE",
            description="ネストした列",
            fields=[
                ColumnSchema(
                    name="child_col",
                    type="STRING",
                    mode="NULLABLE",
                    description="子カラム",
                )
            ],
        ),
    ]

    # 検索実行
    results = _search_columns(
        columns=columns,
        keyword="test",
        project_id="test-project",
        dataset_id="test-dataset",
        table_id="test-table",
    )

    # 結果の検証
    assert len(results) == 1
    assert results[0].type == "column"
    assert results[0].column_name == "test_column"
    assert results[0].match_location == "name"

    # ネストした列の検索
    nested_results = _search_columns(
        columns=columns,
        keyword="child",
        project_id="test-project",
        dataset_id="test-dataset",
        table_id="test-table",
    )

    # 結果の検証
    assert len(nested_results) == 1
    assert nested_results[0].column_name == "child_col"
