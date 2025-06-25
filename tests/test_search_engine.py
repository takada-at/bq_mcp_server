import datetime
from unittest.mock import patch

import pytest

from bq_mcp.core.entities import (
    CachedData,
    ColumnSchema,
    DatasetMetadata,
    TableMetadata,
    TableSchema,
)
from bq_mcp.repositories.search_engine import _search_columns, search_metadata


@pytest.fixture
def test_timestamp():
    """Fixture that provides timestamp for testing"""
    return datetime.datetime.now(datetime.timezone.utc)


@pytest.fixture
def test_cached_data(test_timestamp):
    """Fixture that provides cache data for testing"""
    # Create column schema
    user_id_column = ColumnSchema(
        name="user_id",
        type="STRING",
        mode="REQUIRED",
        description="User unique identifier",
    )

    created_at_column = ColumnSchema(
        name="created_at",
        type="TIMESTAMP",
        mode="REQUIRED",
        description="Time when user was created",
    )

    address_column = ColumnSchema(
        name="address",
        type="RECORD",
        mode="NULLABLE",
        description="User address information",
        fields=[
            ColumnSchema(
                name="postal_code",
                type="STRING",
                mode="NULLABLE",
                description="Postal code",
            ),
            ColumnSchema(
                name="prefecture",
                type="STRING",
                mode="NULLABLE",
                description="Prefecture",
            ),
        ],
    )

    # Create table schema
    user_schema = TableSchema(
        columns=[user_id_column, created_at_column, address_column]
    )

    product_id_column = ColumnSchema(
        name="product_id",
        type="STRING",
        mode="REQUIRED",
        description="Product unique identifier",
    )

    product_name_column = ColumnSchema(
        name="product_name", type="STRING", mode="REQUIRED", description="Product name"
    )

    # Create table schema
    product_schema = TableSchema(columns=[product_id_column, product_name_column])

    # Create dataset metadata
    user_dataset = DatasetMetadata(
        project_id="test-project",
        dataset_id="user_data",
        description="User-related dataset",
    )

    product_dataset = DatasetMetadata(
        project_id="test-project",
        dataset_id="product_data",
        description="Product-related dataset",
    )

    # Create table metadata
    users_table = TableMetadata(
        project_id="test-project",
        dataset_id="user_data",
        table_id="users",
        full_table_id="test-project.user_data.users",
        schema_=user_schema,
        description="User information table",
        num_rows=1000,
        created_time=test_timestamp,
    )

    products_table = TableMetadata(
        project_id="test-project",
        dataset_id="product_data",
        table_id="products",
        full_table_id="test-project.product_data.products",
        schema_=product_schema,
        description="Product information table",
        num_rows=500,
        created_time=test_timestamp,
    )

    # Create cache data
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
    """Returns empty list when there is no cache data"""
    # Mock state with no cache data
    mock_get_cached_data.return_value = None

    # Execute search
    results = await search_metadata("user")

    # Verify results
    assert len(results) == 0
    assert isinstance(results, list)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_dataset_name(mock_get_cached_data, test_cached_data):
    """Test case for keyword matching in dataset name"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search
    results = await search_metadata("user")

    # Verify results - check if dataset name match is included
    dataset_results = [
        r for r in results if r.type == "dataset" and r.match_location == "name"
    ]
    assert any(r.dataset_id == "user_data" for r in dataset_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_dataset_description(
    mock_get_cached_data, test_cached_data
):
    """Test case for keyword matching in dataset description"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search
    results = await search_metadata("User-related")

    # Verify results - check if dataset description match is included
    dataset_results = [
        r for r in results if r.type == "dataset" and r.match_location == "description"
    ]
    assert any(r.dataset_id == "user_data" for r in dataset_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_table_name(mock_get_cached_data, test_cached_data):
    """Test case for keyword matching in table name"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search
    results = await search_metadata("user")

    # Verify results - check if table name match is included
    table_results = [
        r for r in results if r.type == "table" and r.match_location == "name"
    ]
    assert any(r.table_id == "users" for r in table_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_table_description(
    mock_get_cached_data, test_cached_data
):
    """Test case for keyword matching in table description"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search
    results = await search_metadata("User information")

    # Verify results - check if table description match is included
    table_results = [
        r for r in results if r.type == "table" and r.match_location == "description"
    ]
    assert any(r.table_id == "users" for r in table_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_column_name(mock_get_cached_data, test_cached_data):
    """Test case for keyword matching in column name"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search
    results = await search_metadata("user_id")

    # Verify results - check if column name match is included
    column_results = [
        r for r in results if r.type == "column" and r.match_location == "name"
    ]
    assert any(r.column_name == "user_id" for r in column_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_column_description(
    mock_get_cached_data, test_cached_data
):
    """Test case for keyword matching in column description"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search
    results = await search_metadata("unique identifier")

    # Verify results - check if column description match is included
    column_results = [
        r for r in results if r.type == "column" and r.match_location == "description"
    ]
    assert any(r.column_name == "user_id" for r in column_results)
    assert any(r.column_name == "product_id" for r in column_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_nested_column(mock_get_cached_data, test_cached_data):
    """Test case for keyword matching in nested columns"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search
    results = await search_metadata("postal_code")

    # Verify results - check if nested column name match is included
    column_results = [
        r for r in results if r.type == "column" and r.match_location == "name"
    ]
    assert any(r.column_name == "postal_code" for r in column_results)


@pytest.mark.asyncio
@patch("bq_mcp.repositories.cache_manager.get_cached_data")
async def test_search_metadata_case_insensitive(mock_get_cached_data, test_cached_data):
    """Test to confirm that search is case insensitive"""
    # Mock cache data
    mock_get_cached_data.return_value = test_cached_data

    # Execute search with uppercase
    results = await search_metadata("USER")

    # Verify results - should match regardless of case
    assert any(r.dataset_id == "user_data" for r in results if r.type == "dataset")
    assert any(r.table_id == "users" for r in results if r.type == "table")


def test_search_columns():
    """Unit test for _search_columns function"""
    # Create test column list
    columns = [
        ColumnSchema(
            name="test_column",
            type="STRING",
            mode="REQUIRED",
            description="Test column",
        ),
        ColumnSchema(
            name="nested_col",
            type="RECORD",
            mode="NULLABLE",
            description="Nested column",
            fields=[
                ColumnSchema(
                    name="child_col",
                    type="STRING",
                    mode="NULLABLE",
                    description="Child column",
                )
            ],
        ),
    ]

    # Execute search
    results = _search_columns(
        columns=columns,
        keyword="test",
        project_id="test-project",
        dataset_id="test-dataset",
        table_id="test-table",
    )

    # Verify results
    assert len(results) == 2  # Both name and description matches
    assert results[0].type == "column"
    assert results[0].column_name == "test_column"
    assert results[0].match_location == "name"

    # Search for nested columns
    nested_results = _search_columns(
        columns=columns,
        keyword="child",
        project_id="test-project",
        dataset_id="test-dataset",
        table_id="test-table",
    )

    # Verify results
    assert len(nested_results) == 2  # Both name and description matches
    assert nested_results[0].column_name == "child_col"
