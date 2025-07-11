"""Unit tests for core/logic_base.py - pure business logic tests"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from bq_mcp.core import logic_base
from bq_mcp.core.entities import (
    CachedData,
    ColumnSchema,
    DatasetListResponse,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    TableMetadata,
    TableSchema,
)


@pytest.fixture
def sample_datasets():
    """Sample dataset metadata for testing"""
    return [
        DatasetMetadata(
            project_id="project1",
            dataset_id="dataset1",
            location="US",
            description="Test dataset 1",
        ),
        DatasetMetadata(
            project_id="project1",
            dataset_id="dataset2",
            location="US",
            description="Test dataset 2",
        ),
    ]


@pytest.fixture
def sample_tables():
    """Sample table metadata for testing"""
    return [
        TableMetadata(
            project_id="project1",
            dataset_id="dataset1",
            table_id="table1",
            full_table_id="project1.dataset1.table1",
            table_type="TABLE",
            schema=TableSchema(
                columns=[
                    ColumnSchema(
                        name="col1",
                        type="STRING",
                        mode="NULLABLE",
                        description="Column 1",
                    )
                ]
            ),
        ),
        TableMetadata(
            project_id="project1",
            dataset_id="dataset1",
            table_id="table2",
            full_table_id="project1.dataset1.table2",
            table_type="TABLE",
            schema=TableSchema(columns=[]),
        ),
    ]


@pytest.fixture
def sample_cache(sample_datasets, sample_tables):
    """Sample cache data for testing"""
    return CachedData(
        datasets={
            "project1": sample_datasets,
            "project2": [
                DatasetMetadata(
                    project_id="project2",
                    dataset_id="dataset3",
                    location="EU",
                )
            ],
        },
        tables={
            "project1": {"dataset1": sample_tables, "dataset2": []},
            "project2": {"dataset3": []},
        },
        last_updated=datetime.now(),
    )


class TestGetDatasets:
    """Test create_get_datasets function"""

    @pytest.mark.asyncio
    async def test_get_datasets_returns_all_datasets(self, sample_cache):
        """Test that get_datasets returns all datasets from cache"""
        # Arrange
        mock_get_cache = AsyncMock(return_value=sample_cache)
        get_datasets = logic_base.create_get_datasets(mock_get_cache)

        # Act
        result = await get_datasets()

        # Assert
        assert isinstance(result, DatasetListResponse)
        assert len(result.datasets) == 3
        assert result.datasets[0].project_id == "project1"
        assert result.datasets[2].project_id == "project2"
        mock_get_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_datasets_empty_cache(self):
        """Test get_datasets with empty cache"""
        # Arrange
        empty_cache = CachedData(datasets={}, tables={}, last_updated=datetime.now())
        mock_get_cache = AsyncMock(return_value=empty_cache)
        get_datasets = logic_base.create_get_datasets(mock_get_cache)

        # Act
        result = await get_datasets()

        # Assert
        assert isinstance(result, DatasetListResponse)
        assert len(result.datasets) == 0


class TestGetDatasetsByProject:
    """Test create_get_datasets_by_project function"""

    @pytest.mark.asyncio
    async def test_get_datasets_by_existing_project(self, sample_cache):
        """Test getting datasets for existing project"""
        # Arrange
        mock_get_cache = AsyncMock(return_value=sample_cache)
        get_datasets_by_project = logic_base.create_get_datasets_by_project(
            mock_get_cache
        )

        # Act
        result = await get_datasets_by_project("project1")

        # Assert
        assert isinstance(result, DatasetListResponse)
        assert len(result.datasets) == 2
        assert all(d.project_id == "project1" for d in result.datasets)

    @pytest.mark.asyncio
    async def test_get_datasets_by_nonexistent_project(self, sample_cache):
        """Test getting datasets for non-existent project"""
        # Arrange
        mock_get_cache = AsyncMock(return_value=sample_cache)
        get_datasets_by_project = logic_base.create_get_datasets_by_project(
            mock_get_cache
        )

        # Act
        result = await get_datasets_by_project("nonexistent")

        # Assert
        assert isinstance(result, DatasetListResponse)
        assert len(result.datasets) == 0


class TestGetTables:
    """Test create_get_tables function"""

    @pytest.mark.asyncio
    async def test_get_tables_with_project_id(self, sample_cache, sample_tables):
        """Test getting tables with specific project ID"""
        # Arrange
        mock_get_cached_data = AsyncMock(
            return_value=(sample_cache.datasets["project1"][0], sample_tables)
        )
        mock_get_cache = AsyncMock(return_value=sample_cache)

        def mock_get_project_ids():
            return ["project1", "project2"]

        get_tables = logic_base.create_get_tables(
            mock_get_cached_data, mock_get_cache, mock_get_project_ids
        )

        # Act
        result = await get_tables("dataset1", "project1")

        # Assert
        assert len(result) == 2
        assert result[0].table_id == "table1"
        mock_get_cached_data.assert_called_once_with("project1", "dataset1")
        mock_get_cache.assert_not_called()  # Should not need cache when project specified

    @pytest.mark.asyncio
    async def test_get_tables_without_project_id(self, sample_cache, sample_tables):
        """Test getting tables without project ID (search all projects)"""
        # Arrange
        mock_get_cached_data = AsyncMock(
            return_value=(sample_cache.datasets["project1"][0], sample_tables)
        )
        mock_get_cache = AsyncMock(return_value=sample_cache)

        def mock_get_project_ids():
            return ["project1", "project2"]

        get_tables = logic_base.create_get_tables(
            mock_get_cached_data, mock_get_cache, mock_get_project_ids
        )

        # Act
        result = await get_tables("dataset1")

        # Assert
        assert len(result) == 2
        mock_get_cache.assert_called_once()
        mock_get_cached_data.assert_called_once_with("project1", "dataset1")

    @pytest.mark.asyncio
    async def test_get_tables_dataset_not_found(self, sample_cache):
        """Test getting tables for non-existent dataset"""
        # Arrange
        mock_get_cached_data = AsyncMock(return_value=(None, []))
        mock_get_cache = AsyncMock(return_value=sample_cache)

        def mock_get_project_ids():
            return ["project1", "project2"]

        get_tables = logic_base.create_get_tables(
            mock_get_cached_data, mock_get_cache, mock_get_project_ids
        )

        # Act
        result = await get_tables("nonexistent", "project1")

        # Assert
        assert len(result) == 0


class TestCheckQueryScanAmount:
    """Test create_check_query_scan_amount function"""

    @pytest.mark.asyncio
    async def test_check_query_scan_amount_success(self):
        """Test successful query scan amount check"""
        # Arrange
        expected_result = QueryDryRunResult(
            total_bytes_processed=1024 * 1024,  # 1 MB
            total_bytes_billed=1024 * 1024,
            is_safe=True,
            modified_sql="SELECT * FROM table",
        )
        mock_check_scan = AsyncMock(return_value=expected_result)
        mock_logger = MagicMock()

        check_query_scan_amount = logic_base.create_check_query_scan_amount(
            mock_check_scan, mock_logger
        )

        # Act
        result = await check_query_scan_amount("SELECT * FROM table", "project1")

        # Assert
        assert result == expected_result
        mock_check_scan.assert_called_once_with("SELECT * FROM table", "project1")
        mock_logger.assert_called_once_with(
            "Scan amount check completed: 1,048,576 bytes"
        )


class TestExecuteQuery:
    """Test create_execute_query function"""

    @pytest.mark.asyncio
    async def test_execute_query_success(self):
        """Test successful query execution"""
        # Arrange
        expected_result = QueryExecutionResult(
            success=True,
            rows=[{"col1": "value1"}],
            total_rows=1,
            schema=[{"name": "col1", "type": "STRING"}],
            total_bytes_processed=1024,
            total_bytes_billed=1024,
        )
        mock_execute = AsyncMock(return_value=expected_result)
        mock_logger = MagicMock()

        execute_query = logic_base.create_execute_query(mock_execute, mock_logger)

        # Act
        result = await execute_query("SELECT * FROM table", "project1")

        # Assert
        assert result == expected_result
        mock_execute.assert_called_once_with("SELECT * FROM table", "project1")
        mock_logger.assert_called_once_with(
            "Query execution successful - result rows: 1"
        )

    @pytest.mark.asyncio
    async def test_execute_query_failure(self):
        """Test failed query execution"""
        # Arrange
        expected_result = QueryExecutionResult(
            success=False,
            error_message="Query failed",
            rows=[],
            total_rows=0,
            schema=[],
        )
        mock_execute = AsyncMock(return_value=expected_result)
        mock_logger = MagicMock()

        execute_query = logic_base.create_execute_query(mock_execute, mock_logger)

        # Act
        result = await execute_query("INVALID SQL", "project1")

        # Assert
        assert result == expected_result
        mock_logger.assert_called_once_with("Query execution failed: Query failed")
