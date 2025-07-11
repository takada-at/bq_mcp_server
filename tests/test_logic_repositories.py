"""Integration tests for repositories/logic.py - testing with error handling"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from bq_mcp.core.entities import (
    CachedData,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    Settings,
    TableMetadata,
    TableSchema,
)
from bq_mcp.repositories import logic


@pytest.fixture
def sample_cache():
    """Sample cache data for testing"""
    return CachedData(
        datasets={
            "project1": [
                DatasetMetadata(
                    project_id="project1",
                    dataset_id="dataset1",
                    location="US",
                )
            ]
        },
        tables={
            "project1": {
                "dataset1": [
                    TableMetadata(
                        project_id="project1",
                        dataset_id="dataset1",
                        table_id="table1",
                        full_table_id="project1.dataset1.table1",
                        table_type="TABLE",
                        schema=TableSchema(columns=[]),
                    )
                ]
            }
        },
        last_updated=datetime.now(),
    )


@pytest.fixture
def mock_settings():
    """Mock settings for testing"""
    return Settings(
        project_ids=["project1", "project2"],
        dataset_filters=[],
        cache_ttl_seconds=3600,
    )


class TestGetDatasets:
    """Test get_datasets function with error handling"""

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_datasets_success(self, mock_cache_manager, sample_cache):
        """Test successful dataset retrieval"""
        # Arrange
        mock_cache_manager.load_cache.return_value = sample_cache
        mock_cache_manager.is_cache_valid.return_value = True

        # Act
        result = await logic.get_datasets()

        # Assert
        assert len(result.datasets) == 1
        assert result.datasets[0].dataset_id == "dataset1"

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_datasets_cache_update_failure(self, mock_cache_manager):
        """Test handling of cache update failure"""
        # Arrange
        mock_cache_manager.load_cache.return_value = None
        mock_cache_manager.update_cache = AsyncMock(return_value=None)

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_datasets()
        assert exc.value.status_code == 503
        assert "Failed to retrieve cache data" in exc.value.detail

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    @patch("bq_mcp.repositories.logic._get_datasets_impl")
    async def test_get_datasets_generic_exception(
        self, mock_impl, mock_cache_manager, sample_cache
    ):
        """Test handling of generic exceptions"""
        # Arrange
        mock_cache_manager.load_cache.return_value = sample_cache
        mock_cache_manager.is_cache_valid.return_value = True
        mock_impl.side_effect = RuntimeError("Unexpected error")

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_datasets()
        assert exc.value.status_code == 503
        assert "Failed to retrieve dataset list" in exc.value.detail


class TestGetDatasetsByProject:
    """Test get_datasets_by_project function"""

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_datasets_by_project_success(
        self, mock_cache_manager, sample_cache
    ):
        """Test successful dataset retrieval for specific project"""
        # Arrange
        mock_cache_manager.load_cache.return_value = sample_cache
        mock_cache_manager.is_cache_valid.return_value = True

        # Act
        result = await logic.get_datasets_by_project("project1")

        # Assert
        assert len(result.datasets) == 1
        assert result.datasets[0].project_id == "project1"
        assert result.datasets[0].dataset_id == "dataset1"

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_datasets_by_project_not_found(
        self, mock_cache_manager, sample_cache
    ):
        """Test project not found error"""
        # Arrange
        mock_cache_manager.load_cache.return_value = sample_cache
        mock_cache_manager.is_cache_valid.return_value = True

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_datasets_by_project("nonexistent")
        assert exc.value.status_code == 404
        assert "Project 'nonexistent' not found" in exc.value.detail

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_datasets_by_project_http_exception_propagates(
        self, mock_cache_manager
    ):
        """Test that HTTPException from cache loading propagates"""
        # Arrange
        expected_exception = HTTPException(status_code=503, detail="Cache unavailable")
        mock_cache_manager.load_cache.side_effect = expected_exception

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_datasets_by_project("project1")
        assert exc.value.status_code == 503
        assert exc.value.detail == "Cache unavailable"


class TestGetTables:
    """Test get_tables function with error handling"""

    @pytest.mark.asyncio
    async def test_get_tables_with_project_success(self, sample_cache, mock_settings):
        """Test successful table retrieval with project ID"""
        # Arrange
        with patch("bq_mcp.repositories.logic.config") as mock_config:
            mock_config.get_settings.return_value = mock_settings

            # Mock the entire _get_tables_impl function
            with patch(
                "bq_mcp.repositories.logic._get_tables_impl", new_callable=AsyncMock
            ) as mock_get_tables:
                mock_get_tables.return_value = sample_cache.tables["project1"][
                    "dataset1"
                ]

                # Act
                result = await logic.get_tables("dataset1", "project1")

                # Assert
                assert len(result) == 1
                assert result[0].table_id == "table1"
                mock_get_tables.assert_called_once_with("dataset1", "project1")

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_tables_dataset_not_found(self, mock_cache_manager):
        """Test dataset not found error"""
        # Arrange
        mock_cache_manager.get_cached_dataset_data = AsyncMock(return_value=(None, []))

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_tables("nonexistent", "project1")
        assert exc.value.status_code == 404
        assert "Dataset 'project1.nonexistent' not found" in exc.value.detail

    @pytest.mark.asyncio
    async def test_get_tables_no_project_success(self, sample_cache, mock_settings):
        """Test successful table retrieval without project ID specified"""
        # Arrange
        with patch("bq_mcp.repositories.logic.config") as mock_config:
            mock_config.get_settings.return_value = mock_settings

            with patch("bq_mcp.repositories.logic.cache_manager") as mock_cache_manager:
                mock_cache_manager.load_cache.return_value = sample_cache
                mock_cache_manager.is_cache_valid.return_value = True

                # Mock the entire _get_tables_impl function
                with patch(
                    "bq_mcp.repositories.logic._get_tables_impl"
                ) as mock_get_tables:
                    mock_get_tables.return_value = sample_cache.tables["project1"][
                        "dataset1"
                    ]

                    # Act
                    result = await logic.get_tables("dataset1")

                    # Assert
                    assert len(result) == 1
                    assert result[0].table_id == "table1"
                    mock_get_tables.assert_called_once_with("dataset1", None)

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    @patch("bq_mcp.repositories.logic.config")
    async def test_get_tables_no_project_not_found(
        self, mock_config, mock_cache_manager, sample_cache, mock_settings
    ):
        """Test dataset not found when searching all projects"""
        # Arrange
        mock_config.get_settings.return_value = mock_settings
        mock_cache_manager.load_cache.return_value = sample_cache
        mock_cache_manager.is_cache_valid.return_value = True
        mock_cache_manager.get_cached_dataset_data = AsyncMock(return_value=(None, []))

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_tables("nonexistent")
        assert exc.value.status_code == 404
        assert "Dataset 'nonexistent' not found" in exc.value.detail

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    @patch("bq_mcp.repositories.logic.config")
    async def test_get_tables_no_project_http_exception_propagates(
        self, mock_config, mock_cache_manager, mock_settings
    ):
        """Test that HTTPException from cache loading propagates"""
        # Arrange
        mock_config.get_settings.return_value = mock_settings
        expected_exception = HTTPException(
            status_code=503, detail="Cache service unavailable"
        )
        mock_cache_manager.load_cache.side_effect = expected_exception

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_tables("dataset1")
        assert exc.value.status_code == 503
        assert exc.value.detail == "Cache service unavailable"


class TestQueryFunctions:
    """Test query-related functions"""

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.config")
    @patch("bq_mcp.repositories.logic.QueryExecutor")
    async def test_check_query_scan_amount_success(
        self, mock_executor_class, mock_config, mock_settings
    ):
        """Test successful query scan amount check"""
        # Arrange
        mock_config.get_settings.return_value = mock_settings
        mock_executor = AsyncMock()
        mock_executor_class.return_value = mock_executor

        expected_result = QueryDryRunResult(
            total_bytes_processed=1024,
            total_bytes_billed=1024,
            is_safe=True,
            modified_sql="SELECT 1",
        )
        mock_executor.check_scan_amount = AsyncMock(return_value=expected_result)

        # Act
        result = await logic.check_query_scan_amount("SELECT 1")

        # Assert
        assert result == expected_result
        mock_executor.check_scan_amount.assert_called_once_with("SELECT 1", None)

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.config")
    @patch("bq_mcp.repositories.logic.QueryExecutor")
    async def test_execute_query_success(
        self, mock_executor_class, mock_config, mock_settings
    ):
        """Test successful query execution"""
        # Arrange
        mock_config.get_settings.return_value = mock_settings
        mock_executor = AsyncMock()
        mock_executor_class.return_value = mock_executor

        expected_result = QueryExecutionResult(
            success=True,
            rows=[{"col": "value"}],
            total_rows=1,
            schema=[],
        )
        mock_executor.execute_query = AsyncMock(return_value=expected_result)

        # Act
        result = await logic.execute_query("SELECT 1", force=True)

        # Assert
        assert result == expected_result
        mock_executor.execute_query.assert_called_once_with(
            "SELECT 1", None, force_execute=True
        )


class TestCacheManagement:
    """Test cache management functions"""

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_current_cache_valid(self, mock_cache_manager, sample_cache):
        """Test getting valid cache"""
        # Arrange
        mock_cache_manager.load_cache.return_value = sample_cache
        mock_cache_manager.is_cache_valid.return_value = True

        # Act
        result = await logic.get_current_cache()

        # Assert
        assert result == sample_cache
        mock_cache_manager.update_cache.assert_not_called()

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    @patch("bq_mcp.repositories.logic.asyncio")
    async def test_get_current_cache_expired(
        self, mock_asyncio, mock_cache_manager, sample_cache
    ):
        """Test handling of expired cache with background update"""
        # Arrange
        mock_cache_manager.load_cache.return_value = sample_cache
        mock_cache_manager.is_cache_valid.return_value = False
        mock_asyncio.create_task = MagicMock()

        # Act
        result = await logic.get_current_cache()

        # Assert
        assert result == sample_cache
        mock_asyncio.create_task.assert_called_once()

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_current_cache_initial_load(
        self, mock_cache_manager, sample_cache
    ):
        """Test initial cache load when no cache exists"""
        # Arrange
        mock_cache_manager.load_cache.return_value = None
        mock_cache_manager.update_cache = AsyncMock(return_value=sample_cache)

        # Act
        result = await logic.get_current_cache()

        # Assert
        assert result == sample_cache
        mock_cache_manager.update_cache.assert_called_once()

    @pytest.mark.asyncio
    @patch("bq_mcp.repositories.logic.cache_manager")
    async def test_get_current_cache_update_fails(self, mock_cache_manager):
        """Test cache update failure handling"""
        # Arrange
        mock_cache_manager.load_cache.return_value = None
        mock_cache_manager.update_cache = AsyncMock(return_value=None)

        # Act & Assert
        with pytest.raises(HTTPException) as exc:
            await logic.get_current_cache()
        assert exc.value.status_code == 503
        assert "Failed to retrieve cache data" in exc.value.detail
