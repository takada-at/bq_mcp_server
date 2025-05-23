import pytest
import traceback  # Added import
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import HTTPException

# Import the functions to be tested
from bq_meta_api.core import logic
from bq_meta_api.core.entities import (
    CachedData,
    DatasetMetadata,
    TableMetadata,
    DatasetListResponse,
)


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.cache_manager")
async def test_get_current_cache_valid_direct(mock_cache_manager, mock_get_logger):
    """Tests get_current_cache when the cache is valid and loaded directly."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    expected_cache = CachedData(datasets={}, tables={})
    mock_cache_manager.load_cache.return_value = expected_cache
    mock_cache_manager.is_cache_valid.return_value = True

    # Call function
    actual_cache = await logic.get_current_cache()

    # Assertions
    assert actual_cache == expected_cache
    mock_cache_manager.load_cache.assert_called_once()
    mock_cache_manager.is_cache_valid.assert_called_once_with(expected_cache)
    mock_cache_manager.update_cache.assert_not_called()
    # Removed log assertions as they are not present in this path in logic.py


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.cache_manager")
async def test_get_current_cache_loaded_after_none(mock_cache_manager, mock_get_logger):
    """Tests get_current_cache when cache is initially None and reloaded."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Corrected DatasetMetadata instantiation
    dataset_meta = DatasetMetadata(project_id="p1", dataset_id="d1", location="l1")
    updated_cache = CachedData(
        datasets={"p1": [dataset_meta]}, tables={}
    )  # Assuming datasets is Dict[str, List[DatasetMetadata]]
    mock_cache_manager.load_cache.return_value = None
    mock_cache_manager.update_cache.return_value = updated_cache

    # Call function
    actual_cache = await logic.get_current_cache()

    # Assertions
    assert actual_cache == updated_cache
    mock_cache_manager.load_cache.assert_called_once()
    mock_cache_manager.is_cache_valid.assert_not_called()
    mock_cache_manager.update_cache.assert_called_once()
    mock_logger.info.assert_any_call(
        "キャッシュが無効または存在しないため、更新を試みます..."
    )  # Updated to Japanese
    # Removed: mock_logger.warning.assert_any_call("Cache file not found. Triggering update.") (not logged by logic.py)
    # Removed: mock_logger.info.assert_any_call(f"Cache updated successfully: {updated_cache}") (no such log in logic.py for this path)


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.cache_manager")
async def test_get_current_cache_loaded_after_invalid(
    mock_cache_manager, mock_get_logger
):
    """Tests get_current_cache when existing cache is invalid and reloaded."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    initial_cache = CachedData(datasets={}, tables={})
    # Corrected DatasetMetadata instantiation
    dataset_meta = DatasetMetadata(project_id="p1", dataset_id="d1", location="l1")
    updated_cache = CachedData(
        datasets={"p1": [dataset_meta]}, tables={}
    )  # Assuming datasets is Dict[str, List[DatasetMetadata]]
    mock_cache_manager.load_cache.return_value = initial_cache
    mock_cache_manager.is_cache_valid.return_value = False
    mock_cache_manager.update_cache.return_value = updated_cache

    # Call function
    actual_cache = await logic.get_current_cache()

    # Assertions
    assert actual_cache == updated_cache
    mock_cache_manager.load_cache.assert_called_once()
    mock_cache_manager.is_cache_valid.assert_called_once_with(initial_cache)
    mock_cache_manager.update_cache.assert_called_once()
    mock_logger.info.assert_any_call(
        "キャッシュが無効または存在しないため、更新を試みます..."
    )  # Updated to Japanese
    # Removed: mock_logger.warning.assert_any_call(f"Cache is invalid or expired: {initial_cache}. Triggering update.") (not logged by logic.py)
    # Removed: mock_logger.info.assert_any_call(f"Cache updated successfully: {updated_cache}") (no such log in logic.py for this path)


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.cache_manager")
async def test_get_current_cache_update_fails(mock_cache_manager, mock_get_logger):
    """Tests get_current_cache when cache update fails, expecting HTTPException 503."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    mock_cache_manager.load_cache.return_value = None
    mock_cache_manager.update_cache.return_value = None

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_current_cache()

    # Assertions
    assert exc_info.value.status_code == 503
    assert (
        exc_info.value.detail
        == "キャッシュデータの取得に失敗しました。サーバーが利用できません。"
    )  # Updated to Japanese
    mock_cache_manager.load_cache.assert_called_once()
    mock_cache_manager.update_cache.assert_called_once()
    mock_logger.info.assert_any_call(
        "キャッシュが無効または存在しないため、更新を試みます..."
    )  # Updated to Japanese
    # Removed: mock_logger.warning.assert_any_call("Cache file not found. Triggering update.") (not logged by logic.py)
    mock_logger.error.assert_any_call(
        "キャッシュの更新に失敗しました。"
    )  # Updated to Japanese


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_successful_retrieval(
    mock_get_current_cache, mock_get_logger
):
    """Tests get_datasets for successful retrieval of all dataset metadata."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Corrected DatasetMetadata instantiation
    datasets_proj1 = [DatasetMetadata(project_id="p1", dataset_id="d1", location="l1")]
    datasets_proj2 = [DatasetMetadata(project_id="p2", dataset_id="d2", location="l2")]
    mock_cache = CachedData(
        datasets={
            "p1": datasets_proj1,
            "p2": datasets_proj2,
        },  # Keys should be project_id
        tables={},
    )
    mock_get_current_cache.return_value = mock_cache

    # Call function
    response = await logic.get_datasets()

    # Assertions
    assert isinstance(response, DatasetListResponse)
    assert len(response.datasets) == 2
    assert datasets_proj1[0] in response.datasets
    assert datasets_proj2[0] in response.datasets
    mock_get_current_cache.assert_called_once()
    # Removed: mock_logger.info.assert_any_call("Successfully retrieved all datasets.")


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_no_datasets_in_cache(
    mock_get_current_cache, mock_get_logger
):
    """Tests get_datasets when cache is empty, expecting an empty dataset list."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    mock_cache = CachedData(datasets={}, tables={})
    mock_get_current_cache.return_value = mock_cache

    # Call function
    response = await logic.get_datasets()

    # Assertions
    assert isinstance(response, DatasetListResponse)
    assert len(response.datasets) == 0
    mock_get_current_cache.assert_called_once()
    # Removed: mock_logger.info.assert_any_call("Successfully retrieved all datasets.")


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_http_exception_propagates(
    mock_get_current_cache, mock_get_logger
):
    """Tests that HTTPException from get_current_cache propagates through get_datasets."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    expected_exception = HTTPException(status_code=503, detail="Cache unavailable")
    mock_get_current_cache.side_effect = expected_exception

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_datasets()

    # Assertions
    assert exc_info.value.status_code == expected_exception.status_code
    assert exc_info.value.detail == expected_exception.detail
    mock_get_current_cache.assert_called_once()


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_generic_exception_triggers_http_exception(
    mock_get_current_cache, mock_get_logger
):
    """Tests that a generic exception in get_current_cache leads to HTTPException 503 in get_datasets."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    original_exception = ValueError("Something went wrong")
    mock_get_current_cache.side_effect = original_exception

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_datasets()

    # Assertions
    assert exc_info.value.status_code == 503
    assert (
        exc_info.value.detail
        == "データセット一覧の取得に失敗しました。サーバーが利用できません。"
    )  # Updated to Japanese
    mock_get_current_cache.assert_called_once()
    # Check for the two separate error log calls
    mock_logger.error.assert_any_call(
        f"データセット一覧の取得中にエラーが発生: {original_exception}"
    )  # Updated to Japanese
    mock_logger.error.assert_any_call(traceback.format_exception(original_exception))


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_by_project_successful_retrieval(
    mock_get_current_cache, mock_get_logger
):
    """Tests get_datasets_by_project for successful retrieval for a specific project."""
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Corrected DatasetMetadata instantiation
    datasets_proj1 = [DatasetMetadata(project_id="p1", dataset_id="d1", location="l1")]
    datasets_proj2 = [DatasetMetadata(project_id="p2", dataset_id="d2", location="l2")]
    mock_cache = CachedData(
        datasets={
            "p1": datasets_proj1,
            "p2": datasets_proj2,
        },  # Keys should be project_id
        tables={},
    )
    mock_get_current_cache.return_value = mock_cache
    project_id_to_test = "p1"

    # Call function
    response = await logic.get_datasets_by_project(project_id_to_test)

    # Assertions
    assert isinstance(response, DatasetListResponse)
    assert len(response.datasets) == 1
    assert response.datasets == datasets_proj1
    mock_get_current_cache.assert_called_once()
    # Removed: mock_logger.info.assert_any_call(f"Successfully retrieved datasets for project: {project_id_to_test}")


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_by_project_project_not_found(
    mock_get_current_cache, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    # Corrected DatasetMetadata instantiation
    datasets_proj1 = [DatasetMetadata(project_id="p1", dataset_id="d1", location="l1")]
    mock_cache = CachedData(
        datasets={"p1": datasets_proj1},  # Key should be project_id
        tables={},
    )
    mock_get_current_cache.return_value = mock_cache
    project_id_to_test = "non_existent_project"

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_datasets_by_project(project_id_to_test)

    # Assertions
    assert exc_info.value.status_code == 404
    assert (
        exc_info.value.detail
        == f"プロジェクト '{project_id_to_test}' は見つかりません。"
    )  # Updated to Japanese
    mock_get_current_cache.assert_called_once()
    # Removed: mock_logger.warning.assert_any_call(f"Project ID not found: {project_id_to_test}") (log not in logic.py)


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_by_project_http_exception_propagates(
    mock_get_current_cache, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()  # Not strictly necessary for this test, but good practice
    mock_get_logger.return_value = mock_logger
    expected_exception = HTTPException(status_code=503, detail="Cache unavailable")
    mock_get_current_cache.side_effect = expected_exception
    project_id_to_test = "any_project"

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_datasets_by_project(project_id_to_test)

    # Assertions
    assert exc_info.value.status_code == expected_exception.status_code
    assert exc_info.value.detail == expected_exception.detail
    mock_get_current_cache.assert_called_once()


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_datasets_by_project_generic_exception_triggers_http_exception(
    mock_get_current_cache, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    original_exception = RuntimeError("Unexpected error")
    mock_get_current_cache.side_effect = original_exception
    project_id_to_test = "any_project"

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_datasets_by_project(project_id_to_test)

    # Assertions
    assert exc_info.value.status_code == 503
    assert (
        exc_info.value.detail
        == f"プロジェクト '{project_id_to_test}' のデータセット一覧の取得に失敗しました。"
    )  # Updated to Japanese
    mock_get_current_cache.assert_called_once()
    mock_logger.error.assert_called_once_with(
        f"プロジェクト '{project_id_to_test}' のデータセット一覧の取得中にエラーが発生: {original_exception}"  # Updated to Japanese, removed exc_info=True
    )


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.cache_manager")
@patch(
    "bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock
)  # Not used in this path, but good to have for consistency
async def test_get_tables_project_id_provided_dataset_found(
    mock_get_current_cache, mock_cache_manager, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    dataset_id = "d1"
    project_id = "p1"

    # Corrected TableMetadata and DatasetMetadata instantiation
    expected_tables = [
        TableMetadata(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id="t1",
            full_table_id=f"{project_id}.{dataset_id}.t1",
        )
    ]
    dataset_meta = DatasetMetadata(
        project_id=project_id, dataset_id=dataset_id, location="EU"
    )
    # Note: DatasetMetadata in models.py does not have a 'tables' field directly.
    # It seems table lists are managed separately in CachedData.tables or fetched.
    # For this test, get_cached_dataset_data should return the dataset_meta and its associated tables.

    mock_cache_manager.get_cached_dataset_data.return_value = (
        dataset_meta,
        expected_tables,
    )

    # Call function
    actual_tables = await logic.get_tables(dataset_id, project_id)

    # Assertions
    assert actual_tables == expected_tables
    mock_cache_manager.get_cached_dataset_data.assert_called_once_with(
        project_id, dataset_id
    )  # Corrected argument order
    mock_get_current_cache.assert_not_called()
    # Removed: mock_logger.info.assert_called_once_with(f"Successfully retrieved tables for dataset: {dataset_id} in project: {project_id}")


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.cache_manager")
async def test_get_tables_project_id_provided_dataset_not_found(
    mock_cache_manager, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    dataset_id = "d1"
    project_id = "p1"

    mock_cache_manager.get_cached_dataset_data.return_value = (None, [])

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_tables(dataset_id, project_id)

    # Assertions
    assert exc_info.value.status_code == 404
    assert (
        exc_info.value.detail
        == f"データセット '{project_id}.{dataset_id}' は見つかりません。"
    )  # Updated to Japanese
    mock_cache_manager.get_cached_dataset_data.assert_called_once_with(
        project_id, dataset_id
    )  # Corrected argument order
    # Removed: mock_logger.warning.assert_called_once_with(f"Dataset not found: {dataset_id} in project {project_id}") (log not in logic.py)


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.config")
@patch("bq_meta_api.core.logic.cache_manager")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_tables_no_project_id_dataset_found(
    mock_get_current_cache, mock_cache_manager, mock_config, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    dataset_id_to_find = "d1"
    project_id_with_dataset = "p1"
    other_project_id = "p2"

    # Corrected TableMetadata and DatasetMetadata instantiation
    expected_tables = [
        TableMetadata(
            project_id=project_id_with_dataset,
            dataset_id=dataset_id_to_find,
            table_id="t1",
            full_table_id=f"{project_id_with_dataset}.{dataset_id_to_find}.t1",
        )
    ]
    dataset_meta = DatasetMetadata(
        project_id=project_id_with_dataset, dataset_id=dataset_id_to_find, location="EU"
    )

    mock_settings = MagicMock()
    mock_settings.project_ids = [project_id_with_dataset, other_project_id]
    mock_config.get_settings.return_value = mock_settings

    mock_cache_data = CachedData(
        datasets={  # project_id is the key
            project_id_with_dataset: [dataset_meta],
            other_project_id: [],
        },
        tables={  # project_id is the first key, dataset_id is the second
            project_id_with_dataset: {dataset_id_to_find: expected_tables},
            other_project_id: {"d_other": []},  # Assuming d_other is a dataset_id
        },
    )
    mock_get_current_cache.return_value = mock_cache_data

    # Define side_effect for get_cached_dataset_data, matching (proj_id, ds_id) call order
    def get_cached_dataset_data_side_effect(p_id, d_id):  # Corrected parameter order
        if p_id == project_id_with_dataset and d_id == dataset_id_to_find:
            return (dataset_meta, expected_tables)
        return (None, [])

    mock_cache_manager.get_cached_dataset_data.side_effect = (
        get_cached_dataset_data_side_effect
    )

    # Call function
    actual_tables = await logic.get_tables(dataset_id_to_find)

    # Assertions
    assert actual_tables == expected_tables
    mock_get_current_cache.assert_called_once()
    mock_config.get_settings.assert_called_once()
    # Assert that get_cached_dataset_data was called for the project that has the dataset_id in its tables dict
    mock_cache_manager.get_cached_dataset_data.assert_any_call(
        project_id_with_dataset, dataset_id_to_find
    )  # Corrected argument order
    # Removed: mock_logger.info.assert_any_call(f"Searching for dataset {dataset_id_to_find} in project {project_id_with_dataset}") (log not in logic.py)
    # Removed: mock_logger.info.assert_called_with(f"Successfully retrieved tables for dataset: {dataset_id_to_find} from project {project_id_with_dataset}") (log not in logic.py)


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.config")
@patch("bq_meta_api.core.logic.cache_manager")
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_tables_no_project_id_dataset_not_found(
    mock_get_current_cache, mock_cache_manager, mock_config, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    dataset_id_to_find = "d3"  # This dataset does not exist
    project_ids = ["p1", "p2"]

    mock_settings = MagicMock()
    mock_settings.project_ids = project_ids
    mock_config.get_settings.return_value = mock_settings

    mock_cache_data = CachedData(
        datasets={},
        tables={
            project_ids[0]: {"d1": []},
            project_ids[1]: {"d2": []},
        },  # d3 is not here
    )
    mock_get_current_cache.return_value = mock_cache_data
    mock_cache_manager.get_cached_dataset_data.return_value = (
        None,
        [],
    )  # Should return not found for any call

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_tables(dataset_id_to_find)

    # Assertions
    assert exc_info.value.status_code == 404
    assert (
        exc_info.value.detail
        == f"データセット '{dataset_id_to_find}' は見つかりません。"
    )  # Updated to Japanese
    mock_get_current_cache.assert_called_once()
    mock_config.get_settings.assert_called_once()
    # Check get_cached_dataset_data calls for each project that might contain the dataset_id
    for proj_id in project_ids:
        if dataset_id_to_find in mock_cache_data.tables.get(
            proj_id, {}
        ):  # only if dataset_id could be in project
            mock_cache_manager.get_cached_dataset_data.assert_any_call(
                proj_id, dataset_id_to_find
            )  # Corrected argument order
        # Removed: mock_logger.info.assert_any_call(f"Searching for dataset {dataset_id_to_find} in project {proj_id}") (log not in logic.py)
    # Removed: mock_logger.warning.assert_called_once_with(f"Dataset not found: {dataset_id_to_find} in any configured project.") (log not in logic.py)


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.config")  # Mock config as it's used in the function
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_tables_no_project_id_get_current_cache_http_exception(
    mock_get_current_cache, mock_config, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    dataset_id = "any_dataset"
    expected_exception = HTTPException(
        status_code=503, detail="Cache service unavailable"
    )
    mock_get_current_cache.side_effect = expected_exception

    mock_settings = MagicMock()  # Settings are needed for the function path
    mock_settings.project_ids = ["p1"]
    mock_config.get_settings.return_value = mock_settings

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_tables(dataset_id)

    # Assertions
    assert exc_info.value.status_code == expected_exception.status_code
    assert exc_info.value.detail == expected_exception.detail
    mock_get_current_cache.assert_called_once()


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.config")  # Mock config as it's used in the function
@patch("bq_meta_api.core.logic.get_current_cache", new_callable=AsyncMock)
async def test_get_tables_no_project_id_get_current_cache_generic_exception(
    mock_get_current_cache, mock_config, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    dataset_id = "any_dataset"
    original_exception = ValueError("Something went wrong with cache loading")
    mock_get_current_cache.side_effect = original_exception

    mock_settings = MagicMock()  # Settings are needed for the function path
    mock_settings.project_ids = ["p1"]
    mock_config.get_settings.return_value = mock_settings

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_tables(dataset_id)

    # Assertions
    assert exc_info.value.status_code == 503
    assert (
        exc_info.value.detail
        == f"データセット '{dataset_id}' のテーブル一覧の取得に失敗しました。"
    )  # Updated to Japanese
    mock_get_current_cache.assert_called_once()
    mock_logger.error.assert_called_once_with(
        f"テーブル一覧の取得中にエラーが発生: {dataset_id}, {original_exception}"  # Updated to Japanese
        # Removed exc_info=True as it's not in the actual log call in logic.py
    )


@pytest.mark.asyncio
@patch("bq_meta_api.core.logic.log.get_logger")
@patch("bq_meta_api.core.logic.cache_manager")
async def test_get_tables_project_id_provided_generic_exception(
    mock_cache_manager, mock_get_logger
):
    # Setup mocks
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger
    dataset_id = "d1"
    project_id = "p1"
    original_exception = RuntimeError("Underlying storage issue")
    mock_cache_manager.get_cached_dataset_data.side_effect = original_exception

    # Call function and assert exception
    with pytest.raises(HTTPException) as exc_info:
        await logic.get_tables(dataset_id, project_id)

    # Assertions
    assert exc_info.value.status_code == 503
    assert (
        exc_info.value.detail
        == f"データセット '{project_id}.{dataset_id}' のテーブル一覧の取得に失敗しました。"
    )  # Updated to Japanese
    mock_cache_manager.get_cached_dataset_data.assert_called_once_with(
        project_id, dataset_id
    )  # Corrected argument order
    mock_logger.error.assert_called_once_with(
        f"テーブル一覧の取得中にエラーが発生: {project_id}.{dataset_id}, {original_exception}"  # Updated to Japanese
        # Removed exc_info=True as it's not in the actual log call in logic.py
    )
