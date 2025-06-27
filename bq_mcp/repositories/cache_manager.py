# cache_manager.py: Manages local caching of BigQuery metadata
import asyncio
import datetime
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bq_mcp.core.entities import CachedData, DatasetMetadata, TableMetadata
from bq_mcp.repositories import bigquery_client, config, log
from bq_mcp.repositories.config import should_include_dataset

# In-memory cache (singleton-like retention)
_cache: Optional[CachedData] = None
_project_datasets_cache: Dict[
    str, Dict[str, datetime.datetime]
] = {}  # project_id -> {dataset_id -> last_updated}


def get_cache_file_path(project_id: str, dataset_id: str) -> Path:
    """
    Returns the path to the cache file corresponding to the specified project ID and dataset ID.

    Args:
        project_id: Project ID
        dataset_id: Dataset ID

    Returns:
        Path object to the cache file
    """
    setting = config.get_settings()
    return Path(setting.cache_file_base_dir) / project_id / f"{dataset_id}.json"


def _ensure_timezone_aware(dt: datetime.datetime) -> datetime.datetime:
    """Ensure datetime is timezone-aware, treating naive as UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt


def _is_cache_expired(last_updated: datetime.datetime, ttl_seconds: int) -> bool:
    """Check if cache timestamp is expired based on TTL."""
    ttl = datetime.timedelta(seconds=ttl_seconds)
    now = datetime.datetime.now(datetime.timezone.utc)
    last_updated_aware = _ensure_timezone_aware(last_updated)
    return (now - last_updated_aware) >= ttl


def _update_memory_cache(
    project_id: str, dataset_id: str, timestamp: datetime.datetime
) -> None:
    """Update the in-memory cache with timestamp."""
    if project_id not in _project_datasets_cache:
        _project_datasets_cache[project_id] = {}
    _project_datasets_cache[project_id][dataset_id] = timestamp


def load_cache_file(
    project_id: str, dataset_id: str, cache_file: Path
) -> Optional[CachedData]:
    logger = log.get_logger()
    settings = config.get_settings()

    with open(cache_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        last_updated = datetime.datetime.fromisoformat(data["last_updated"])
        last_updated = _ensure_timezone_aware(last_updated)

        # Check if cache is within valid period
        if _is_cache_expired(last_updated, settings.cache_ttl_seconds):
            logger.info(f"Cache expired: {project_id}.{dataset_id}")
            return None

        # Update memory cache
        _update_memory_cache(project_id, dataset_id, last_updated)

        # Add dataset information
        dataset_meta = DatasetMetadata.model_validate(data["dataset"])

        # Add table information
        tables = [TableMetadata.model_validate(table) for table in data["tables"]]
        return dataset_meta, tables


def load_cache() -> Optional[CachedData]:
    """
    Load cache files and return CachedData object.
    Returns None if file does not exist or is invalid.
    """
    global _cache, _project_datasets_cache
    settings = config.get_settings()
    cache_dir = Path(settings.cache_file_base_dir)
    logger = log.get_logger()

    # Return memory cache if valid
    if _cache and is_cache_valid(_cache):
        logger.debug("Using memory cache.")
        return _cache

    latest_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    # Load data from new cache structure
    if cache_dir.exists():
        logger.info(f"Loading from cache directory: {cache_dir}")

        all_datasets: Dict[str, List[DatasetMetadata]] = {}
        all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}
        latest_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

        # Scan project ID directories
        for project_dir in cache_dir.iterdir():
            if project_dir.is_dir():
                project_id = project_dir.name
                all_datasets[project_id] = []
                all_tables[project_id] = {}

                # Scan dataset cache files
                for cache_file in project_dir.glob("*.json"):
                    dataset_id = cache_file.stem
                    loaded_data = load_cache_file(project_id, dataset_id, cache_file)
                    if loaded_data:
                        dataset_meta, tables = loaded_data
                        all_datasets[project_id].append(dataset_meta)
                        all_tables[project_id][dataset_id] = tables
                        latest_updated = max(
                            latest_updated,
                            _project_datasets_cache[project_id][dataset_id],
                        )

        # If there is valid cache data
        if latest_updated > datetime.datetime.min.replace(tzinfo=datetime.timezone.utc):
            _cache = CachedData(
                datasets=all_datasets, tables=all_tables, last_updated=latest_updated
            )
            return _cache

    logger.info("No valid cache found")
    return None


def save_dataset_cache(
    project_id: str,
    dataset: DatasetMetadata,
    tables: List[TableMetadata],
    timestamp: Optional[datetime.datetime] = None,
):
    """
    Save dataset and its table information to cache files.

    Args:
        project_id: Project ID
        dataset: Dataset metadata
        tables: List of table metadata
        timestamp: Timestamp (current time if not specified)
    """
    logger = log.get_logger()
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc)

    timestamp = _ensure_timezone_aware(timestamp)
    cache_file = get_cache_file_path(project_id, dataset.dataset_id)

    # Create directory if it doesn't exist
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    # Create cache data
    cache_data = {
        "dataset": dataset.model_dump(mode="json"),
        "tables": [table.model_dump(mode="json") for table in tables],
        "last_updated": timestamp.isoformat(),
    }

    try:
        logger.info(f"Saving cache: {project_id}.{dataset.dataset_id}")
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)

        # Also update memory cache
        _update_memory_cache(project_id, dataset.dataset_id, timestamp)
    except Exception as e:
        logger.error(f"Error occurred while saving cache file: {cache_file}, {e}")


def save_cache(data: CachedData):
    """Save CachedData object to cache files."""
    global _cache
    logger = log.get_logger()
    try:
        logger.info("Saving cache.")

        # Save cache files for each project/dataset
        for project_id, datasets in data.datasets.items():
            for dataset in datasets:
                if (
                    project_id in data.tables
                    and dataset.dataset_id in data.tables[project_id]
                ):
                    tables = data.tables[project_id][dataset.dataset_id]
                    save_dataset_cache(project_id, dataset, tables, data.last_updated)

        _cache = data  # Update memory cache
        logger.info("Cache save completed.")
    except Exception as e:
        logger.error(f"Error occurred while saving cache files: {e}")


def is_cache_valid(cached_data: Optional[CachedData]) -> bool:
    """Check if cache data is within valid period."""
    if not cached_data or not cached_data.last_updated:
        return False

    logger = log.get_logger()
    settings = config.get_settings()

    is_valid = not _is_cache_expired(
        cached_data.last_updated, settings.cache_ttl_seconds
    )
    last_updated_aware = _ensure_timezone_aware(cached_data.last_updated)

    logger.debug(
        f"Cache validity check: LastUpdated={last_updated_aware}, Valid={is_valid}"
    )
    return is_valid


def is_dataset_cache_valid(project_id: str, dataset_id: str) -> bool:
    """
    Check if cache for specific dataset is valid.

    Args:
        project_id: Project ID
        dataset_id: Dataset ID

    Returns:
        True if cache is valid, False otherwise
    """
    logger = log.get_logger()
    settings = config.get_settings()

    # Check memory cache
    if (
        project_id in _project_datasets_cache
        and dataset_id in _project_datasets_cache[project_id]
    ):
        last_updated = _project_datasets_cache[project_id][dataset_id]
        return not _is_cache_expired(last_updated, settings.cache_ttl_seconds)

    # Check file
    cache_file = get_cache_file_path(project_id, dataset_id)
    if not cache_file.exists():
        return False

    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            last_updated = datetime.datetime.fromisoformat(data["last_updated"])
            last_updated = _ensure_timezone_aware(last_updated)

            is_valid = not _is_cache_expired(last_updated, settings.cache_ttl_seconds)

            # Update memory cache if valid
            if is_valid:
                _update_memory_cache(project_id, dataset_id, last_updated)

            return is_valid
    except Exception as e:
        logger.error(f"Error during cache validity check: {cache_file}, {e}")
        return False


async def update_cache() -> Optional[CachedData]:
    """
    Asynchronously retrieve latest metadata from BigQuery and create new cache data.
    Returns None if retrieval fails.
    """
    global _cache
    logger = log.get_logger()
    logger.info("Starting asynchronous cache update...")
    settings = config.get_settings()

    bq_client = bigquery_client.get_bigquery_client()
    if not bq_client:
        logger.error("Could not obtain BigQuery client and session for cache update.")
        return None

    if not settings.project_ids:
        logger.warning("No project IDs configured for cache update.")
        return CachedData(last_updated=datetime.datetime.now(datetime.timezone.utc))
    all_datasets: Dict[str, List[DatasetMetadata]] = {}
    all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    try:
        tasks = []
        for project_id in settings.project_ids:
            logger.info(
                f"Asynchronously retrieving metadata for project '{project_id}'..."
            )
            task = asyncio.create_task(
                update_cache_project(bq_client, project_id, logger, timestamp)
            )
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        for project_id, datasets, project_tables in results:
            all_datasets[project_id] = datasets
            for project_id, dataset_id in project_tables.keys():
                if project_id not in all_tables:
                    all_tables[project_id] = {}
                if dataset_id not in all_tables[project_id]:
                    all_tables[project_id][dataset_id] = project_tables[
                        project_id, dataset_id
                    ]
            logger.info(f"Metadata retrieval for project '{project_id}' completed.")

        new_cache_data = CachedData(
            datasets=all_datasets,
            tables=all_tables,
            last_updated=timestamp,
        )
        logger.info("Asynchronous cache update completed.")
        _cache = new_cache_data  # Update memory cache
    finally:
        logger.debug("Closing aiohttp.ClientSession in update_cache.")
        await bigquery_client.close_client(bq_client)

    return new_cache_data


async def fetch_and_save_dataset(project_id: str, dataset: Dict, bq_client, timestamp):
    tables = await bigquery_client.fetch_tables_and_schemas(
        bq_client, project_id, dataset.dataset_id
    )
    save_dataset_cache(project_id, dataset, tables, timestamp)
    return dataset, tables


async def update_cache_project(
    bq_client, project_id: str, logger, timestamp
) -> Tuple[str, List[DatasetMetadata], Dict[str, List[TableMetadata]]]:
    all_datasets = await bigquery_client.fetch_datasets(bq_client, project_id)

    # Apply dataset filters
    settings = config.get_settings()
    if settings.dataset_filters:
        filtered_datasets = []
        for dataset in all_datasets:
            if should_include_dataset(
                project_id, dataset.dataset_id, settings.dataset_filters
            ):
                filtered_datasets.append(dataset)
                logger.debug(
                    f"Dataset {project_id}.{dataset.dataset_id} matched filter conditions"
                )
            else:
                logger.debug(
                    f"Dataset {project_id}.{dataset.dataset_id} was excluded by filter conditions"
                )
        datasets = filtered_datasets
        logger.info(
            f"Retrieved {len(all_datasets)} datasets from project '{project_id}', {len(datasets)} datasets targeted after filtering."
        )
    else:
        datasets = all_datasets
        logger.info(
            f"Retrieved {len(datasets)} dataset information from project '{project_id}'."
        )

    project_tables = {}
    tasks = [
        asyncio.create_task(
            fetch_and_save_dataset(project_id, dataset, bq_client, timestamp)
        )
        for dataset in datasets
    ]
    results = await asyncio.gather(*tasks)
    for dataset, tables in results:
        project_tables[project_id, dataset.dataset_id] = tables
    return project_id, datasets, project_tables


async def update_dataset_cache(project_id: str, dataset_id: str) -> bool:
    """
    Asynchronously updates the cache for a specific dataset.

    Args:
        project_id: Project ID
        dataset_id: Dataset ID

    Returns:
        True if update succeeded, False if failed
    """
    logger = log.get_logger()
    logger.info(
        f"Starting asynchronous cache update for dataset '{project_id}.{dataset_id}'..."
    )

    client_session_tuple = bigquery_client.get_bigquery_client()
    if not client_session_tuple:
        logger.error("Could not obtain BigQuery client and session.")
        return False

    bq_client, session = client_session_tuple

    if not session:
        logger.error("Could not obtain aiohttp.ClientSession.")
        return False

    success = False
    try:
        dataset = await bigquery_client.get_dataset_detail(
            bq_client, project_id, dataset_id
        )
        if not dataset:
            logger.error(f"Dataset '{project_id}.{dataset_id}' not found in project.")
            return False  # success remains False

        # Retrieve table information
        tables = await bigquery_client.fetch_tables_and_schemas(
            bq_client, project_id, dataset_id
        )

        # Save cache (save_dataset_cache is synchronous)
        current_timestamp = datetime.datetime.now(datetime.timezone.utc)
        save_dataset_cache(project_id, dataset, tables, current_timestamp)

        # Update global memory cache if it exists
        global _cache
        if _cache:
            if project_id not in _cache.datasets:
                _cache.datasets[project_id] = []
                _cache.tables[project_id] = {}  # Ensure tables dict for project exists

            # Update or add existing dataset information
            ds_idx = next(
                (
                    i
                    for i, ds_item in enumerate(_cache.datasets.get(project_id, []))
                    if ds_item.dataset_id == dataset_id
                ),
                None,
            )
            if ds_idx is not None:
                _cache.datasets[project_id][ds_idx] = dataset
            else:
                _cache.datasets[project_id].append(dataset)

            # Update table information
            _cache.tables[project_id][dataset_id] = tables
            _cache.last_updated = current_timestamp  # Update global cache timestamp

        logger.info(
            f"Updated asynchronous cache for dataset '{project_id}.{dataset_id}'."
        )
        success = True

    except Exception as e:
        logger.error(
            f"Error occurred during asynchronous cache update for dataset '{project_id}.{dataset_id}': {e}"
        )
        success = False
    finally:
        logger.debug(
            f"Closing aiohttp.ClientSession in update_dataset_cache for {project_id}.{dataset_id}."
        )
        await session.close()

    return success


async def get_cached_data() -> Optional[CachedData]:
    """
    Asynchronously retrieves valid cache data.
    If cache doesn't exist or is invalid, attempts to update.
    """
    cached_data = (
        load_cache()
    )  # load_cache is sync and also checks validity internally somewhat
    logger = log.get_logger()
    # load_cache() can return None if no valid cache files are found or they are expired.
    # is_cache_valid() provides an explicit check on the loaded _cache (if any).
    if is_cache_valid(cached_data):  # Explicitly check the loaded cache
        logger.info("Valid memory cache or file cache found.")
        return cached_data
    else:
        logger.info("No valid cache found, attempting asynchronous update.")
        # update_cache() is now async
        return await update_cache()


async def get_cached_dataset_data(
    project_id: str, dataset_id: str
) -> Tuple[Optional[DatasetMetadata], List[TableMetadata]]:
    """
    Asynchronously retrieves cache data for a specific dataset and its tables.
    If cache is invalid, updates asynchronously.

    Args:
        project_id: Project ID
        dataset_id: Dataset ID

    Returns:
        Tuple of (dataset metadata, list of table metadata)
        Returns (None, []) if dataset is not found
    """
    logger = log.get_logger()
    # is_dataset_cache_valid is sync
    if not is_dataset_cache_valid(project_id, dataset_id):
        logger.info(
            f"Cache for dataset '{project_id}.{dataset_id}' is invalid or doesn't exist, attempting asynchronous update."
        )
        # update_dataset_cache is now async
        updated_successfully = await update_dataset_cache(project_id, dataset_id)
        if not updated_successfully:
            logger.warning(
                f"Failed to update cache for dataset '{project_id}.{dataset_id}'."
            )
            return None, []
        # After successful update, the cache file should be readable.

    # If cache was valid or updated successfully, try to load from file.
    # This part remains synchronous as it's file I/O.
    try:
        cache_file = get_cache_file_path(project_id, dataset_id)
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            dataset = DatasetMetadata.model_validate(data["dataset"])
            tables = [TableMetadata.model_validate(table) for table in data["tables"]]
            return dataset, tables
    except Exception as e:
        logger.error(f"Error occurred while loading dataset cache: {e}")
        return None, []
