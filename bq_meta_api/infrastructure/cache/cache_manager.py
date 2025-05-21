# cache_manager.py: Manages local caching of BigQuery metadata
import json
import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
# Adjust imports based on new structure
from ..bigquery import BigQueryClient # Example, might need to be bq_meta_api.infrastructure.bigquery
from ... import config, log # Assuming config and log are in parent of infrastructure
# Assuming models will be in domain.entities
from ...domain.entities import CachedData, DatasetMetadata, TableMetadata # Update path later
from ...domain.repositories import ICacheRepository


# インメモリキャッシュ（シングルトン的に保持） - This might be part of the class or managed differently
_cache_instance: Optional[CachedData] = None # Renamed to avoid conflict if class uses _cache
_project_datasets_cache_instance: Dict[ # Renamed
    str, Dict[str, datetime.datetime]
] = {}  # project_id -> {dataset_id -> last_updated}


# This class will encapsulate cache logic and implement ICacheRepository
class CacheManager(ICacheRepository):
    def __init__(self):
        self.logger = log.get_logger()
        self.settings = config.get_settings()
        # Initialize in-memory cache for this instance if needed, or rely on global ones for now
        # For simplicity, this example will continue to use the global-like _cache_instance
        # but ideally, this would be instance-specific or a proper singleton pattern.
        global _cache_instance, _project_datasets_cache_instance
        self._cache = _cache_instance
        self._project_datasets_cache = _project_datasets_cache_instance
        self._bigquery_client = None # Lazy loaded

    def _get_bigquery_client(self):
        # This is a simplified way to get a BigQuery client.
        # In a real app, this might be injected or retrieved from a service locator.
        if not self._bigquery_client:
            # Assuming BigQueryClient can be instantiated without args for default project
            # Or it needs specific project_id handling here
            self._bigquery_client = BigQueryClient()
        return self._bigquery_client


    def _get_cache_file_path(self, project_id: str, dataset_id: str) -> Path:
        """
        指定されたプロジェクトIDとデータセットIDに対応するキャッシュファイルのパスを返します。
        Moved into class.
        """
        return Path(self.settings.cache_file_base_dir) / project_id / f"{dataset_id}.json"

    def _load_cache_file(
        self, project_id: str, dataset_id: str, cache_file: Path
    ) -> Optional[Tuple[DatasetMetadata, List[TableMetadata]]]: # Adjusted return type
        ttl = datetime.timedelta(seconds=self.settings.cache_ttl_seconds)
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            last_updated = datetime.datetime.fromisoformat(data["last_updated"])
            if last_updated.tzinfo is None:
                last_updated = last_updated.replace(tzinfo=datetime.timezone.utc)

            if (datetime.datetime.now(datetime.timezone.utc) - last_updated) >= ttl:
                self.logger.info(f"キャッシュの有効期限切れ: {project_id}.{dataset_id}")
                return None

            if project_id not in self._project_datasets_cache:
                self._project_datasets_cache[project_id] = {}
            self._project_datasets_cache[project_id][dataset_id] = last_updated

            dataset_meta = DatasetMetadata.model_validate(data["dataset"])
            tables = [TableMetadata.model_validate(table) for table in data["tables"]]
            return dataset_meta, tables
        except FileNotFoundError:
            self.logger.warning(f"Cache file not found: {cache_file}")
            return None
        except Exception as e:
            self.logger.error(f"Error loading cache file {cache_file}: {e}")
            return None

    def _save_dataset_cache(
        self,
        project_id: str,
        dataset: DatasetMetadata,
        tables: List[TableMetadata],
        timestamp: Optional[datetime.datetime] = None,
    ):
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

        cache_file = self._get_cache_file_path(project_id, dataset.dataset_id)
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_data = {
            "dataset": dataset.model_dump(mode="json"),
            "tables": [table.model_dump(mode="json") for table in tables],
            "last_updated": timestamp.isoformat(),
        }
        try:
            self.logger.info(f"キャッシュを保存: {project_id}.{dataset.dataset_id}")
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, indent=2)
            if project_id not in self._project_datasets_cache:
                self._project_datasets_cache[project_id] = {}
            self._project_datasets_cache[project_id][dataset.dataset_id] = timestamp
        except Exception as e:
            self.logger.error(f"キャッシュファイルの保存中にエラーが発生: {cache_file}, {e}")

    def _is_dataset_cache_valid(self, project_id: str, dataset_id: str) -> bool:
        ttl = datetime.timedelta(seconds=self.settings.cache_ttl_seconds)
        if (
            project_id in self._project_datasets_cache
            and dataset_id in self._project_datasets_cache[project_id]
        ):
            last_updated = self._project_datasets_cache[project_id][dataset_id]
            return (datetime.datetime.now(datetime.timezone.utc) - last_updated) < ttl

        cache_file = self._get_cache_file_path(project_id, dataset_id)
        if not cache_file.exists():
            return False
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            last_updated = datetime.datetime.fromisoformat(data["last_updated"])
            if last_updated.tzinfo is None:
                last_updated = last_updated.replace(tzinfo=datetime.timezone.utc)
            is_valid = (datetime.datetime.now(datetime.timezone.utc) - last_updated) < ttl
            if is_valid:
                if project_id not in self._project_datasets_cache:
                    self._project_datasets_cache[project_id] = {}
                self._project_datasets_cache[project_id][dataset_id] = last_updated
            return is_valid
        except Exception as e:
            self.logger.error(f"キャッシュ有効性チェック中にエラー: {cache_file}, {e}")
            return False

    def _update_dataset_cache(self, project_id: str, dataset_id: str) -> bool:
        self.logger.info(f"データセット '{project_id}.{dataset_id}' のキャッシュを更新中...")
        # bq_client = bigquery_client.get_bigquery_client() # Old way
        bq_repo = self._get_bigquery_client() # Using the new repository interface via helper
        if not bq_repo or not bq_repo.client: # Check if client exists
            self.logger.error("BigQueryクライアントを取得できませんでした。")
            return False
        try:
            # datasets = bigquery_client.fetch_datasets(bq_client, project_id) # Old way
            datasets = bq_repo.list_datasets(project_id) # New way
            dataset = next((ds for ds in datasets if ds.dataset_id == dataset_id), None)
            if not dataset:
                self.logger.error(f"データセット '{project_id}.{dataset_id}' が見つかりません。")
                return False
            # tables = bigquery_client.fetch_tables_and_schemas(bq_client, project_id, dataset_id) # Old way
            tables = bq_repo.list_tables(project_id, dataset_id) # New way
            self._save_dataset_cache(project_id, dataset, tables)

            global _cache_instance # Access global instance
            if _cache_instance:
                if project_id not in _cache_instance.datasets:
                    _cache_instance.datasets[project_id] = []
                ds_idx = next(
                    (i for i, ds in enumerate(_cache_instance.datasets[project_id]) if ds.dataset_id == dataset_id), None,
                )
                if ds_idx is not None:
                    _cache_instance.datasets[project_id][ds_idx] = dataset
                else:
                    _cache_instance.datasets[project_id].append(dataset)
                if project_id not in _cache_instance.tables:
                    _cache_instance.tables[project_id] = {}
                _cache_instance.tables[project_id][dataset_id] = tables
                _cache_instance.last_updated = datetime.datetime.now(datetime.timezone.utc)
            self.logger.info(f"データセット '{project_id}.{dataset_id}' のキャッシュを更新しました。")
            return True
        except Exception as e:
            self.logger.error(f"データセットキャッシュの更新中にエラーが発生: {e}")
            return False

    # ICacheRepository methods
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieves an item from the cache.
        'key' could be 'project_id.dataset_id' for dataset data, or other structured keys.
        This is a simplified example; a real implementation might need more robust key handling.
        """
        self.logger.debug(f"Cache GET request for key: {key}")
        parts = key.split('.')
        if len(parts) == 2: # Assuming key is "project_id.dataset_id"
            project_id, dataset_id = parts
            if not self._is_dataset_cache_valid(project_id, dataset_id):
                if not self._update_dataset_cache(project_id, dataset_id):
                    return None # Update failed
            
            cache_file = self._get_cache_file_path(project_id, dataset_id)
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Returning the whole dataset cache for this key. Adjust if finer-grained caching is needed.
                return data 
            except Exception as e:
                self.logger.error(f"Error reading dataset cache for {key}: {e}")
                return None
        elif len(parts) == 1: # Assuming key is for the global cache "all_metadata"
             # This part needs to align with how `load_cache` and `update_cache` are structured.
             # The current `load_cache` loads everything.
             # Let's assume 'all_metadata' is a conventional key for the full cache.
            if key == "all_metadata":
                return self.load_cache_all() # Delegate to a method that loads/updates full cache
            else:
                self.logger.warning(f"Cache GET for key '{key}' not supported in this format.")
                return None
        else:
            self.logger.warning(f"Cache GET for key '{key}' not supported or invalid format.")
            return None

    def set(self, key: str, value: Any, timeout: Optional[int] = None) -> None:
        """
        Sets an item in the cache. 'key' determines what is being set.
        'value' is the data. 'timeout' could override default TTL if implemented.
        """
        self.logger.debug(f"Cache SET request for key: {key}")
        parts = key.split('.')
        if len(parts) == 2 and isinstance(value, dict) and "dataset" in value and "tables" in value:
            # Assuming key is "project_id.dataset_id" and value is snapshot for a dataset
            project_id, dataset_id = parts
            try:
                dataset_meta = DatasetMetadata.model_validate(value["dataset"])
                tables_meta = [TableMetadata.model_validate(t) for t in value["tables"]]
                # Use current time for 'last_updated' when setting explicitly
                self._save_dataset_cache(project_id, dataset_meta, tables_meta, datetime.datetime.now(datetime.timezone.utc))
                self.logger.info(f"Cache SET for dataset {key} successful.")
            except Exception as e:
                self.logger.error(f"Error setting dataset cache for {key}: {e}")

        elif key == "all_metadata" and isinstance(value, CachedData):
            self.save_cache_all(value) # Delegate to a method that saves the full cache
        else:
            self.logger.warning(f"Cache SET for key '{key}' not supported or value is of wrong type.")

    def delete(self, key: str) -> None:
        """Deletes an item from the cache by key."""
        self.logger.debug(f"Cache DELETE request for key: {key}")
        parts = key.split('.')
        if len(parts) == 2: # "project_id.dataset_id"
            project_id, dataset_id = parts
            cache_file = self._get_cache_file_path(project_id, dataset_id)
            if cache_file.exists():
                try:
                    cache_file.unlink()
                    self.logger.info(f"Cache file {cache_file} deleted.")
                    # Also remove from in-memory lookup
                    if project_id in self._project_datasets_cache and dataset_id in self._project_datasets_cache[project_id]:
                        del self._project_datasets_cache[project_id][dataset_id]
                        if not self._project_datasets_cache[project_id]:
                            del self._project_datasets_cache[project_id]
                except Exception as e:
                    self.logger.error(f"Error deleting cache file {cache_file}: {e}")
            else:
                self.logger.info(f"Cache file {cache_file} not found for deletion.")
        elif key == "all_metadata": # Request to delete all cache files
            cache_dir = Path(self.settings.cache_file_base_dir)
            if cache_dir.exists():
                for project_dir in cache_dir.iterdir():
                    if project_dir.is_dir():
                        for cache_file in project_dir.glob("*.json"):
                            try:
                                cache_file.unlink()
                                self.logger.info(f"Deleted cache file: {cache_file}")
                            except Exception as e:
                                self.logger.error(f"Error deleting cache file {cache_file}: {e}")
                # Clear in-memory state
                global _cache_instance, _project_datasets_cache_instance
                _cache_instance = None
                _project_datasets_cache_instance.clear()
                self.logger.info("All disk cache files and in-memory cache cleared.")
        else:
            self.logger.warning(f"Cache DELETE for key '{key}' not supported.")

    def load_cache_all(self) -> Optional[CachedData]:
        """Loads the entire cache from disk, similar to original load_cache."""
        global _cache_instance
        cache_dir = Path(self.settings.cache_file_base_dir)
        if _cache_instance and self._is_cache_valid_global(_cache_instance): # check global cache validity
            self.logger.debug("メモリキャッシュを使用します。")
            return _cache_instance

        latest_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        all_datasets: Dict[str, List[DatasetMetadata]] = {}
        all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}

        if cache_dir.exists():
            self.logger.info(f"キャッシュディレクトリから読み込みます: {cache_dir}")
            for project_dir in cache_dir.iterdir():
                if project_dir.is_dir():
                    project_id = project_dir.name
                    all_datasets[project_id] = []
                    all_tables[project_id] = {}
                    for cache_file in project_dir.glob("*.json"):
                        dataset_id = cache_file.stem
                        loaded_data = self._load_cache_file(project_id, dataset_id, cache_file)
                        if loaded_data:
                            dataset_meta, tables = loaded_data
                            all_datasets[project_id].append(dataset_meta)
                            all_tables[project_id][dataset_id] = tables
                            if project_id in self._project_datasets_cache and dataset_id in self._project_datasets_cache[project_id]:
                                latest_updated = max(latest_updated, self._project_datasets_cache[project_id][dataset_id])
        
        if latest_updated > datetime.datetime.min.replace(tzinfo=datetime.timezone.utc):
            _cache_instance = CachedData(datasets=all_datasets, tables=all_tables, last_updated=latest_updated)
            return _cache_instance
        
        self.logger.info("有効な全プロジェクトキャッシュが見つかりません。")
        return None

    def save_cache_all(self, data: CachedData):
        """Saves the entire CachedData object to disk."""
        global _cache_instance
        self.logger.info("全キャッシュを保存します。")
        try:
            for project_id, datasets_in_project in data.datasets.items():
                for dataset in datasets_in_project:
                    tables_for_dataset = data.tables.get(project_id, {}).get(dataset.dataset_id, [])
                    self._save_dataset_cache(project_id, dataset, tables_for_dataset, data.last_updated)
            _cache_instance = data # Update global in-memory cache
            self.logger.info("全キャッシュの保存が完了しました。")
        except Exception as e:
            self.logger.error(f"全キャッシュファイルの保存中にエラー: {e}")
            
    def _is_cache_valid_global(self, cached_data: Optional[CachedData]) -> bool:
        """Checks if the global CachedData object is valid based on TTL."""
        if not cached_data or not cached_data.last_updated:
            return False
        ttl = datetime.timedelta(seconds=self.settings.cache_ttl_seconds)
        now = datetime.datetime.now(datetime.timezone.utc)
        last_updated_aware = cached_data.last_updated
        if last_updated_aware.tzinfo is None:
            last_updated_aware = last_updated_aware.replace(tzinfo=datetime.timezone.utc)
        is_valid = (now - last_updated_aware) < ttl
        self.logger.debug(f"グローバルキャッシュ有効性: Now={now}, LastUpdated={last_updated_aware}, TTL={ttl}, Valid={is_valid}")
        return is_valid

    def update_cache_all(self) -> Optional[CachedData]:
        """Updates the entire cache from BigQuery."""
        global _cache_instance
        self.logger.info("全キャッシュの更新を開始します...")
        # bq_client = bigquery_client.get_bigquery_client() # Old way
        bq_repo = self._get_bigquery_client()
        if not bq_repo or not bq_repo.client:
            self.logger.error("キャッシュ更新のためBigQueryクライアントを取得できませんでした。")
            return None
        
        if not self.settings.project_ids:
            self.logger.warning("キャッシュ更新対象のプロジェクトIDが設定されていません。")
            _cache_instance = CachedData(last_updated=datetime.datetime.now(datetime.timezone.utc))
            return _cache_instance

        all_datasets: Dict[str, List[DatasetMetadata]] = {}
        all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}
        timestamp = datetime.datetime.now(datetime.timezone.utc)

        for project_id in self.settings.project_ids:
            self.logger.info(f"プロジェクト '{project_id}' のメタデータを取得中...")
            # datasets = bigquery_client.fetch_datasets(bq_client, project_id) # Old
            datasets = bq_repo.list_datasets(project_id) # New
            all_datasets[project_id] = datasets
            all_tables[project_id] = {}
            for dataset in datasets:
                self.logger.info(f"データセット '{project_id}.{dataset.dataset_id}' のテーブルを取得中...")
                # tables = bigquery_client.fetch_tables_and_schemas(bq_client, project_id, dataset.dataset_id) # Old
                tables = bq_repo.list_tables(project_id, dataset.dataset_id) # New
                all_tables[project_id][dataset.dataset_id] = tables
                self._save_dataset_cache(project_id, dataset, tables, timestamp) # Save individual files

        new_cache_data = CachedData(datasets=all_datasets, tables=all_tables, last_updated=timestamp)
        _cache_instance = new_cache_data # Update global in-memory cache
        self.logger.info("全キャッシュの更新が完了しました。")
        return new_cache_data

    async def get_cached_data_all(self) -> Optional[CachedData]:
        """Async wrapper to get all cached data, updating if necessary."""
        cached_data = self.load_cache_all()
        if cached_data:
            self.logger.info("有効な全キャッシュが見つかりました。")
            return cached_data
        else:
            self.logger.info("有効な全キャッシュが見つからないため、更新を試みます。")
            return self.update_cache_all()

    async def get_cached_dataset_data(
        self, project_id: str, dataset_id: str
    ) -> Tuple[Optional[DatasetMetadata], List[TableMetadata]]:
        """Async wrapper for specific dataset retrieval."""
        if not self._is_dataset_cache_valid(project_id, dataset_id):
            if not self._update_dataset_cache(project_id, dataset_id):
                return None, []
        
        cache_file = self._get_cache_file_path(project_id, dataset_id)
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            dataset = DatasetMetadata.model_validate(data["dataset"])
            tables = [TableMetadata.model_validate(t) for t in data["tables"]]
            return dataset, tables
        except Exception as e:
            self.logger.error(f"データセットキャッシュ '{project_id}.{dataset_id}' の非同期読み込みエラー: {e}")
            return None, []

# Remove old global functions or adapt them to use CacheManager instance
# For example:
# def get_cache_file_path(project_id: str, dataset_id: str) -> Path:
#    return CacheManager()._get_cache_file_path(project_id, dataset_id) # Not ideal to instantiate per call

# It's better to have a single CacheManager instance exported or used within the module.
# For now, the old functions are effectively replaced by methods of CacheManager.
# Callers will need to be updated to use a CacheManager instance.

# Global functions like load_cache, save_cache, is_cache_valid, update_cache,
# get_cached_data, get_cached_dataset_data are now methods of CacheManager
# (e.g., load_cache_all, save_cache_all, _is_cache_valid_global, update_cache_all,
# get_cached_data_all, get_cached_dataset_data respectively).
# The `async` nature of some original functions has been preserved in the new async methods.
# Direct usage of global _cache and _project_datasets_cache should be phased out
# in favor of instance variables if CacheManager is instantiated multiple times,
# or carefully managed if a singleton CacheManager is intended.
# This refactoring assumes a singleton-like behavior for _cache_instance and _project_datasets_cache_instance.


def get_cache_file_path(project_id: str, dataset_id: str) -> Path:
    """
    指定されたプロジェクトIDとデータセットIDに対応するキャッシュファイルのパスを返します。
    Moved into class.
    """
    # This function is now a method _get_cache_file_path of CacheManager
    # Kept here to show where it was, but it should be removed from global scope.
    # To use it: CacheManager()._get_cache_file_path(project_id, dataset_id)
    # However, direct instantiation per call is not good. A shared instance is preferred.
    pass # Placeholder, this global function is removed.


# All the functions below (load_cache_file, load_cache, save_dataset_cache, save_cache,
# is_cache_valid, is_dataset_cache_valid, update_cache, update_dataset_cache,
# get_cached_data, get_cached_dataset_data) have been refactored into methods
# of the CacheManager class. Refer to the CacheManager class definition for the new structure.

# For example:
# load_cache() -> CacheManager().load_cache_all()
# save_cache(data) -> CacheManager().save_cache_all(data)
# get(key) -> CacheManager().get(key)
# set(key, value) -> CacheManager().set(key, value)
# delete(key) -> CacheManager().delete(key)

# The old global functions are no longer directly available and should be removed
# to avoid confusion and ensure the new class-based structure is used.
# Callers should instantiate CacheManager and call its methods.
