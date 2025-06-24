# cache_manager.py: Manages local caching of BigQuery metadata
import asyncio
import json
import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from bq_meta_api.repositories import config
from bq_meta_api.core.entities import CachedData, DatasetMetadata, TableMetadata
from bq_meta_api.repositories import bigquery_client, log
from bq_meta_api.repositories.config import should_include_dataset


# インメモリキャッシュ（シングルトン的に保持）
_cache: Optional[CachedData] = None
_project_datasets_cache: Dict[
    str, Dict[str, datetime.datetime]
] = {}  # project_id -> {dataset_id -> last_updated}


def get_cache_file_path(project_id: str, dataset_id: str) -> Path:
    """
    指定されたプロジェクトIDとデータセットIDに対応するキャッシュファイルのパスを返します。

    Args:
        project_id: プロジェクトID
        dataset_id: データセットID

    Returns:
        キャッシュファイルへのPathオブジェクト
    """
    setting = config.get_settings()
    return Path(setting.cache_file_base_dir) / project_id / f"{dataset_id}.json"


def load_cache_file(
    project_id: str, dataset_id: str, cache_file: Path
) -> Optional[CachedData]:
    logger = log.get_logger()
    settings = config.get_settings()
    ttl = datetime.timedelta(seconds=settings.cache_ttl_seconds)
    with open(cache_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        last_updated = datetime.datetime.fromisoformat(data["last_updated"])

        # タイムゾーン情報がない場合はUTCとみなす
        if last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=datetime.timezone.utc)

        # キャッシュが有効期限内か確認
        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - last_updated) >= ttl:
            logger.info(f"キャッシュの有効期限切れ: {project_id}.{dataset_id}")
            return None

        # メモリキャッシュを更新
        if project_id not in _project_datasets_cache:
            _project_datasets_cache[project_id] = {}
        _project_datasets_cache[project_id][dataset_id] = last_updated

        # データセット情報を追加
        dataset_meta = DatasetMetadata.model_validate(data["dataset"])

        # テーブル情報を追加
        tables = [TableMetadata.model_validate(table) for table in data["tables"]]
        return dataset_meta, tables


def load_cache() -> Optional[CachedData]:
    """
    キャッシュファイルを読み込み、CachedDataオブジェクトを返します。
    ファイルが存在しない、または無効な場合はNoneを返します。
    """
    global _cache, _project_datasets_cache
    settings = config.get_settings()
    cache_dir = Path(settings.cache_file_base_dir)
    logger = log.get_logger()

    # メモリ上のキャッシュが有効ならそれを返す
    if _cache and is_cache_valid(_cache):
        logger.debug("メモリキャッシュを使用します。")
        return _cache

    latest_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    # 新しいキャッシュ構造からデータを読み込む
    if cache_dir.exists():
        logger.info(f"キャッシュディレクトリから読み込みます: {cache_dir}")

        all_datasets: Dict[str, List[DatasetMetadata]] = {}
        all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}
        latest_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

        # プロジェクトIDのディレクトリを走査
        for project_dir in cache_dir.iterdir():
            if project_dir.is_dir():
                project_id = project_dir.name
                all_datasets[project_id] = []
                all_tables[project_id] = {}

                # データセットのキャッシュファイルを走査
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

        # 有効なキャッシュデータがある場合
        if latest_updated > datetime.datetime.min.replace(tzinfo=datetime.timezone.utc):
            _cache = CachedData(
                datasets=all_datasets, tables=all_tables, last_updated=latest_updated
            )
            return _cache

    logger.info("有効なキャッシュが見つかりません")
    return None


def save_dataset_cache(
    project_id: str,
    dataset: DatasetMetadata,
    tables: List[TableMetadata],
    timestamp: Optional[datetime.datetime] = None,
):
    """
    データセットとそのテーブルの情報をキャッシュファイルに保存します。

    Args:
        project_id: プロジェクトID
        dataset: データセットのメタデータ
        tables: テーブルのメタデータリスト
        timestamp: タイムスタンプ（指定がなければ現在時刻）
    """
    logger = log.get_logger()
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc)

    # タイムゾーン情報がない場合はUTCとみなす
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

    cache_file = get_cache_file_path(project_id, dataset.dataset_id)

    # ディレクトリが存在しない場合は作成
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    # キャッシュデータを作成
    cache_data = {
        "dataset": dataset.model_dump(mode="json"),
        "tables": [table.model_dump(mode="json") for table in tables],
        "last_updated": timestamp.isoformat(),
    }

    try:
        logger.info(f"キャッシュを保存: {project_id}.{dataset.dataset_id}")
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)

        # メモリキャッシュも更新
        if project_id not in _project_datasets_cache:
            _project_datasets_cache[project_id] = {}
        _project_datasets_cache[project_id][dataset.dataset_id] = timestamp
    except Exception as e:
        logger.error(f"キャッシュファイルの保存中にエラーが発生: {cache_file}, {e}")


def save_cache(data: CachedData):
    """CachedDataオブジェクトをキャッシュファイルに保存します。"""
    global _cache
    logger = log.get_logger()
    try:
        logger.info("キャッシュを保存します。")

        # 各プロジェクト・データセットごとにキャッシュファイルを保存
        for project_id, datasets in data.datasets.items():
            for dataset in datasets:
                if (
                    project_id in data.tables
                    and dataset.dataset_id in data.tables[project_id]
                ):
                    tables = data.tables[project_id][dataset.dataset_id]
                    save_dataset_cache(project_id, dataset, tables, data.last_updated)

        _cache = data  # メモリキャッシュを更新
        logger.info("キャッシュの保存が完了しました。")
    except Exception as e:
        logger.error(f"キャッシュファイルの保存中にエラーが発生しました: {e}")


def is_cache_valid(cached_data: Optional[CachedData]) -> bool:
    """キャッシュデータが有効期限内かどうかをチェックします。"""
    if not cached_data or not cached_data.last_updated:
        return False
    logger = log.get_logger()
    settings = config.get_settings()
    ttl = datetime.timedelta(seconds=settings.cache_ttl_seconds)
    now = datetime.datetime.now(datetime.timezone.utc)
    # last_updatedがtimezone情報を持っていない場合があるため、awareに変換
    last_updated_aware = cached_data.last_updated
    if last_updated_aware.tzinfo is None:
        # naiveなdatetimeの場合、ローカルタイムゾーンと仮定するか、UTCと仮定するか。
        # ここではUTCとして扱う（BigQueryのタイムスタンプは通常UTC）
        last_updated_aware = last_updated_aware.replace(tzinfo=datetime.timezone.utc)

    is_valid = (now - last_updated_aware) < ttl
    logger.debug(
        f"キャッシュ有効性チェック: Now={now}, LastUpdated={last_updated_aware}, TTL={ttl}, Valid={is_valid}"
    )
    return is_valid


def is_dataset_cache_valid(project_id: str, dataset_id: str) -> bool:
    """
    特定のデータセットのキャッシュが有効かどうかをチェックします。

    Args:
        project_id: プロジェクトID
        dataset_id: データセットID

    Returns:
        キャッシュが有効な場合はTrue、それ以外はFalse
    """
    logger = log.get_logger()
    settings = config.get_settings()
    ttl = datetime.timedelta(seconds=settings.cache_ttl_seconds)
    # メモリキャッシュをチェック
    if (
        project_id in _project_datasets_cache
        and dataset_id in _project_datasets_cache[project_id]
    ):
        last_updated = _project_datasets_cache[project_id][dataset_id]
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - last_updated) < ttl

    # ファイルをチェック
    cache_file = get_cache_file_path(project_id, dataset_id)
    if not cache_file.exists():
        return False

    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            last_updated = datetime.datetime.fromisoformat(data["last_updated"])

            # タイムゾーン情報がない場合はUTCとみなす
            if last_updated.tzinfo is None:
                last_updated = last_updated.replace(tzinfo=datetime.timezone.utc)

            now = datetime.datetime.now(datetime.timezone.utc)
            is_valid = (now - last_updated) < ttl

            # メモリキャッシュを更新
            if is_valid:
                if project_id not in _project_datasets_cache:
                    _project_datasets_cache[project_id] = {}
                _project_datasets_cache[project_id][dataset_id] = last_updated

            return is_valid
    except Exception as e:
        logger.error(f"キャッシュ有効性チェック中にエラー: {cache_file}, {e}")
        return False


async def update_cache() -> Optional[CachedData]:
    """
    BigQueryから最新のメタデータを非同期で取得し、新しいキャッシュデータを作成して返します。
    取得に失敗した場合はNoneを返します。
    """
    global _cache
    logger = log.get_logger()
    logger.info("非同期キャッシュの更新を開始します...")
    settings = config.get_settings()

    bq_client = bigquery_client.get_bigquery_client()
    if not bq_client:
        logger.error(
            "キャッシュ更新のためBigQueryクライアントとセッションを取得できませんでした。"
        )
        return None

    if not settings.project_ids:
        logger.warning("キャッシュ更新対象のプロジェクトIDが設定されていません。")
        return CachedData(last_updated=datetime.datetime.now(datetime.timezone.utc))
    all_datasets: Dict[str, List[DatasetMetadata]] = {}
    all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    try:
        tasks = []
        for project_id in settings.project_ids:
            logger.info(f"プロジェクト '{project_id}' のメタデータを非同期取得中...")
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
            logger.info(f"プロジェクト '{project_id}' のメタデータ取得が完了しました。")

        new_cache_data = CachedData(
            datasets=all_datasets,
            tables=all_tables,
            last_updated=timestamp,
        )
        logger.info("非同期キャッシュの更新が完了しました。")
        _cache = new_cache_data  # メモリキャッシュを更新
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

    # データセットフィルターを適用
    settings = config.get_settings()
    if settings.dataset_filters:
        filtered_datasets = []
        for dataset in all_datasets:
            if should_include_dataset(
                project_id, dataset.dataset_id, settings.dataset_filters
            ):
                filtered_datasets.append(dataset)
                logger.debug(
                    f"データセット {project_id}.{dataset.dataset_id} がフィルター条件に一致しました"
                )
            else:
                logger.debug(
                    f"データセット {project_id}.{dataset.dataset_id} がフィルター条件で除外されました"
                )
        datasets = filtered_datasets
        logger.info(
            f"プロジェクト '{project_id}' から {len(all_datasets)} 個のデータセットを取得し、フィルター後 {len(datasets)} 個が対象となりました。"
        )
    else:
        datasets = all_datasets
        logger.info(
            f"プロジェクト '{project_id}' から {len(datasets)} 個のデータセット情報を取得しました。"
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
    特定のデータセットのキャッシュを非同期で更新します。

    Args:
        project_id: プロジェクトID
        dataset_id: データセットID

    Returns:
        更新に成功した場合はTrue、失敗した場合はFalse
    """
    logger = log.get_logger()
    logger.info(
        f"データセット '{project_id}.{dataset_id}' の非同期キャッシュ更新を開始..."
    )

    client_session_tuple = bigquery_client.get_bigquery_client()
    if not client_session_tuple:
        logger.error("BigQueryクライアントとセッションを取得できませんでした。")
        return False

    bq_client, session = client_session_tuple

    if not session:
        logger.error("aiohttp.ClientSessionが取得できませんでした。")
        return False

    success = False
    try:
        dataset = await bigquery_client.get_dataset_detail(
            bq_client, project_id, dataset_id
        )
        if not dataset:
            logger.error(
                f"データセット '{project_id}.{dataset_id}' がプロジェクト内で見つかりません。"
            )
            return False  # success remains False

        # テーブル情報を取得
        tables = await bigquery_client.fetch_tables_and_schemas(
            bq_client, project_id, dataset_id
        )

        # キャッシュを保存 (save_dataset_cache is synchronous)
        current_timestamp = datetime.datetime.now(datetime.timezone.utc)
        save_dataset_cache(project_id, dataset, tables, current_timestamp)

        # グローバルメモリキャッシュがある場合は更新
        global _cache
        if _cache:
            if project_id not in _cache.datasets:
                _cache.datasets[project_id] = []
                _cache.tables[project_id] = {}  # Ensure tables dict for project exists

            # 既存のデータセット情報を更新または追加
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

            # テーブル情報を更新
            _cache.tables[project_id][dataset_id] = tables
            _cache.last_updated = current_timestamp  # Update global cache timestamp

        logger.info(
            f"データセット '{project_id}.{dataset_id}' の非同期キャッシュを更新しました。"
        )
        success = True

    except Exception as e:
        logger.error(
            f"データセット '{project_id}.{dataset_id}' の非同期キャッシュ更新中にエラーが発生: {e}"
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
    有効なキャッシュデータを非同期で取得します。
    キャッシュが存在しないか無効な場合は、更新を試みます。
    """
    cached_data = (
        load_cache()
    )  # load_cache is sync and also checks validity internally somewhat
    logger = log.get_logger()
    # load_cache() can return None if no valid cache files are found or they are expired.
    # is_cache_valid() provides an explicit check on the loaded _cache (if any).
    if is_cache_valid(cached_data):  # Explicitly check the loaded cache
        logger.info("有効なメモリキャッシュまたはファイルキャッシュが見つかりました。")
        return cached_data
    else:
        logger.info("有効なキャッシュが見つからないため、非同期更新を試みます。")
        # update_cache() is now async
        return await update_cache()


async def get_cached_dataset_data(
    project_id: str, dataset_id: str
) -> Tuple[Optional[DatasetMetadata], List[TableMetadata]]:
    """
    特定のデータセットとそのテーブルのキャッシュデータを非同期で取得します。
    キャッシュが無効な場合は非同期で更新します。

    Args:
        project_id: プロジェクトID
        dataset_id: データセットID

    Returns:
        (データセットメタデータ, テーブルメタデータのリスト)のタプル
        データセットが見つからない場合は(None, [])を返します
    """
    logger = log.get_logger()
    # is_dataset_cache_valid is sync
    if not is_dataset_cache_valid(project_id, dataset_id):
        logger.info(
            f"データセット '{project_id}.{dataset_id}' のキャッシュが無効または存在しないため、非同期更新を試みます。"
        )
        # update_dataset_cache is now async
        updated_successfully = await update_dataset_cache(project_id, dataset_id)
        if not updated_successfully:
            logger.warning(
                f"データセット '{project_id}.{dataset_id}' のキャッシュ更新に失敗しました。"
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
        logger.error(f"データセットキャッシュの読み込み中にエラーが発生: {e}")
        return None, []
