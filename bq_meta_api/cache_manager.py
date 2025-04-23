# cache_manager.py: Manages local caching of BigQuery metadata
import json
import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from bq_meta_api.config import settings
from bq_meta_api.models import CachedData, DatasetMetadata, TableMetadata
from bq_meta_api import bigquery_client, log


logger = log.logger

# キャッシュ関連の定数
BASE_CACHE_DIR = Path(settings.cache_file_base_dir)
CACHE_TTL = datetime.timedelta(seconds=settings.cache_ttl_seconds)

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
    return BASE_CACHE_DIR / project_id / f"{dataset_id}.json"


def load_cache_file(
    project_id: str, dataset_id: str, cache_file: Path
) -> Optional[CachedData]:
    with open(cache_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        last_updated = datetime.datetime.fromisoformat(data["last_updated"])

        # タイムゾーン情報がない場合はUTCとみなす
        if last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=datetime.timezone.utc)

        # キャッシュが有効期限内か確認
        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - last_updated) >= CACHE_TTL:
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

    # メモリ上のキャッシュが有効ならそれを返す
    if _cache and is_cache_valid(_cache):
        logger.debug("メモリキャッシュを使用します。")
        return _cache

    latest_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    # 新しいキャッシュ構造からデータを読み込む
    if BASE_CACHE_DIR.exists():
        logger.info(f"キャッシュディレクトリから読み込みます: {BASE_CACHE_DIR}")

        all_datasets: Dict[str, List[DatasetMetadata]] = {}
        all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}
        latest_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

        # プロジェクトIDのディレクトリを走査
        for project_dir in BASE_CACHE_DIR.iterdir():
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
    now = datetime.datetime.now(datetime.timezone.utc)
    # last_updatedがtimezone情報を持っていない場合があるため、awareに変換
    last_updated_aware = cached_data.last_updated
    if last_updated_aware.tzinfo is None:
        # naiveなdatetimeの場合、ローカルタイムゾーンと仮定するか、UTCと仮定するか。
        # ここではUTCとして扱う（BigQueryのタイムスタンプは通常UTC）
        last_updated_aware = last_updated_aware.replace(tzinfo=datetime.timezone.utc)

    is_valid = (now - last_updated_aware) < CACHE_TTL
    logger.debug(
        f"キャッシュ有効性チェック: Now={now}, LastUpdated={last_updated_aware}, TTL={CACHE_TTL}, Valid={is_valid}"
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
    # メモリキャッシュをチェック
    if (
        project_id in _project_datasets_cache
        and dataset_id in _project_datasets_cache[project_id]
    ):
        last_updated = _project_datasets_cache[project_id][dataset_id]
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - last_updated) < CACHE_TTL

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
            is_valid = (now - last_updated) < CACHE_TTL

            # メモリキャッシュを更新
            if is_valid:
                if project_id not in _project_datasets_cache:
                    _project_datasets_cache[project_id] = {}
                _project_datasets_cache[project_id][dataset_id] = last_updated

            return is_valid
    except Exception as e:
        logger.error(f"キャッシュ有効性チェック中にエラー: {cache_file}, {e}")
        return False


def update_cache() -> Optional[CachedData]:
    """
    BigQueryから最新のメタデータを取得し、新しいキャッシュデータを作成して返します。
    取得に失敗した場合はNoneを返します。
    """
    global _cache
    logger.info("キャッシュの更新を開始します...")
    bq_client = bigquery_client.get_bigquery_client()
    if not bq_client:
        logger.error("キャッシュ更新のためBigQueryクライアントを取得できませんでした。")
        return None
    if not settings.project_ids:
        logger.warning("キャッシュ更新対象のプロジェクトIDが設定されていません。")
        return CachedData(
            last_updated=datetime.datetime.now(datetime.timezone.utc)
        )  # 空のキャッシュを返す

    all_datasets: Dict[str, List[DatasetMetadata]] = {}
    all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}
    timestamp = datetime.datetime.now(datetime.timezone.utc)

    for project_id in settings.project_ids:
        logger.info(f"プロジェクト '{project_id}' のメタデータを取得中...")
        datasets = bigquery_client.fetch_datasets(bq_client, project_id)
        all_datasets[project_id] = datasets
        all_tables[project_id] = {}

        for dataset in datasets:
            logger.info(
                f"データセット '{project_id}.{dataset.dataset_id}' のテーブルを取得中..."
            )
            tables = bigquery_client.fetch_tables_and_schemas(
                bq_client, project_id, dataset.dataset_id
            )
            all_tables[project_id][dataset.dataset_id] = tables

            # 各データセットごとにキャッシュを保存
            save_dataset_cache(project_id, dataset, tables, timestamp)

    new_cache_data = CachedData(
        datasets=all_datasets,
        tables=all_tables,
        last_updated=timestamp,
    )
    logger.info("キャッシュの更新が完了しました。")
    _cache = new_cache_data  # メモリキャッシュを更新
    return new_cache_data


def update_dataset_cache(project_id: str, dataset_id: str) -> bool:
    """
    特定のデータセットのキャッシュを更新します。

    Args:
        project_id: プロジェクトID
        dataset_id: データセットID

    Returns:
        更新に成功した場合はTrue、失敗した場合はFalse
    """
    logger.info(f"データセット '{project_id}.{dataset_id}' のキャッシュを更新中...")
    bq_client = bigquery_client.get_bigquery_client()
    if not bq_client:
        logger.error("BigQueryクライアントを取得できませんでした。")
        return False

    try:
        # データセット情報を取得
        datasets = bigquery_client.fetch_datasets(bq_client, project_id)
        dataset = next((ds for ds in datasets if ds.dataset_id == dataset_id), None)
        if not dataset:
            logger.error(f"データセット '{project_id}.{dataset_id}' が見つかりません。")
            return False

        # テーブル情報を取得
        tables = bigquery_client.fetch_tables_and_schemas(
            bq_client, project_id, dataset_id
        )

        # キャッシュを保存
        save_dataset_cache(project_id, dataset, tables)

        # グローバルキャッシュがある場合は更新
        global _cache
        if _cache:
            if project_id not in _cache.datasets:
                _cache.datasets[project_id] = []

            # 既存のデータセット情報を更新または追加
            ds_idx = next(
                (
                    i
                    for i, ds in enumerate(_cache.datasets[project_id])
                    if ds.dataset_id == dataset_id
                ),
                None,
            )
            if ds_idx is not None:
                _cache.datasets[project_id][ds_idx] = dataset
            else:
                _cache.datasets[project_id].append(dataset)

            # テーブル情報を更新
            if project_id not in _cache.tables:
                _cache.tables[project_id] = {}
            _cache.tables[project_id][dataset_id] = tables

            # 最終更新時刻を更新
            _cache.last_updated = datetime.datetime.now(datetime.timezone.utc)

        logger.info(
            f"データセット '{project_id}.{dataset_id}' のキャッシュを更新しました。"
        )
        return True

    except Exception as e:
        logger.error(f"データセットキャッシュの更新中にエラーが発生: {e}")
        return False


def get_cached_data() -> Optional[CachedData]:
    """
    有効なキャッシュデータを取得します。
    キャッシュが存在しないか無効な場合は、更新を試みます。
    """
    cached_data = load_cache()
    if cached_data:  # is_cache_valid は load_cache 内でチェック済み
        logger.info("有効なキャッシュが見つかりました。")
        return cached_data
    else:
        logger.info("有効なキャッシュが見つからないため、更新を試みます。")
        return update_cache()


# --- 新しい関数 ---
def get_cached_dataset_data(
    project_id: str, dataset_id: str
) -> Tuple[Optional[DatasetMetadata], List[TableMetadata]]:
    """
    特定のデータセットとそのテーブルのキャッシュデータを取得します。
    キャッシュが無効な場合は更新します。

    Args:
        project_id: プロジェクトID
        dataset_id: データセットID

    Returns:
        (データセットメタデータ, テーブルメタデータのリスト)のタプル
        データセットが見つからない場合は(None, [])を返します
    """
    # キャッシュが有効かチェック
    if not is_dataset_cache_valid(project_id, dataset_id):
        # キャッシュを更新
        if not update_dataset_cache(project_id, dataset_id):
            return None, []

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


# --- テスト用コード ---
if __name__ == "__main__":
    logger.info("Cache Manager テスト実行...")

    # 1. キャッシュ取得（初回または期限切れの場合、更新が走るはず）
    logger.info("\n--- 1. キャッシュ取得/更新 ---")
    data = get_cached_data()
    if data:
        logger.info(f"キャッシュ取得成功。最終更新: {data.last_updated}")
        logger.info(
            f"データセット数: {sum(len(ds_list) for ds_list in data.datasets.values())}"
        )
        logger.info(
            f"テーブル数: {sum(len(tbl_list) for ds_dict in data.tables.values() for tbl_list in ds_dict.values())}"
        )
    else:
        logger.info("キャッシュの取得/更新に失敗しました。")

    # 2. 再度キャッシュ取得（TTL内であれば更新は走らないはず）
    logger.info("\n--- 2. 再度キャッシュ取得 (TTL内) ---")
    data_again = get_cached_data()
    if data_again:
        logger.info(f"キャッシュ取得成功。最終更新: {data_again.last_updated}")
    else:
        logger.info("キャッシュの取得に失敗しました。")

    # 3. キャッシュの手動更新
    logger.info("\n--- 3. キャッシュの手動更新 ---")
    updated_data = update_cache()
    if updated_data:
        logger.info(f"キャッシュの手動更新成功。最終更新: {updated_data.last_updated}")
    else:
        logger.info("キャッシュの手動更新に失敗しました。")
