# cache_manager.py: Manages local caching of BigQuery metadata
import logging
import json
import datetime
from pathlib import Path
from typing import Optional, Dict, List
from .config import settings
from .models import CachedData, DatasetMetadata, TableMetadata
from . import bigquery_client # bigquery_clientモジュールをインポート

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_FILE = Path(settings.cache_file_path)
CACHE_TTL = datetime.timedelta(seconds=settings.cache_ttl_seconds)

_cache: Optional[CachedData] = None # インメモリキャッシュ（シングルトン的に保持）

def load_cache() -> Optional[CachedData]:
    """
    キャッシュファイルを読み込み、CachedDataオブジェクトを返します。
    ファイルが存在しない、または無効な場合はNoneを返します。
    """
    global _cache
    if _cache and is_cache_valid(_cache): # メモリ上のキャッシュが有効ならそれを返す
        logger.debug("メモリキャッシュを使用します。")
        return _cache

    if not CACHE_FILE.exists():
        logger.info(f"キャッシュファイルが見つかりません: {CACHE_FILE}")
        return None
    try:
        logger.info(f"キャッシュファイル {CACHE_FILE} を読み込みます。")
        # Pydantic v2では parse_file がクラスメソッドではない
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            cached_data = CachedData.model_validate(data) # model_validateを使用
        if is_cache_valid(cached_data):
            _cache = cached_data # メモリにもキャッシュ
            return cached_data
        else:
            logger.info("キャッシュの有効期限が切れています。")
            _cache = None # 古いメモリキャッシュをクリア
            return None
    except json.JSONDecodeError as e:
        logger.error(f"キャッシュファイルのJSONデコードに失敗しました: {e}")
        _cache = None
        return None
    except Exception as e:
        logger.error(f"キャッシュファイルの読み込み中に予期せぬエラーが発生しました: {e}")
        _cache = None
        return None

def save_cache(data: CachedData):
    """CachedDataオブジェクトをキャッシュファイルに保存します。"""
    global _cache
    try:
        logger.info(f"キャッシュをファイル {CACHE_FILE} に保存します。")
        # ディレクトリが存在しない場合は作成
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Pydantic v2では model_dump_json を使用
        json_data = data.model_dump_json(indent=2)
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            f.write(json_data)
        _cache = data # メモリキャッシュも更新
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
    logger.debug(f"キャッシュ有効性チェック: Now={now}, LastUpdated={last_updated_aware}, TTL={CACHE_TTL}, Valid={is_valid}")
    return is_valid

def update_cache() -> Optional[CachedData]:
    """
    BigQueryから最新のメタデータを取得し、新しいキャッシュデータを作成して返します。
    取得に失敗した場合はNoneを返します。
    """
    logger.info("キャッシュの更新を開始します...")
    bq_client = bigquery_client.get_bigquery_client()
    if not bq_client:
        logger.error("キャッシュ更新のためBigQueryクライアントを取得できませんでした。")
        return None
    if not settings.project_ids:
         logger.warning("キャッシュ更新対象のプロジェクトIDが設定されていません。")
         return CachedData(last_updated=datetime.datetime.now(datetime.timezone.utc)) # 空のキャッシュを返す

    all_datasets: Dict[str, List[DatasetMetadata]] = {}
    all_tables: Dict[str, Dict[str, List[TableMetadata]]] = {}

    for project_id in settings.project_ids:
        logger.info(f"プロジェクト '{project_id}' のメタデータを取得中...")
        datasets = bigquery_client.fetch_datasets(bq_client, project_id)
        all_datasets[project_id] = datasets
        all_tables[project_id] = {}
        for dataset in datasets:
            logger.info(f"データセット '{project_id}.{dataset.dataset_id}' のテーブルを取得中...")
            tables = bigquery_client.fetch_tables_and_schemas(bq_client, project_id, dataset.dataset_id)
            all_tables[project_id][dataset.dataset_id] = tables

    new_cache_data = CachedData(
        datasets=all_datasets,
        tables=all_tables,
        last_updated=datetime.datetime.now(datetime.timezone.utc)
    )
    logger.info("キャッシュの更新が完了しました。")
    save_cache(new_cache_data) # 更新したキャッシュを保存
    return new_cache_data

def get_cached_data() -> Optional[CachedData]:
    """
    有効なキャッシュデータを取得します。
    キャッシュが存在しないか無効な場合は、更新を試みます。
    """
    cached_data = load_cache()
    if cached_data: # is_cache_valid は load_cache 内でチェック済み
        logger.info("有効なキャッシュが見つかりました。")
        return cached_data
    else:
        logger.info("有効なキャッシュが見つからないため、更新を試みます。")
        return update_cache()

# --- テスト用コード ---
if __name__ == "__main__":
    print("Cache Manager テスト実行...")

    # 1. キャッシュ取得（初回または期限切れの場合、更新が走るはず）
    print("\n--- 1. キャッシュ取得/更新 ---")
    data = get_cached_data()
    if data:
        print(f"キャッシュ取得成功。最終更新: {data.last_updated}")
        print(f"データセット数: {sum(len(ds_list) for ds_list in data.datasets.values())}")
        print(f"テーブル数: {sum(len(tbl_list) for ds_dict in data.tables.values() for tbl_list in ds_dict.values())}")
    else:
        print("キャッシュの取得/更新に失敗しました。")

    # 2. 再度キャッシュ取得（TTL内であれば更新は走らないはず）
    print("\n--- 2. 再度キャッシュ取得 (TTL内) ---")
    data_again = get_cached_data()
    if data_again:
        print(f"キャッシュ取得成功。最終更新: {data_again.last_updated}")
    else:
        print("キャッシュの取得に失敗しました。")

    # 3. キャッシュの手動更新
    print("\n--- 3. キャッシュの手動更新 ---")
    updated_data = update_cache()
    if updated_data:
         print(f"キャッシュの手動更新成功。最終更新: {updated_data.last_updated}")
    else:
         print("キャッシュの手動更新に失敗しました。")