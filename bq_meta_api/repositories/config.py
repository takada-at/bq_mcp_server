# config.py: Manages application configuration settings
import os
from dotenv import load_dotenv
from pathlib import Path

from bq_meta_api.core.entities import Settings
from bq_meta_api.repositories import log


# .env ファイルを読み込む
root = Path(__file__).parent.parent.parent.resolve()
envpath = (root / ".env").resolve()

load_dotenv(str(envpath))
_settings = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = init_setting()
    return _settings


def init_setting() -> Settings:
    # 設定インスタンスを作成
    # 環境変数を読み込んで初期値作成
    gcp_service_account_key_path = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH", None)
    project_ids = os.getenv("PROJECT_IDS", "").split(",")
    cache_ttl_seconds = int(os.getenv("CACHE_TTL_SECONDS", 3600))
    cache_file_base_dir = os.getenv("CACHE_FILE_BASE_DIR", str(root / ".bq_metadata_cache"))
    cache_file_base_dir = os.path.abspath(cache_file_base_dir)
    api_host = os.getenv("API_HOST", "127.0.0.1")
    api_port = int(os.getenv("API_PORT", 8000))

    settings = Settings(
        gcp_service_account_key_path=gcp_service_account_key_path,
        project_ids=project_ids,
        cache_ttl_seconds=cache_ttl_seconds,
        cache_file_base_dir=cache_file_base_dir,
        api_host=api_host,
        api_port=api_port,
    )
    logger = log.get_logger()

    # --- 設定値の簡単なバリデーション ---
    if not settings.project_ids:
        logger.warning(
            "警告: 環境変数 'PROJECT_IDS' が設定されていません。カンマ区切りでGCPプロジェクトIDを指定してください。"
        )
        # 必要に応じてここで処理を停止させることも可能
        # raise ValueError("PROJECT_IDS is not set.")

    if settings.gcp_service_account_key_path and not os.path.exists(
        settings.gcp_service_account_key_path
    ):
        logger.warning(
            f"警告: 指定されたGCPサービスアカウントキーファイルが見つかりません: {settings.gcp_service_account_key_path}"
        )
        # 認証が必要な処理でエラーになるため、警告を表示
    elif not settings.gcp_service_account_key_path:
        logger.warning(
            "情報: GCP_SERVICE_ACCOUNT_KEY_PATH が設定されていません。アプリケーションはデフォルトの認証情報（ADC）を使用しようとします。"
        )
    return settings
