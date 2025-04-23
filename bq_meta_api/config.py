# config.py: Manages application configuration settings
import os
from dotenv import load_dotenv
from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings
from typing import List, Optional

from bq_meta_api import log


# .env ファイルを読み込む
root = Path(__file__).parent.parent.resolve()
envpath = (root / ".env").resolve()

load_dotenv(str(envpath))
logger = log.logger


class Settings(BaseSettings):
    """アプリケーション設定を管理するクラス"""

    # GCP関連設定
    gcp_service_account_key_path: Optional[str] = os.getenv(
        "GCP_SERVICE_ACCOUNT_KEY_PATH"
    )
    project_ids: List[str] = [
        pid.strip() for pid in os.getenv("PROJECT_IDS", "").split(",") if pid.strip()
    ]

    # キャッシュ設定
    cache_ttl_seconds: int = int(
        os.getenv("CACHE_TTL_SECONDS", 3600)
    )  # デフォルト1時間
    cache_file_base_dir: str = os.getenv(
        "CACHE_FILE_BASE_DIR", str(root / ".bq_metadata_cache")
    )

    # APIサーバー設定 (uvicorn用)
    api_host: str = os.getenv("API_HOST", "127.0.0.1")
    api_port: int = int(os.getenv("API_PORT", 8000))

    class Config:
        # .env ファイルが存在する場合、そちらを優先する
        env_file = ".env"
        env_file_encoding = "utf-8"
        enable_decoding = False

    @field_validator("project_ids", mode="before")
    @classmethod
    def decode_project_ids(cls, v: str) -> list[int]:
        return [x.strip() for x in v.split(",")]


# 設定インスタンスを作成
settings = Settings()

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


# 設定値へのアクセス例
if __name__ == "__main__":
    logger.warning("--- Application Settings ---")
    logger.warning(
        f"GCP Service Account Key Path: {settings.gcp_service_account_key_path}"
    )
    logger.warning(f"Project IDs: {settings.project_ids}")
    logger.warning(f"Cache TTL (seconds): {settings.cache_ttl_seconds}")
    logger.warning(f"Cache File Base Dir: {settings.cache_file_base_dir}")
    logger.warning(f"API Host: {settings.api_host}")
    logger.warning(f"API Port: {settings.api_port}")
    logger.warning("--------------------------")
