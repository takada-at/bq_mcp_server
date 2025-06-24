# config.py: Manages application configuration settings
import os
import fnmatch
from dotenv import load_dotenv
from pathlib import Path
from typing import List

from bq_mcp.core.entities import Settings
from bq_mcp.repositories import log


# Load .env file
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
    # Create settings instance
    # Load environment variables and create initial values
    gcp_service_account_key_path = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH", None)
    project_ids = os.getenv("PROJECT_IDS", "").split(",")
    dataset_filters_str = os.getenv("DATASET_FILTERS", "")
    dataset_filters = [f.strip() for f in dataset_filters_str.split(",") if f.strip()]
    cache_ttl_seconds = int(os.getenv("CACHE_TTL_SECONDS", 3600))
    cache_file_base_dir = os.getenv(
        "CACHE_FILE_BASE_DIR", str(root / ".bq_metadata_cache")
    )
    cache_file_base_dir = os.path.abspath(cache_file_base_dir)
    api_host = os.getenv("API_HOST", "127.0.0.1")
    api_port = int(os.getenv("API_PORT", 8000))

    # Query execution settings
    max_scan_bytes = int(os.getenv("MAX_SCAN_BYTES", 1024 * 1024 * 1024))  # 1GB
    default_query_limit = int(os.getenv("DEFAULT_QUERY_LIMIT", 10))
    query_timeout_seconds = int(os.getenv("QUERY_TIMEOUT_SECONDS", 300))  # 5 minutes

    settings = Settings(
        gcp_service_account_key_path=gcp_service_account_key_path,
        project_ids=project_ids,
        dataset_filters=dataset_filters,
        cache_ttl_seconds=cache_ttl_seconds,
        cache_file_base_dir=cache_file_base_dir,
        api_host=api_host,
        api_port=api_port,
        max_scan_bytes=max_scan_bytes,
        default_query_limit=default_query_limit,
        query_timeout_seconds=query_timeout_seconds,
    )
    logger = log.get_logger()

    # --- Simple validation of configuration values ---
    if not settings.project_ids:
        logger.warning(
            "Warning: Environment variable 'PROJECT_IDS' is not set. Please specify GCP project IDs separated by commas."
        )
        # If needed, processing can be stopped here
        # raise ValueError("PROJECT_IDS is not set.")

    if settings.gcp_service_account_key_path and not os.path.exists(
        settings.gcp_service_account_key_path
    ):
        logger.warning(
            f"Warning: The specified GCP service account key file was not found: {settings.gcp_service_account_key_path}"
        )
        # Display warning as authentication will error in processes that require it
    elif not settings.gcp_service_account_key_path:
        logger.warning(
            "Info: GCP_SERVICE_ACCOUNT_KEY_PATH is not set. The application will try to use default authentication credentials (ADC)."
        )
    return settings


def should_include_dataset(
    project_id: str, dataset_id: str, filters: List[str]
) -> bool:
    """
    Determines whether a dataset matches filter conditions.

    Args:
        project_id: Project ID
        dataset_id: Dataset ID
        filters: List of filter conditions (e.g., ["pj1.*", "pj2.dataset1"])

    Returns:
        True if matches filter conditions, False if not
        Always True if filters are empty (include all)
    """
    if not filters:
        return True

    full_dataset_name = f"{project_id}.{dataset_id}"

    for filter_pattern in filters:
        # Evaluate filter pattern with fnmatch
        if fnmatch.fnmatch(full_dataset_name, filter_pattern):
            return True

    return False
