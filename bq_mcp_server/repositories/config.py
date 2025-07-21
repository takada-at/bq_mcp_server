# config.py: Manages application configuration settings
import fnmatch
import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv

from bq_mcp_server.core.entities import Settings
from bq_mcp_server.repositories import log

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


def _load_env_variable(key: str, default=None, cast_func=None):
    """Load environment variable with optional type casting"""
    value = os.getenv(key, default)
    if cast_func and value is not None:
        try:
            return cast_func(value)
        except (ValueError, TypeError):
            return default
    return value


def _parse_filter_list(filters_str: str) -> List[str]:
    """Parse comma-separated filter string into list"""
    if not filters_str:
        return []
    return [f.strip() for f in filters_str.split(",") if f.strip()]


def _validate_settings(settings: Settings) -> None:
    """Validate settings and log warnings for issues"""
    logger = log.get_logger()

    if not settings.project_ids:
        logger.warning(
            "Warning: Environment variable 'PROJECT_IDS' is not set. Please specify GCP project IDs separated by commas."
        )

    if settings.gcp_service_account_key_path:
        if not os.path.exists(settings.gcp_service_account_key_path):
            logger.warning(
                f"Warning: The specified GCP service account key file was not found: {settings.gcp_service_account_key_path}"
            )
    else:
        logger.warning(
            "Info: GCP_SERVICE_ACCOUNT_KEY_PATH is not set. The application will try to use default authentication credentials (ADC)."
        )


def init_setting() -> Settings:
    """Initialize settings from environment variables"""
    # Load environment variables
    gcp_service_account_key_path = _load_env_variable("GCP_SERVICE_ACCOUNT_KEY_PATH")
    project_ids = [
        val
        for val in _load_env_variable("PROJECT_IDS", "").split(",")
        if val is not None and val.strip() != ""
    ]
    dataset_filters = _parse_filter_list(_load_env_variable("DATASET_FILTERS", ""))
    cache_ttl_seconds = _load_env_variable("CACHE_TTL_SECONDS", 3600, int)
    cache_file_base_dir = os.path.abspath(
        _load_env_variable("CACHE_FILE_BASE_DIR", str(root / ".bq_metadata_cache"))
    )
    api_host = _load_env_variable("API_HOST", "127.0.0.1")
    api_port = _load_env_variable("API_PORT", 8000, int)

    # Query execution settings
    query_execution_project_id = _load_env_variable("QUERY_EXECUTION_PROJECT_ID")
    max_scan_bytes = _load_env_variable(
        "MAX_SCAN_BYTES", 1024 * 1024 * 1024, int
    )  # 1GB
    default_query_limit = _load_env_variable("DEFAULT_QUERY_LIMIT", 10, int)
    query_timeout_seconds = _load_env_variable(
        "QUERY_TIMEOUT_SECONDS", 300, int
    )  # 5 minutes
    # Logging settings
    enable_file_logging = _load_env_variable("ENABLE_FILE_LOGGING", False, bool)

    settings = Settings(
        gcp_service_account_key_path=gcp_service_account_key_path,
        project_ids=project_ids,
        dataset_filters=dataset_filters,
        cache_ttl_seconds=cache_ttl_seconds,
        cache_file_base_dir=cache_file_base_dir,
        api_host=api_host,
        api_port=api_port,
        query_execution_project_id=query_execution_project_id,
        max_scan_bytes=max_scan_bytes,
        default_query_limit=default_query_limit,
        query_timeout_seconds=query_timeout_seconds,
        enable_file_logging=enable_file_logging,
    )

    _validate_settings(settings)
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
