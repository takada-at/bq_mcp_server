# bigquery_client.py: Handles communication with Google BigQuery API
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp
from gcloud.aio.auth.token import Token
from gcloud.aio.bigquery import Dataset, Table
from google.auth.exceptions import DefaultCredentialsError, RefreshError

from bq_mcp_server.core.async_funcs import gather_in_batches
from bq_mcp_server.core.entities import (
    ColumnSchema,
    DatasetMetadata,
    TableMetadata,
    TableSchema,
)
from bq_mcp_server.repositories import config, log
from bq_mcp_server.repositories.config import should_include_dataset


def get_bigquery_client() -> Optional[Dataset]:
    """
    Initialize and return an asynchronous BigQuery client.
    Use service account key if configured,
    otherwise try Application Default Credentials (ADC).

    Returns:
        Optional[BigQuery]: Initialized BigQuery client and session. None if initialization fails.
    """
    logger = log.get_logger()
    settings = config.get_settings()
    session: Optional[aiohttp.ClientSession] = None
    try:
        session = aiohttp.ClientSession()
        project_to_use = settings.project_ids[0] if settings.project_ids else None
        if settings.gcp_service_account_key_path:
            logger.info(
                f"Authenticating using service account key: {settings.gcp_service_account_key_path}"
            )
            token = Token(settings.gcp_service_account_key_path)
            dataset = Dataset(project=project_to_use, session=session, token=token)  # type: ignore
        else:
            logger.info("Authenticating using Application Default Credentials (ADC).")
            token = Token()
            dataset = Dataset(project=project_to_use, session=session, token=token)  # type: ignore
        logger.info(
            f"Async BigQuery client initialization ready. Default project: {project_to_use}"
        )
        return dataset
    except FileNotFoundError:
        logger.error(
            f"Service account key file not found: {settings.gcp_service_account_key_path}"
        )
        return None
    except DefaultCredentialsError as e:
        logger.exception(e)
        logger.error(
            "Application Default Credentials (ADC) not found. Please run gcloud auth application-default login or configure service account key."
        )
        return None
    except RefreshError as e:  # Might still be relevant for ADC
        logger.error(f"Failed to refresh credentials: {e}")
        return None
    except Exception as e:
        logger.error(
            f"Unexpected error occurred during async BigQuery client initialization: {e}"
        )
        return None


async def _paginate_bigquery_api(
    api_call: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
    items_key: str,
    next_page_token_key: str = "nextPageToken",
    operation_name: str = "API call",
) -> List[Dict[str, Any]]:
    """
    Common function for BigQuery API pagination processing.

    Args:
        api_call: Function to call API (receives params and returns response)
        items_key: Key name of item array in response
        next_page_token_key: Key name of next page token
        operation_name: Operation name for logging

    Returns:
        List of items from all pages
    """
    logger = log.get_logger()
    all_items = []
    page_token = None
    page_count = 0

    while True:
        page_count += 1
        params = {"maxResults": 1000}
        if page_token:
            params["pageToken"] = page_token

        logger.debug(f"{operation_name} (page {page_count})")

        response = await api_call(params)
        items = response.get(items_key, [])
        all_items.extend(items)

        # Check for next page
        page_token = response.get(next_page_token_key)
        if not page_token:
            break

    logger.info(
        f"{operation_name} completed: retrieved {len(all_items)} items in {page_count} pages"
    )
    return all_items


async def get_dataset_detail(
    client: Dataset, project_id: str, dataset_id: str
) -> Optional[DatasetMetadata]:
    """Asynchronously retrieve details of specified project and dataset ID."""
    logger = log.get_logger()
    try:
        dataset = Dataset(
            dataset_name=dataset_id,
            project=project_id,
            session=client.session.session,  # type: ignore
            token=client.token,
        )
        dataset_details = await dataset.get(session=client.session)  # type: ignore
        if not dataset_details:
            logger.warning(f"Dataset {project_id}.{dataset_id} not found.")
            return None

        ds_ref = dataset_details.get("datasetReference", {})
        actual_project_id = ds_ref.get("projectId", project_id)
        actual_dataset_id = ds_ref.get("datasetId")

        if not actual_dataset_id:
            logger.warning(f"Skipping because dataset ID not found: {dataset_details}")
            return None

        description = dataset_details.get("description")
        metadata = DatasetMetadata(
            project_id=actual_project_id,
            dataset_id=actual_dataset_id,
            description=description,
            location=dataset_details.get("location"),
        )
        return metadata
    except Exception as e:
        logger.error(f"Error occurred while retrieving dataset: {e}")
        return None


async def fetch_datasets(client: Dataset, project_id: str) -> List[DatasetMetadata]:
    """Asynchronously retrieve list of datasets for specified project. Supports pagination."""
    logger = log.get_logger()
    settings = config.get_settings()

    async def list_datasets_api(params: Dict[str, Any]) -> Dict[str, Any]:
        """Internal function to call dataset list API"""
        return await Dataset(
            project=project_id,
            session=client.session.session,  # type: ignore
            token=client.token,  # type: ignore
        ).list_datasets(params=params)

    # Execute pagination processing with common function
    datasets_list = await _paginate_bigquery_api(
        api_call=list_datasets_api,
        items_key="datasets",
        next_page_token_key="nextPageToken",
        operation_name=f"Dataset list retrieval (project: {project_id})",
    )

    # Filter datasets and collect info
    datasets_info = []

    for dataset_data in datasets_list:
        # dataset_data is a dict. Example keys: 'kind', 'id', 'datasetReference', 'location'
        # 'id' is usually 'project:dataset'
        # 'datasetReference' is {'datasetId': '...', 'projectId': '...'}
        ds_ref = dataset_data.get("datasetReference", {})
        actual_project_id = ds_ref.get(
            "projectId", project_id
        )  # Prefer projectId from reference
        actual_dataset_id = ds_ref.get("datasetId")

        if not actual_dataset_id:
            logger.warning(f"Skipping because dataset ID not found: {dataset_data}")
            continue

        # Check dataset filter before proceeding with expensive operations
        if settings.dataset_filters:
            if not should_include_dataset(
                actual_project_id, actual_dataset_id, settings.dataset_filters
            ):
                logger.debug(
                    f"Skipping dataset {actual_project_id}.{actual_dataset_id} due to filter"
                )
                continue

        # Store dataset info for processing
        dataset_info = {
            "project_id": actual_project_id,
            "dataset_id": actual_dataset_id,
            "location": dataset_data.get("location"),
        }
        datasets_info.append(dataset_info)

    # Create coroutines for fetching dataset details
    fetch_tasks = []
    for dataset_info in datasets_info:
        dataset = Dataset(
            dataset_name=dataset_info["dataset_id"],
            project=dataset_info["project_id"],
            session=client.session.session,  # type: ignore
            token=client.token,
        )
        fetch_tasks.append(dataset.get(session=client.session))  # type: ignore

    # Execute fetches in batches of 3
    fetch_results = []
    if fetch_tasks:
        fetch_results = await gather_in_batches(fetch_tasks, batch_size=3)

    # Create metadata with fetched descriptions
    datasets_metadata = []
    for i, dataset_info in enumerate(datasets_info):
        description = None
        if i < len(fetch_results):
            description = fetch_results[i].get("description")

        metadata = DatasetMetadata(
            project_id=dataset_info["project_id"],
            dataset_id=dataset_info["dataset_id"],
            description=description,
            location=dataset_info["location"],
        )
        datasets_metadata.append(metadata)

    return datasets_metadata


def _parse_schema(schema_fields: List[dict]) -> List[ColumnSchema]:  # Changed type hint
    """Convert BigQuery schema field list (dict format) to list of ColumnSchema."""
    columns = []
    for field_data in schema_fields:  # field_data is a dict
        nested_fields = None
        # gcloud-aio-bigquery schema field structure:
        # {'name': '...', 'type': 'STRING', 'mode': 'NULLABLE', 'description': '...', 'fields': [...] }
        field_type = field_data.get("type", "UNKNOWN")
        if field_type == "RECORD" or field_type == "STRUCT":
            if field_data.get("fields"):
                nested_fields = _parse_schema(field_data["fields"])

        columns.append(
            ColumnSchema(
                name=field_data.get("name") or "",
                type=field_type,
                mode=field_data.get(
                    "mode", "NULLABLE"
                ),  # Default to NULLABLE if not present
                description=field_data.get("description"),
                fields=nested_fields,
            )
        )
    return columns


def _create_table_metadata(
    table_info: Dict[str, str], table_details: Dict[str, Any]
) -> TableMetadata:
    """Create TableMetadata from table info and details.

    Args:
        table_info: Dictionary containing project_id, dataset_id, table_id, full_table_id
        table_details: Raw table details from BigQuery API

    Returns:
        TableMetadata object
    """
    logger = log.get_logger()
    full_table_id = table_info["full_table_id"]

    # Parse schema
    schema_model = None
    bq_schema_fields = table_details.get("schema", {}).get("fields")
    if bq_schema_fields:
        parsed_columns = _parse_schema(bq_schema_fields)
        schema_model = TableSchema(columns=parsed_columns)

    # Convert time values (milliseconds since epoch string) to datetime objects
    created_time_ms_str = table_details.get("creationTime")
    last_modified_time_ms_str = table_details.get("lastModifiedTime")

    created_dt = None
    if created_time_ms_str:
        try:
            created_dt = datetime.fromtimestamp(
                int(created_time_ms_str) / 1000, tz=timezone.utc
            )
        except (ValueError, TypeError) as e_ts:
            logger.warning(
                f"Could not parse creationTime '{created_time_ms_str}' for table {full_table_id}: {e_ts}"
            )

    modified_dt = None
    if last_modified_time_ms_str:
        try:
            modified_dt = datetime.fromtimestamp(
                int(last_modified_time_ms_str) / 1000, tz=timezone.utc
            )
        except (ValueError, TypeError) as e_ts:
            logger.warning(
                f"Could not parse lastModifiedTime '{last_modified_time_ms_str}' for table {full_table_id}: {e_ts}"
            )

    num_rows_val = table_details.get("numRows")
    num_bytes_val = table_details.get("numBytes")

    metadata = TableMetadata(
        project_id=table_info["project_id"],
        dataset_id=table_info["dataset_id"],
        table_id=table_info["table_id"],
        full_table_id=full_table_id,
        schema_=schema_model,
        description=table_details.get("description")
        or table_details.get("friendlyName"),
        num_rows=int(num_rows_val) if num_rows_val is not None else None,
        num_bytes=int(num_bytes_val) if num_bytes_val is not None else None,
        created_time=created_dt,
        last_modified_time=modified_dt,
    )

    return metadata


async def fetch_tables_and_schemas(
    client: Dataset, project_id: str, dataset_id: str
) -> List[TableMetadata]:
    """Retrieve table list and schema for each table in specified dataset. Supports pagination."""
    logger = log.get_logger()
    dataset = Dataset(
        dataset_name=dataset_id,
        project=project_id,
        session=client.session.session,  # type: ignore
        token=client.token,
    )

    async def list_tables_api(params: Dict[str, Any]) -> Dict[str, Any]:
        """Internal function to call table list API"""
        return await dataset.list_tables(params=params)

    # Execute pagination processing with common function (tables.list uses 'pageToken')
    tables_list = await _paginate_bigquery_api(
        api_call=list_tables_api,
        items_key="tables",
        next_page_token_key="pageToken",  # tables.list uses 'pageToken'
        operation_name=f"Table list retrieval (dataset: {project_id}.{dataset_id})",
    )

    # Filter and collect table information
    tables_info = []

    for table_item_data in tables_list:
        # table_item_data is a dict
        # 'tableReference': {'projectId': 'p', 'datasetId': 'd', 'tableId': 't'}
        tbl_ref = table_item_data.get("tableReference", {})
        actual_project_id = tbl_ref.get("projectId", project_id)
        actual_dataset_id = tbl_ref.get("datasetId", dataset_id)
        actual_table_id = tbl_ref.get("tableId")

        if not actual_table_id:
            logger.warning(f"Skipping because table ID not found: {table_item_data}")
            continue

        table_info = {
            "project_id": actual_project_id,
            "dataset_id": actual_dataset_id,
            "table_id": actual_table_id,
            "full_table_id": f"{actual_project_id}.{actual_dataset_id}.{actual_table_id}",
        }
        tables_info.append(table_info)

    # Create coroutines for fetching table details
    fetch_tasks = []
    for table_info in tables_info:
        table = Table(
            dataset_name=table_info["dataset_id"],
            table_name=table_info["table_id"],
            project=table_info["project_id"],
            session=client.session.session,  # type: ignore
            token=client.token,
        )
        fetch_tasks.append(table.get())

    # Execute fetches in batches of 3
    fetch_results = []
    if fetch_tasks:
        fetch_results = await gather_in_batches(fetch_tasks, batch_size=3)

    # Process results and create metadata
    tables_metadata = []
    for i, table_info in enumerate(tables_info):
        if i >= len(fetch_results):
            continue

        table_details = fetch_results[i]
        metadata = _create_table_metadata(table_info, table_details)
        tables_metadata.append(metadata)

    return tables_metadata


async def close_client(client: Dataset):
    """Close asynchronous client."""
    if client and client.session:
        await client.session.session.close()
        await client.session.close()
        await client.token.close()
        log.get_logger().info("BigQuery client closed successfully.")
    else:
        log.get_logger().warning("Client not initialized or session not available.")
    return None
