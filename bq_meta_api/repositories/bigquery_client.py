# bigquery_client.py: Handles communication with Google BigQuery API
import aiohttp
from datetime import datetime, timezone # Added for timestamp conversion
from typing import List, Optional, Tuple
from gcloud_aio_bigquery import BigQuery
from gcloud_aio_core.token_providers import ServiceAccountTokenProvider, TokenProvider
# from google.oauth2 import service_account # No longer needed, ServiceAccountTokenProvider takes path
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from bq_meta_api.repositories import config
from bq_meta_api.core.entities import (
    DatasetMetadata,
    TableMetadata,
    TableSchema,
    ColumnSchema,
)
from bq_meta_api.repositories import log


def get_bigquery_client() -> Optional[Tuple[BigQuery, aiohttp.ClientSession]]:
    """
    Asynchronous BigQueryクライアントとaiohttpセッションを初期化して返します。
    サービスアカウントキーが設定されていればそれを使用し、
    設定されていなければApplication Default Credentials (ADC) を試みます。

    Returns:
        Optional[Tuple[BigQuery, aiohttp.ClientSession]]: 初期化されたBigQueryクライアントとセッション。初期化に失敗した場合はNone。
    """
    logger = log.get_logger()
    settings = config.get_settings()
    session: Optional[aiohttp.ClientSession] = None
    try:
        session = aiohttp.ClientSession()
        token_provider: Optional[TokenProvider] = None
        project_to_use = settings.project_ids[0] if settings.project_ids else None

        if settings.gcp_service_account_key_path:
            logger.info(
                f"サービスアカウントキーを使用して認証します: {settings.gcp_service_account_key_path}"
            )
            # Note: gcloud-aio-bigquery expects the SA path directly or TokenProvider
            # Using ServiceAccountTokenProvider for explicit control over scopes
            token_provider = ServiceAccountTokenProvider(
                sa_file=settings.gcp_service_account_key_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            # For gcloud-aio, project_id is often passed to the client methods,
            # but can also be set at client instantiation for a default.
            # Let's ensure project_to_use is set if we have a key.
            # The SA key file itself might contain a project_id, which TokenProvider might pick up.
            # However, explicit project_id in BigQuery client is safer.
            client = BigQuery(project=project_to_use, token_provider=token_provider, session=session)
        else:
            logger.info("Application Default Credentials (ADC) を使用して認証します。")
            # ADC should be picked up automatically by gcloud-aio libraries if GOOGLE_APPLICATION_CREDENTIALS env var is set
            # or if running in a GCP environment.
            # Pass project_id if available, otherwise operations might require it per call.
            client = BigQuery(project=project_to_use, session=session)

        # No simple connection test like client.list_projects() available without async call.
        # We'll assume success if instantiation doesn't throw immediate errors.
        # Actual connection test will happen on first API call.
        logger.info(
            f"Async BigQueryクライアントの初期化準備ができました。デフォルトプロジェクト: {project_to_use}"
        )
        return client, session
    except FileNotFoundError:
        logger.error(
            f"サービスアカウントキーファイルが見つかりません: {settings.gcp_service_account_key_path}"
        )
        if session:
            # asyncio.run(session.close()) # Cannot run async here
            logger.warning("aiohttp.ClientSession was opened but not closed due to FileNotFoundError.")
        return None
    except DefaultCredentialsError as e:
        logger.exception(e)
        logger.error(
            "Application Default Credentials (ADC) が見つかりません。gcloud auth application-default login を実行するか、サービスアカウントキーを設定してください。"
        )
        if session:
            logger.warning("aiohttp.ClientSession was opened but not closed due to DefaultCredentialsError.")
        return None
    except RefreshError as e: # Might still be relevant for ADC
        logger.error(f"認証情報の更新に失敗しました: {e}")
        if session:
            logger.warning("aiohttp.ClientSession was opened but not closed due to RefreshError.")
        return None
    except Exception as e:
        logger.error(
            f"Async BigQueryクライアントの初期化中に予期せぬエラーが発生しました: {e}"
        )
        if session:
            # Consider how to close session if it's still open
            # For now, just log it. Proper cleanup should happen in the caller.
            logger.warning("aiohttp.ClientSession was opened but not closed due to an unexpected error.")
        return None


async def fetch_datasets(client: BigQuery, project_id: str) -> List[DatasetMetadata]:
    """指定されたプロジェクトのデータセット一覧を非同期で取得します。"""
    datasets_metadata = []
    logger = log.get_logger()
    try:
        # gcloud-aio-bigquery's list_datasets returns a list of dataset objects directly
        # These objects are dict-like.
        datasets_list = await client.list_datasets(project_id=project_id)
        logger.info(
            f"プロジェクト '{project_id}' から {len(datasets_list)} 個のデータセット情報を取得しました。"
        )
        for dataset_data in datasets_list:
            # dataset_data is a dict. Example keys: 'kind', 'id', 'datasetReference', 'location'
            # 'id' is usually 'project:dataset'
            # 'datasetReference' is {'datasetId': '...', 'projectId': '...'}
            ds_ref = dataset_data.get("datasetReference", {})
            actual_project_id = ds_ref.get("projectId", project_id) # Prefer projectId from reference
            actual_dataset_id = ds_ref.get("datasetId")

            if not actual_dataset_id:
                logger.warning(f"データセットIDが見つからないためスキップします: {dataset_data}")
                continue

            # For description, gcloud-aio-bigquery might require a separate get_dataset call
            # Let's check if 'description' or 'friendlyName' is available directly.
            # Based on docs, list_datasets provides: kind, id, datasetReference, friendlyName, labels, location
            # Description is not in the list response. We need to fetch it.
            description = dataset_data.get("description") # Unlikely to be here
            if description is None:
                try:
                    # Fetch full dataset details for description
                    # Ensure client is passed correctly if this function is used outside a class context
                    full_dataset_details = await client.get_dataset_details(
                        project_id=actual_project_id, dataset_id=actual_dataset_id
                    )
                    description = full_dataset_details.get("description")
                except Exception as e_detail:
                    logger.warning(
                        f"データセット '{actual_project_id}.{actual_dataset_id}' の詳細説明取得中にエラー: {e_detail}"
                    )
            
            metadata = DatasetMetadata(
                project_id=actual_project_id,
                dataset_id=actual_dataset_id,
                description=description,
                location=dataset_data.get("location"), # Location should be in list_datasets response
            )
            datasets_metadata.append(metadata)

    except Exception as e:
        logger.error(f"プロジェクト '{project_id}' のデータセット取得中に非同期エラー: {e}")
    return datasets_metadata


def _parse_schema(schema_fields: List[dict]) -> List[ColumnSchema]: # Changed type hint
    """BigQueryのスキーマフィールドリスト(dict形式)をColumnSchemaのリストに変換します。"""
    columns = []
    for field_data in schema_fields: # field_data is a dict
        nested_fields = None
        # gcloud-aio-bigquery schema field structure:
        # {'name': '...', 'type': 'STRING', 'mode': 'NULLABLE', 'description': '...', 'fields': [...] }
        field_type = field_data.get("type", "UNKNOWN")
        if field_type == "RECORD" or field_type == "STRUCT":
            if field_data.get("fields"):
                nested_fields = _parse_schema(field_data["fields"])

        columns.append(
            ColumnSchema(
                name=field_data.get("name"),
                type=field_type,
                mode=field_data.get("mode", "NULLABLE"), # Default to NULLABLE if not present
                description=field_data.get("description"),
                fields=nested_fields,
            )
        )
    return columns


async def fetch_tables_and_schemas(
    client: BigQuery, project_id: str, dataset_id: str
) -> List[TableMetadata]:
    """指定されたデータセットのテーブル一覧と各テーブルのスキーマを取得します。"""
    logger = log.get_logger()
    tables_metadata = []
    try:
        # gcloud-aio-bigquery list_tables returns a list of dict-like objects
        # Each dict contains: kind, id, tableReference, type, friendlyName, labels
        tables_list = await client.list_tables(dataset_id=dataset_id, project_id=project_id)
        logger.info(
            f"データセット '{project_id}.{dataset_id}' から {len(tables_list)} 個のテーブル情報を取得しました。"
        )
        for table_item_data in tables_list:
            # table_item_data is a dict
            # 'tableReference': {'projectId': 'p', 'datasetId': 'd', 'tableId': 't'}
            tbl_ref = table_item_data.get("tableReference", {})
            actual_project_id = tbl_ref.get("projectId", project_id)
            actual_dataset_id = tbl_ref.get("datasetId", dataset_id)
            actual_table_id = tbl_ref.get("tableId")

            if not actual_table_id:
                logger.warning(f"テーブルIDが見つからないためスキップします: {table_item_data}")
                continue

            full_table_id = f"{actual_project_id}.{actual_dataset_id}.{actual_table_id}"
            try:
                # Fetch full table details for schema and other metadata
                table_details = await client.get_table(
                    project_id=actual_project_id,
                    dataset_id=actual_dataset_id,
                    table_id=actual_table_id,
                )

                schema_model = None
                # Schema is in table_details['schema']['fields']
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
                        created_dt = datetime.fromtimestamp(int(created_time_ms_str) / 1000, tz=timezone.utc)
                    except (ValueError, TypeError) as e_ts:
                        logger.warning(f"Could not parse creationTime '{created_time_ms_str}' for table {full_table_id}: {e_ts}")
                
                modified_dt = None
                if last_modified_time_ms_str:
                    try:
                        modified_dt = datetime.fromtimestamp(int(last_modified_time_ms_str) / 1000, tz=timezone.utc)
                    except (ValueError, TypeError) as e_ts:
                        logger.warning(f"Could not parse lastModifiedTime '{last_modified_time_ms_str}' for table {full_table_id}: {e_ts}")

                num_rows_val = table_details.get("numRows")
                num_bytes_val = table_details.get("numBytes")

                metadata = TableMetadata(
                    project_id=actual_project_id,
                    dataset_id=actual_dataset_id,
                    table_id=actual_table_id,
                    full_table_id=full_table_id,
                    schema_=schema_model,
                    description=table_details.get("description") or table_details.get("friendlyName"),
                    num_rows=int(num_rows_val) if num_rows_val is not None else None,
                    num_bytes=int(num_bytes_val) if num_bytes_val is not None else None,
                    created_time=created_dt,
                    last_modified_time=modified_dt,
                )
                tables_metadata.append(metadata)
            except Exception as e:
                logger.warning(f"テーブル '{full_table_id}' の詳細取得中に非同期エラー: {e}")
                metadata = TableMetadata( # Basic info on error
                    project_id=actual_project_id,
                    dataset_id=actual_dataset_id,
                    table_id=actual_table_id,
                    full_table_id=full_table_id,
                )
                tables_metadata.append(metadata)

    except Exception as e:
        logger.error(
            f"データセット '{project_id}.{dataset_id}' のテーブル取得中に非同期エラー: {e}"
        )
    return tables_metadata
