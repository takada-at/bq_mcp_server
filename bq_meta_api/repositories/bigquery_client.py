# bigquery_client.py: Handles communication with Google BigQuery API
import aiohttp
from datetime import datetime, timezone
from typing import List, Optional
from gcloud.aio.bigquery import Dataset, Table
from gcloud.aio.auth.token import Token
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from bq_meta_api.repositories import config
from bq_meta_api.core.entities import (
    DatasetMetadata,
    TableMetadata,
    TableSchema,
    ColumnSchema,
)
from bq_meta_api.repositories import log


def get_bigquery_client() -> Optional[Dataset]:
    """
    Asynchronous BigQueryクライアントを初期化して返します。
    サービスアカウントキーが設定されていればそれを使用し、
    設定されていなければApplication Default Credentials (ADC) を試みます。

    Returns:
        Optional[BigQuery]: 初期化されたBigQueryクライアントとセッション。初期化に失敗した場合はNone。
    """
    logger = log.get_logger()
    settings = config.get_settings()
    session: Optional[aiohttp.ClientSession] = None
    try:
        session = aiohttp.ClientSession()
        project_to_use = settings.project_ids[0] if settings.project_ids else None
        if settings.gcp_service_account_key_path:
            logger.info(
                f"サービスアカウントキーを使用して認証します: {settings.gcp_service_account_key_path}"
            )
            token = Token(settings.gcp_service_account_key_path)
            dataset = Dataset(project=project_to_use, session=session, token=token)
        else:
            logger.info("Application Default Credentials (ADC) を使用して認証します。")
            token = Token()
            dataset = Dataset(project=project_to_use, session=session, token=token)
        logger.info(
            f"Async BigQueryクライアントの初期化準備ができました。デフォルトプロジェクト: {project_to_use}"
        )
        return dataset
    except FileNotFoundError:
        logger.error(
            f"サービスアカウントキーファイルが見つかりません: {settings.gcp_service_account_key_path}"
        )
        return None
    except DefaultCredentialsError as e:
        logger.exception(e)
        logger.error(
            "Application Default Credentials (ADC) が見つかりません。gcloud auth application-default login を実行するか、サービスアカウントキーを設定してください。"
        )
        return None
    except RefreshError as e:  # Might still be relevant for ADC
        logger.error(f"認証情報の更新に失敗しました: {e}")
        return None
    except Exception as e:
        logger.error(
            f"Async BigQueryクライアントの初期化中に予期せぬエラーが発生しました: {e}"
        )
        return None


async def fetch_datasets(client: Dataset, project_id: str) -> List[DatasetMetadata]:
    """指定されたプロジェクトのデータセット一覧を非同期で取得します。"""
    datasets_metadata = []
    logger = log.get_logger()
    datasets_list = await Dataset(
        project=project_id, session=client.session.session, token=client.token
    ).list_datasets()
    logger.info(
        f"プロジェクト '{project_id}' から {len(datasets_list)} 個のデータセット情報を取得しました。"
    )
    for dataset_data in datasets_list["datasets"]:
        # dataset_data is a dict. Example keys: 'kind', 'id', 'datasetReference', 'location'
        # 'id' is usually 'project:dataset'
        # 'datasetReference' is {'datasetId': '...', 'projectId': '...'}
        ds_ref = dataset_data.get("datasetReference", {})
        actual_project_id = ds_ref.get(
            "projectId", project_id
        )  # Prefer projectId from reference
        actual_dataset_id = ds_ref.get("datasetId")

        if not actual_dataset_id:
            logger.warning(
                f"データセットIDが見つからないためスキップします: {dataset_data}"
            )
            continue

        description = dataset_data.get("description")
        dataset = Dataset(
            dataset_name=actual_dataset_id,
            project=actual_project_id,
            session=client.session.session,
            token=client.token,
        )
        if description is None:
            full_dataset_details = await dataset.get(
                session=client.session
            )  # This will fetch the dataset details
            description = full_dataset_details.get("description")

        metadata = DatasetMetadata(
            project_id=actual_project_id,
            dataset_id=actual_dataset_id,
            description=description,
            location=dataset_data.get(
                "location"
            ),  # Location should be in list_datasets response
        )
        datasets_metadata.append(metadata)

    return datasets_metadata


def _parse_schema(schema_fields: List[dict]) -> List[ColumnSchema]:  # Changed type hint
    """BigQueryのスキーマフィールドリスト(dict形式)をColumnSchemaのリストに変換します。"""
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
                name=field_data.get("name"),
                type=field_type,
                mode=field_data.get(
                    "mode", "NULLABLE"
                ),  # Default to NULLABLE if not present
                description=field_data.get("description"),
                fields=nested_fields,
            )
        )
    return columns


async def fetch_tables_and_schemas(
    client: Dataset, project_id: str, dataset_id: str
) -> List[TableMetadata]:
    """指定されたデータセットのテーブル一覧と各テーブルのスキーマを取得します。"""
    logger = log.get_logger()
    tables_metadata = []
    dataset = Dataset(
        dataset_name=dataset_id,
        project=project_id,
        session=client.session.session,
        token=client.token,
    )
    # gcloud-aio-bigquery list_tables returns a list of dict-like objects
    # Each dict contains: kind, id, tableReference, type, friendlyName, labels
    tables_list = await dataset.list_tables()
    logger.info(
        f"データセット '{project_id}.{dataset_id}' から {len(tables_list)} 個のテーブル情報を取得しました。"
    )
    for table_item_data in tables_list["tables"]:
        # table_item_data is a dict
        # 'tableReference': {'projectId': 'p', 'datasetId': 'd', 'tableId': 't'}
        tbl_ref = table_item_data.get("tableReference", {})
        actual_project_id = tbl_ref.get("projectId", project_id)
        actual_dataset_id = tbl_ref.get("datasetId", dataset_id)
        actual_table_id = tbl_ref.get("tableId")

        if not actual_table_id:
            logger.warning(
                f"テーブルIDが見つからないためスキップします: {table_item_data}"
            )
            continue
        table = Table(
            dataset_name=actual_dataset_id,
            table_name=actual_table_id,
            project=actual_project_id,
            session=client.session.session,
            token=client.token,
        )
        full_table_id = f"{actual_project_id}.{actual_dataset_id}.{actual_table_id}"
        table_details = await table.get()
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
            project_id=actual_project_id,
            dataset_id=actual_dataset_id,
            table_id=actual_table_id,
            full_table_id=full_table_id,
            schema_=schema_model,
            description=table_details.get("description")
            or table_details.get("friendlyName"),
            num_rows=int(num_rows_val) if num_rows_val is not None else None,
            num_bytes=int(num_bytes_val) if num_bytes_val is not None else None,
            created_time=created_dt,
            last_modified_time=modified_dt,
        )
        tables_metadata.append(metadata)

    return tables_metadata


async def close_client(client: Dataset):
    """非同期クライアントを閉じます。"""
    if client and client.session:
        await client.session.session.close()
        await client.session.close()
        await client.token.close()
        log.get_logger().info("BigQueryクライアントを正常に閉じました。")
    else:
        log.get_logger().warning(
            "クライアントが初期化されていないか、セッションがありません。"
        )
    return None
