# bigquery_client.py: Handles communication with Google BigQuery API
from typing import List, Optional
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from bq_meta_api import config, log
from bq_meta_api.models import DatasetMetadata, TableMetadata, TableSchema, ColumnSchema


def get_bigquery_client() -> Optional[bigquery.Client]:
    """
    BigQueryクライアントを初期化して返します。
    サービスアカウントキーが設定されていればそれを使用し、
    設定されていなければApplication Default Credentials (ADC) を試みます。

    Returns:
        bigquery.Client: 初期化されたBigQueryクライアント。初期化に失敗した場合はNone。
    """
    logger = log.get_logger()
    settings = config.get_settings()
    try:
        if settings.gcp_service_account_key_path:
            logger.info(
                f"サービスアカウントキーを使用して認証します: {settings.gcp_service_account_key_path}"
            )
            credentials = service_account.Credentials.from_service_account_file(
                settings.gcp_service_account_key_path,
                scopes=[
                    "https://www.googleapis.com/auth/cloud-platform"
                ],  # 必要なスコープを指定
            )
            # credentials.project_id を使うとキーファイル内のプロジェクトIDに固定されるため、
            # client生成時に project を指定しないことで、リクエストごとにプロジェクトを指定できるようにする
            client = bigquery.Client(credentials=credentials)
        else:
            logger.info("Application Default Credentials (ADC) を使用して認証します。")
            # ADCの場合、デフォルトプロジェクトが設定される可能性があるが、
            # list_datasets などで project パラメータを指定すればオーバーライド可能
            client = bigquery.Client(
                project=settings.project_ids[0]
            )  # 最初のプロジェクトIDを使用

        # 簡単な接続テスト（例：プロジェクトリスト取得など、軽い操作）
        # client.list_projects(max_results=1) # これは権限が必要な場合がある
        logger.info(
            f"BigQueryクライアントの初期化に成功しました。デフォルトプロジェクト: {client.project}"
        )
        return client
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
    except RefreshError as e:
        logger.error(f"認証情報の更新に失敗しました: {e}")
        return None
    except Exception as e:
        logger.error(
            f"BigQueryクライアントの初期化中に予期せぬエラーが発生しました: {e}"
        )
        return None


def fetch_datasets(client: bigquery.Client, project_id: str) -> List[DatasetMetadata]:
    """指定されたプロジェクトのデータセット一覧を取得します。"""
    datasets_metadata = []
    logger = log.get_logger()
    try:
        datasets = list(client.list_datasets(project=project_id))  # プロジェクトを指定
        logger.info(
            f"プロジェクト '{project_id}' から {len(datasets)} 個のデータセットを取得しました。"
        )
        for dataset in datasets:
            # データセットの詳細情報を取得（説明など）
            try:
                dataset_ref = client.get_dataset(dataset.reference)
                metadata = DatasetMetadata(
                    project_id=dataset_ref.project,
                    dataset_id=dataset_ref.dataset_id,
                    description=dataset_ref.description,
                    location=dataset_ref.location,
                )
                datasets_metadata.append(metadata)
            except Exception as e:
                logger.warning(
                    f"データセット '{project_id}.{dataset.dataset_id}' の詳細取得中にエラー: {e}"
                )
                # 詳細が取れなくても基本的な情報は追加
                metadata = DatasetMetadata(
                    project_id=project_id,
                    dataset_id=dataset.dataset_id,
                )
                datasets_metadata.append(metadata)

    except Exception as e:
        logger.error(f"プロジェクト '{project_id}' のデータセット取得中にエラー: {e}")
    return datasets_metadata


def _parse_schema(schema_fields: List[bigquery.SchemaField]) -> List[ColumnSchema]:
    """BigQueryのスキーマフィールドリストをColumnSchemaのリストに変換します。"""
    columns = []
    for field in schema_fields:
        nested_fields = None
        if field.field_type == "RECORD" or field.field_type == "STRUCT":
            # ネストされたフィールドがある場合、再帰的に処理
            if field.fields:
                nested_fields = _parse_schema(field.fields)

        columns.append(
            ColumnSchema(
                name=field.name,
                type=field.field_type,
                mode=field.mode,
                description=field.description,
                fields=nested_fields,
            )
        )
    return columns


def fetch_tables_and_schemas(
    client: bigquery.Client, project_id: str, dataset_id: str
) -> List[TableMetadata]:
    """指定されたデータセットのテーブル一覧と各テーブルのスキーマを取得します。"""
    logger = log.get_logger()
    tables_metadata = []
    dataset_ref = client.dataset(dataset_id, project=project_id)
    try:
        tables = list(client.list_tables(dataset_ref))
        logger.info(
            f"データセット '{project_id}.{dataset_id}' から {len(tables)} 個のテーブルを取得しました。"
        )
        for table_item in tables:
            full_table_id = f"{project_id}.{dataset_id}.{table_item.table_id}"
            try:
                table_ref = dataset_ref.table(table_item.table_id)
                table = client.get_table(table_ref)

                schema_model = None
                if table.schema:
                    parsed_columns = _parse_schema(table.schema)
                    schema_model = TableSchema(columns=parsed_columns)

                metadata = TableMetadata(
                    project_id=table.project,
                    dataset_id=table.dataset_id,
                    table_id=table.table_id,
                    full_table_id=full_table_id,
                    schema_=schema_model,
                    description=table.description,
                    num_rows=table.num_rows,
                    num_bytes=table.num_bytes,
                    created_time=table.created,
                    last_modified_time=table.modified,
                )
                tables_metadata.append(metadata)
            except Exception as e:
                logger.warning(f"テーブル '{full_table_id}' の詳細取得中にエラー: {e}")
                # 詳細が取れなくても基本的な情報は追加（スキーマなし）
                metadata = TableMetadata(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=table_item.table_id,
                    full_table_id=full_table_id,
                )
                tables_metadata.append(metadata)

    except Exception as e:
        logger.error(
            f"データセット '{project_id}.{dataset_id}' のテーブル取得中にエラー: {e}"
        )
    return tables_metadata
