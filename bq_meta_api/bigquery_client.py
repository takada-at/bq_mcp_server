# bigquery_client.py: Handles communication with Google BigQuery API
import logging
from typing import List, Optional, Tuple
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from bq_meta_api import log
from bq_meta_api.config import settings
from bq_meta_api.models import DatasetMetadata, TableMetadata, TableSchema, ColumnSchema


logger = log.logger


def get_bigquery_client() -> Optional[bigquery.Client]:
    """
    BigQueryクライアントを初期化して返します。
    サービスアカウントキーが設定されていればそれを使用し、
    設定されていなければApplication Default Credentials (ADC) を試みます。

    Returns:
        bigquery.Client: 初期化されたBigQueryクライアント。初期化に失敗した場合はNone。
    """
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
                    schema=schema_model,  # schema_のエイリアス
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


# --- テスト用コード ---
if __name__ == "__main__":
    logger.info("BigQuery Client テスト実行...")
    client = get_bigquery_client()
    if client and settings.project_ids:
        test_project_id = settings.project_ids[0]
        logger.info(f"\n--- データセット一覧 ({test_project_id}) ---")
        datasets = fetch_datasets(client, test_project_id)
        if datasets:
            for ds in datasets:
                logger.info(
                    f"- {ds.project_id}.{ds.dataset_id} (Location: {ds.location}, Desc: {ds.description})"
                )

            test_dataset_id = datasets[0].dataset_id  # 最初のデータセットでテスト
            logger.info(
                f"\n--- テーブル一覧とスキーマ ({test_project_id}.{test_dataset_id}) ---"
            )
            tables = fetch_tables_and_schemas(client, test_project_id, test_dataset_id)
            for tbl in tables:
                logger.info(f"\nTable: {tbl.full_table_id}")
                logger.info(f"  Description: {tbl.description}")
                logger.info(f"  Rows: {tbl.num_rows}, Bytes: {tbl.num_bytes}")
                logger.info(
                    f"  Created: {tbl.created_time}, Modified: {tbl.last_modified_time}"
                )
                if tbl.schema_:
                    logger.info("  Schema:")
                    for col in tbl.schema_.columns:
                        logger.info(
                            f"    - {col.name} ({col.type}, {col.mode}) {col.description or ''}"
                        )
                        if col.fields:
                            logger.info(f"      Nested Fields:")
                            for nested_col in col.fields:
                                logger.info(
                                    f"        - {nested_col.name} ({nested_col.type}, {nested_col.mode})"
                                )

        else:
            logger.info(
                f"プロジェクト '{test_project_id}' でデータセットが見つかりませんでした。"
            )
    elif not client:
        logger.info("BigQueryクライアントの初期化に失敗しました。")
    elif not settings.project_ids:
        logger.info("テスト対象のプロジェクトIDが設定されていません。")
