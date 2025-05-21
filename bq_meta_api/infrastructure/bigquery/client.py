# client.py: Handles communication with Google BigQuery API
from typing import List, Optional, Any, Dict
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError, RefreshError
from bq_meta_api import config, log
# Assuming models will be moved to domain.entities, adjust import once that happens
# For now, using relative import assuming models.py is in the parent directory of infrastructure
# This will likely need to be from bq_meta_api.domain.entities import ... later
from ...domain.entities import DatasetMetadata, TableMetadata, TableSchema, ColumnSchema 
from ...domain.repositories import IBigQueryRepository


class BigQueryClient(IBigQueryRepository):
    def __init__(self, project_id: Optional[str] = None):
        self.logger = log.get_logger()
        self.settings = config.get_settings()
        self.client = self._initialize_client(project_id)

    def _initialize_client(self, project_id_override: Optional[str] = None) -> Optional[bigquery.Client]:
        try:
            if self.settings.gcp_service_account_key_path:
                self.logger.info(
                    f"サービスアカウントキーを使用して認証します: {self.settings.gcp_service_account_key_path}"
                )
                credentials = service_account.Credentials.from_service_account_file(
                    self.settings.gcp_service_account_key_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                # If project_id_override is provided, use it. Otherwise, don't set project at client level.
                client_project = project_id_override if project_id_override else None
                return bigquery.Client(credentials=credentials, project=client_project)
            else:
                self.logger.info("Application Default Credentials (ADC) を使用して認証します。")
                # If project_id_override is provided, use it. Otherwise, use first from settings or None.
                client_project = project_id_override if project_id_override else (self.settings.project_ids[0] if self.settings.project_ids else None)
                client = bigquery.Client(project=client_project)
            self.logger.info(f"BigQueryクライアントの初期化に成功しました。デフォルトプロジェクト: {client.project}")
            return client
        except FileNotFoundError:
            self.logger.error(
                f"サービスアカウントキーファイルが見つかりません: {self.settings.gcp_service_account_key_path}"
            )
        except DefaultCredentialsError:
            self.logger.error(
                "Application Default Credentials (ADC) が見つかりません。gcloud auth application-default login を実行するか、サービスアカウントキーを設定してください。"
            )
        except RefreshError as e:
            self.logger.error(f"認証情報の更新に失敗しました: {e}")
        except Exception as e:
            self.logger.error(
                f"BigQueryクライアントの初期化中に予期せぬエラーが発生しました: {e}"
            )
        return None

    def _parse_schema(self, schema_fields: List[bigquery.SchemaField]) -> List[Any]: # Replace Any with ColumnSchema
        columns = []
        for field in schema_fields:
            nested_fields = None
            if field.field_type == "RECORD" or field.field_type == "STRUCT":
                if field.fields:
                    nested_fields = self._parse_schema(field.fields)
            columns.append(
                ColumnSchema( # Assuming ColumnSchema is available
                    name=field.name,
                    type=field.field_type,
                    mode=field.mode,
                    description=field.description,
                    fields=nested_fields,
                )
            )
        return columns

    def get_table_meta(self, project_id: str, dataset_id: str, table_id: str) -> Optional[Any]: # Replace Any with Table
        if not self.client:
            self.logger.error("BigQuery client not initialized.")
            return None
        try:
            table_ref = self.client.dataset(dataset_id, project=project_id).table(table_id)
            table = self.client.get_table(table_ref)
            
            schema_model = None
            if table.schema:
                parsed_columns = self._parse_schema(table.schema)
                schema_model = TableSchema(columns=parsed_columns) # Assuming TableSchema is available

            return TableMetadata( # Assuming TableMetadata is available
                project_id=table.project,
                dataset_id=table.dataset_id,
                table_id=table.table_id,
                full_table_id=f"{table.project}.{table.dataset_id}.{table.table_id}",
                schema_=schema_model,
                description=table.description,
                num_rows=table.num_rows,
                num_bytes=table.num_bytes,
                created_time=table.created,
                last_modified_time=table.modified,
            )
        except Exception as e:
            self.logger.error(f"テーブル '{project_id}.{dataset_id}.{table_id}' のメタデータ取得中にエラー: {e}")
            return None

    def get_routine_meta(self, project_id: str, dataset_id: str, routine_id: str) -> Optional[Any]: # Replace Any with Routine
        # This method needs to be implemented based on how routines are fetched.
        # google-cloud-bigquery library has client.get_routine()
        if not self.client:
            self.logger.error("BigQuery client not initialized.")
            return None
        try:
            routine_ref = self.client.dataset(dataset_id, project=project_id).routine(routine_id)
            routine = self.client.get_routine(routine_ref)
            # Convert 'routine' to your domain Routine object here.
            # For now, returning the raw routine object or a placeholder.
            # Example:
            # return Routine(name=routine.name, type=routine.type, ...) 
            self.logger.info(f"Routine {routine.reference} fetched successfully.")
            return routine # Replace with domain entity
        except Exception as e:
            self.logger.error(f"ルーチン '{project_id}.{dataset_id}.{routine_id}' のメタデータ取得中にエラー: {e}")
            return None

    def list_datasets(self, project_id: str) -> List[Any]: # Replace Any with Dataset
        if not self.client:
            self.logger.error("BigQuery client not initialized.")
            return []
        datasets_metadata = []
        try:
            datasets = list(self.client.list_datasets(project=project_id))
            self.logger.info(
                f"プロジェクト '{project_id}' から {len(datasets)} 個のデータセットを取得しました。"
            )
            for dataset in datasets:
                try:
                    dataset_ref = self.client.get_dataset(dataset.reference)
                    metadata = DatasetMetadata( # Assuming DatasetMetadata is available
                        project_id=dataset_ref.project,
                        dataset_id=dataset_ref.dataset_id,
                        description=dataset_ref.description,
                        location=dataset_ref.location,
                    )
                    datasets_metadata.append(metadata)
                except Exception as e:
                    self.logger.warning(
                        f"データセット '{project_id}.{dataset.dataset_id}' の詳細取得中にエラー: {e}"
                    )
                    metadata = DatasetMetadata(
                        project_id=project_id,
                        dataset_id=dataset.dataset_id,
                    )
                    datasets_metadata.append(metadata)
        except Exception as e:
            self.logger.error(f"プロジェクト '{project_id}' のデータセット取得中にエラー: {e}")
        return datasets_metadata

    def list_tables(self, project_id: str, dataset_id: str) -> List[Any]: # Replace Any with Table
        if not self.client:
            self.logger.error("BigQuery client not initialized.")
            return []
        tables_metadata = []
        dataset_ref = self.client.dataset(dataset_id, project=project_id)
        try:
            tables = list(self.client.list_tables(dataset_ref))
            self.logger.info(
                f"データセット '{project_id}.{dataset_id}' から {len(tables)} 個のテーブルを取得しました。"
            )
            for table_item in tables:
                # Fetch full table metadata for each table
                table_meta = self.get_table_meta(project_id, dataset_id, table_item.table_id)
                if table_meta:
                    tables_metadata.append(table_meta)
                else:
                    # Fallback if get_table_meta fails for some reason, add basic info
                    full_table_id = f"{project_id}.{dataset_id}.{table_item.table_id}"
                    tables_metadata.append(TableMetadata(
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table_item.table_id,
                        full_table_id=full_table_id,
                    ))
        except Exception as e:
            self.logger.error(
                f"データセット '{project_id}.{dataset_id}' のテーブル取得中にエラー: {e}"
            )
        return tables_metadata

    def list_routines(self, project_id: str, dataset_id: str) -> List[Any]: # Replace Any with Routine
        if not self.client:
            self.logger.error("BigQuery client not initialized.")
            return []
        routines_list = []
        try:
            routines_iterator = self.client.list_routines(f"{project_id}.{dataset_id}")
            for routine in routines_iterator:
                # Convert 'routine' to your domain Routine object here.
                # For now, appending the raw routine object or a placeholder.
                # Example:
                # routines_list.append(Routine(name=routine.name, ...))
                routines_list.append(routine) # Replace with domain entity
            self.logger.info(f"Dataset {project_id}.{dataset_id} listed {len(routines_list)} routines.")
        except Exception as e:
            self.logger.error(f"データセット '{project_id}.{dataset_id}' のルーチン取得中にエラー: {e}")
        return routines_list

# Helper function to get a configured client instance - can be removed if not used externally
# Or kept if the old way of getting client is still needed by other parts of the app for a while.
def get_bigquery_client(project_id: Optional[str] = None) -> Optional[bigquery.Client]:
    """
    BigQueryクライアントを初期化して返します。
    This is now a wrapper around the BigQueryClient class.
    Consider refactoring callers to use BigQueryClient directly.
    """
    client_instance = BigQueryClient(project_id=project_id)
    return client_instance.client

# The following functions are now methods of BigQueryClient or can be adapted.
# fetch_datasets -> BigQueryClient.list_datasets
# _parse_schema -> BigQueryClient._parse_schema
# fetch_tables_and_schemas -> BigQueryClient.list_tables (which calls get_table_meta)
# Keep them here if they are used by other modules directly, but ideally refactor those modules.
# For this refactoring, we assume they are effectively replaced by class methods.
# If other modules still rely on these exact function signatures, they'd need separate adaptation.

# Example of how the old fetch_datasets might be mapped if needed for compatibility:
# def fetch_datasets(client_or_project_id: Any, project_id_arg: Optional[str] = None) -> List[Any]:
#     logger = log.get_logger()
#     if isinstance(client_or_project_id, str) and project_id_arg is None: # Only project_id passed
#         project_id = client_or_project_id
#         repo = BigQueryClient() # Uses default project or ADC
#     elif isinstance(client_or_project_id, bigquery.Client) and project_id_arg: # Client and project_id passed
#         # This case is tricky because BigQueryClient manages its own client instance.
#         # One option is to instantiate BigQueryClient with the project_id from the passed client.
#         project_id = project_id_arg
#         # repo = BigQueryClient(project_id=client_or_project_id.project) # This might not be correct
#         # Or, if the passed client should be used directly (less ideal for the new structure):
#         logger.warning("fetch_datasets called with explicit client, direct use preferred via BigQueryClient instance.")
#         # This direct call bypasses the BigQueryClient class structure for this specific call.
#         # return BigQueryClient().list_datasets(project_id) # This would create a new client.
#         # The most straightforward is to require refactoring of callers.
#         # For now, let's assume callers will be updated or these functions are removed.
#         raise NotImplementedError("fetch_datasets with explicit client needs refactoring.")
#     else: # project_id from the first argument
#         # This case is to support the old call style `fetch_datasets(client, project_id)`
#         # It's best to refactor callers.
#         logger.warning("fetch_datasets should be called with project_id only, or use BigQueryClient directly.")
#         if project_id_arg:
#            repo = BigQueryClient(project_id=project_id_arg)
#            return repo.list_datasets(project_id_arg)
#         else:
#            # Cannot determine project_id if only client is passed without project_id_arg
            # logger.error("fetch_datasets called with client but no project_id")
#            return []
#
#     return repo.list_datasets(project_id)


# def fetch_tables_and_schemas(client: bigquery.Client, project_id: str, dataset_id: str) -> List[Any]:
#     repo = BigQueryClient(project_id=project_id) # Or determine project from client
#     return repo.list_tables(project_id, dataset_id)
