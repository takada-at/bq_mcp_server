from typing import Optional
from bq_meta_api.domain.entities import TableMetadata as Table # Using TableMetadata as Table
from bq_meta_api.domain.repositories import IBigQueryRepository
from bq_meta_api.domain.use_cases import IFetchBigQueryTableMetadataUseCase
from bq_meta_api import log # For logging

class FetchBigQueryTableMetadataService(IFetchBigQueryTableMetadataUseCase):
    def __init__(self, bigquery_repository: IBigQueryRepository):
        self.bigquery_repository = bigquery_repository
        self.logger = log.get_logger()

    def execute(self, project_id: str, dataset_id: str, table_id: str) -> Optional[Table]:
        self.logger.info(f"Executing FetchBigQueryTableMetadataService for {project_id}.{dataset_id}.{table_id}")
        try:
            table = self.bigquery_repository.get_table_meta(project_id, dataset_id, table_id)
            if table:
                self.logger.info(f"Successfully fetched metadata for table {project_id}.{dataset_id}.{table_id}")
                return table
            else:
                self.logger.warning(f"Could not find table {project_id}.{dataset_id}.{table_id}")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching metadata for table {project_id}.{dataset_id}.{table_id}: {e}", exc_info=True)
            # Depending on desired error handling, could raise a custom domain exception here
            return None
