from typing import List
from bq_meta_api.domain.entities import DatasetMetadata as Dataset # Using DatasetMetadata as Dataset
from bq_meta_api.domain.repositories import IBigQueryRepository
from bq_meta_api.domain.use_cases import IListDatasetsUseCase
from bq_meta_api import log # For logging
# Import HTTPException for error handling if needed, or define custom exceptions
# from fastapi import HTTPException # Example, if re-raising HTTP exceptions

class ListDatasetsService(IListDatasetsUseCase):
    def __init__(self, bigquery_repository: IBigQueryRepository):
        self.bigquery_repository = bigquery_repository
        self.logger = log.get_logger()

    def execute(self, project_id: str) -> List[Dataset]:
        self.logger.info(f"Executing ListDatasetsService for project {project_id}")
        try:
            datasets = self.bigquery_repository.list_datasets(project_id)
            if datasets is not None: # list_datasets is expected to return a list, even if empty
                self.logger.info(f"Successfully fetched {len(datasets)} datasets for project {project_id}")
                return datasets
            else:
                # This case should ideally not happen if repository returns [] for no datasets
                self.logger.warning(f"list_datasets returned None for project {project_id}, expected an empty list.")
                return [] 
        except Exception as e:
            self.logger.error(f"Error fetching datasets for project {project_id}: {e}", exc_info=True)
            # Depending on desired error handling:
            # Option 1: Re-raise a custom domain/application exception
            # raise MyCustomApplicationException(f"Failed to list datasets for {project_id}") from e
            # Option 2: Return empty list (as per original placeholder, but errors are masked)
            return []
            # Option 3: If this service is directly used by an API that can raise HTTPException:
            # raise HTTPException(status_code=500, detail=f"Failed to list datasets for project {project_id}")
