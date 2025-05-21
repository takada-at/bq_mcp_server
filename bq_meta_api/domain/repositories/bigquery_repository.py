from abc import ABC, abstractmethod
from typing import Dict, List, Optional 
# Import TableMetadata as Table and DatasetMetadata as Dataset
from bq_meta_api.domain.entities import TableMetadata as Table, DatasetMetadata as Dataset, Routine # Assuming Routine entity exists

class IBigQueryRepository(ABC):
    @abstractmethod
    def get_table_meta(self, project_id: str, dataset_id: str, table_id: str) -> Optional[Table]:
        pass

    @abstractmethod
    def get_routine_meta(self, project_id: str, dataset_id: str, routine_id: str) -> Optional[Routine]: # Routine type hint
        pass

    @abstractmethod
    def list_datasets(self, project_id: str) -> List[Dataset]:
        pass

    @abstractmethod
    def list_tables(self, project_id: str, dataset_id: str) -> List[Table]: # Table type hint
        pass

    @abstractmethod
    def list_routines(self, project_id: str, dataset_id: str) -> List[Routine]: # Routine type hint
        pass
