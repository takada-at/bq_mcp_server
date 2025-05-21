from abc import ABC, abstractmethod
from typing import List, Optional # Using Any for now for entities
# Import TableMetadata as Table and DatasetMetadata as Dataset
from bq_meta_api.domain.entities import TableMetadata as Table, DatasetMetadata as Dataset, Routine, SearchResultItem 

class IFetchBigQueryTableMetadataUseCase(ABC):
    @abstractmethod
    def execute(self, project_id: str, dataset_id: str, table_id: str) -> Optional[Table]:
        pass

class IFetchBigQueryRoutineMetadataUseCase(ABC):
    @abstractmethod
    def execute(self, project_id: str, dataset_id: str, routine_id: str) -> Optional[Routine]: # Routine type hint
        pass

class IListDatasetsUseCase(ABC):
    @abstractmethod
    def execute(self, project_id: str) -> List[Dataset]:
        pass

class IListTablesInDatasetUseCase(ABC):
    @abstractmethod
    def execute(self, project_id: str, dataset_id: str) -> List[Table]: # Table type hint
        pass

class IListRoutinesInDatasetUseCase(ABC):
    @abstractmethod
    def execute(self, project_id: str, dataset_id: str) -> List[Routine]: # Routine type hint
        pass

class ISearchMetadataUseCase(ABC):
    @abstractmethod
    def execute(self, project_id: str, query: str, limit: int = 10, offset: int = 0) -> List[SearchResultItem]: # SearchResultItem type hint
        pass
