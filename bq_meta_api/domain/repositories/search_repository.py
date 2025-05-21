from abc import ABC, abstractmethod
from typing import Any, List, Dict # Assuming SearchResultItem will be an entity or DTO

# from bq_meta_api.domain.entities import SearchResultItem # Target import

class ISearchRepository(ABC):
    @abstractmethod
    def search_tables(self, project_id: str, query: str, limit: int = 10, offset: int = 0) -> List[Any]: # Replace Any with SearchResultItem
        pass

    @abstractmethod
    def search_routines(self, project_id: str, query: str, limit: int = 10, offset: int = 0) -> List[Any]: # Replace Any with SearchResultItem
        pass

    @abstractmethod
    def search_columns(self, project_id: str, query: str, limit: int = 10, offset: int = 0) -> List[Any]: # Replace Any with SearchResultItem
        pass

    @abstractmethod
    def index_table(self, project_id: str, dataset_id: str, table_id: str, table_data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def index_routine(self, project_id: str, dataset_id: str, routine_id: str, routine_data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def delete_table_index(self, project_id: str, dataset_id: str, table_id: str) -> None:
        pass
