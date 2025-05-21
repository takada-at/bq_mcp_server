# search_engine.py: Provides search functionality over cached metadata
import asyncio
from typing import List, Optional, Any, Dict
# Adjust imports based on new structure
from ..cache.cache_manager import CacheManager # Example, might need to be bq_meta_api.infrastructure.cache
from ... import log # Assuming log is in parent of infrastructure
# Assuming models will be in domain.entities
from ...domain.entities import CachedData, SearchResultItem, ColumnSchema # Update path later
from ...domain.repositories import ISearchRepository


class SearchEngine(ISearchRepository):
    def __init__(self, cache_repository: Optional[CacheManager] = None): # Allow injection
        self.logger = log.get_logger()
        # If cache_repository is not provided, instantiate a default one.
        # This makes SearchEngine usable standalone but also testable with mocks.
        self.cache_manager = cache_repository if cache_repository else CacheManager()


    def _search_columns_recursive( # Renamed from _search_columns to avoid conflict if it were global
        self,
        columns: List[Any], # Replace Any with ColumnSchema entity
        keyword: str,
        project_id: str,
        dataset_id: str,
        table_id: str,
    ) -> List[Any]: # Replace Any with SearchResultItem entity
        results: List[Any] = [] # Replace Any with SearchResultItem
        lower_keyword = keyword.lower()

        for column in columns:
            # Ensure column has necessary attributes (name, description, fields)
            # This depends on the actual structure of ColumnSchema entity
            if lower_keyword in getattr(column, 'name', '').lower():
                results.append(
                    SearchResultItem( # Assuming SearchResultItem is available
                        type="column",
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table_id,
                        column_name=getattr(column, 'name', ''),
                        match_location="name",
                    )
                )
            if getattr(column, 'description', None) and lower_keyword in getattr(column, 'description', '').lower():
                if not any(
                    r.type == "column"
                    and r.column_name == getattr(column, 'name', '')
                    and r.match_location == "description"
                    for r in results # Ensure 'r' also has these attributes if it's a SearchResultItem
                ):
                    results.append(
                        SearchResultItem(
                            type="column",
                            project_id=project_id,
                            dataset_id=dataset_id,
                            table_id=table_id,
                            column_name=getattr(column, 'name', ''),
                            match_location="description",
                        )
                    )
            
            nested_fields = getattr(column, 'fields', None)
            if nested_fields:
                results.extend(
                    self._search_columns_recursive(
                        nested_fields, keyword, project_id, dataset_id, table_id
                    )
                )
        return results

    async def _search_metadata_inner(self, project_id_filter: Optional[str], keyword: str, limit: int, offset: int) -> List[Any]: # Replace Any with SearchResultItem
        self.logger.info(f"メタデータ検索を実行中: keyword='{keyword}', project_id='{project_id_filter}'")
        results: List[Any] = [] # Replace Any with SearchResultItem
        
        # Use the CacheManager instance associated with this SearchEngine
        # The original get_cached_data was an async global function.
        # CacheManager has get_cached_data_all() which is async.
        cached_data: Optional[CachedData] = await self.cache_manager.get_cached_data_all()


        if not cached_data:
            self.logger.warning("検索対象のキャッシュデータがありません。")
            return results

        lower_keyword = keyword.lower()
        
        # Iterate through projects. If project_id_filter is set, only process that project.
        project_ids_to_search = [project_id_filter] if project_id_filter else cached_data.datasets.keys()

        for project_id in project_ids_to_search:
            if project_id not in cached_data.datasets:
                continue

            # 1. Search Datasets
            for dataset in cached_data.datasets.get(project_id, []):
                if lower_keyword in dataset.dataset_id.lower():
                    results.append(SearchResultItem(type="dataset", project_id=project_id, dataset_id=dataset.dataset_id, match_location="name"))
                if dataset.description and lower_keyword in dataset.description.lower():
                    if not any(r.type == "dataset" and r.dataset_id == dataset.dataset_id and r.match_location == "description" for r in results):
                        results.append(SearchResultItem(type="dataset", project_id=project_id, dataset_id=dataset.dataset_id, match_location="description"))

            # 2. Search Tables and Columns
            project_tables = cached_data.tables.get(project_id, {})
            for dataset_id, tables_in_dataset in project_tables.items():
                for table in tables_in_dataset:
                    if lower_keyword in table.table_id.lower():
                        results.append(SearchResultItem(type="table", project_id=project_id, dataset_id=dataset_id, table_id=table.table_id, match_location="name"))
                    if table.description and lower_keyword in table.description.lower():
                        if not any(r.type == "table" and r.table_id == table.table_id and r.match_location == "description" for r in results):
                             results.append(SearchResultItem(type="table", project_id=project_id, dataset_id=dataset_id, table_id=table.table_id, match_location="description"))
                    
                    if table.schema_: # Ensure schema_ has columns attribute
                        column_results = self._search_columns_recursive(
                            table.schema_.columns, keyword, project_id, dataset_id, table.table_id
                        )
                        for col_res in column_results:
                            # Simplified duplicate check for brevity, ensure it's robust
                            if col_res not in results:
                                results.append(col_res)
        
        # Apply limit and offset
        paginated_results = results[offset : offset + limit]
        self.logger.info(f"検索完了。 {len(results)} 件中 {len(paginated_results)} 件を返します。")
        return paginated_results

    def _multi_split(self, text: str, delimiters: List[str]) -> List[str]:
        result = [text]
        for delimiter in delimiters:
            temp_result = []
            for item in result:
                temp_result.extend(item.split(delimiter))
            result = temp_result
        return [item.strip() for item in result if item]

    async def _execute_search(self, project_id: str, query: str, limit: int, offset: int, search_type_filter: Optional[str] = None) -> List[Any]: # Replace Any with SearchResultItem
        keywords = self._multi_split(query, [" ", ",", "."])
        all_results: List[Any] = [] # Replace Any with SearchResultItem
        
        # Since _search_metadata_inner now handles project_id filtering, call it once.
        # We might need to aggregate results if keywords are meant to be OR'd, 
        # or refine if they are AND'd. The original code OR'd results from each keyword.
        # For simplicity, let's assume keywords are OR'd for now.
        # A more sophisticated approach might involve building a complex query for the search backend.
        
        # This part needs careful thought on how multiple keywords and types are handled.
        # The original search_metadata ran search_metadata_inner for each keyword and extended results.
        # ISearchRepository has search_tables, search_routines, search_columns.
        # Let's assume the 'query' is a single keyword for these specific methods.
        
        # If we are implementing the broader search_metadata method:
        # temp_results_set = set() # Use a set to store tuples of result fields to handle duplicates
        # for k in keywords:
        #     if not k: continue
        #     k_cleaned = k.replace('"', "").replace("`", "")
        #     # _search_metadata_inner needs to be adapted or called multiple times if it processes one keyword.
        #     # Current _search_metadata_inner already iterates through all content for one keyword.
        #     # So, we call it for each keyword and aggregate.
        #     keyword_results = await self._search_metadata_inner(project_id, k_cleaned, 0, 0) # Get all for this keyword first
        #     for res in keyword_results:
        #         # Create a hashable representation of the result for deduplication
        #         # This assumes SearchResultItem is hashable or can be converted to a hashable tuple
        #         # Example: (res.type, res.project_id, res.dataset_id, res.table_id, res.column_name, res.match_location)
        #         # This needs to be defined based on SearchResultItem structure
        #         # For now, let's assume direct addition and handle duplicates later if necessary, or that SearchResultItem is properly hashable.
        #         all_results.append(res)

        # The above block is for a general search. For specific searches (tables, routines, columns):
        # We'd call _search_metadata_inner once with the query, then filter by type.

        raw_results = await self._search_metadata_inner(project_id, query, 0, 0) # Fetch all matching this query first

        if search_type_filter:
            filtered_results = [r for r in raw_results if r.type == search_type_filter]
        else: # General search, no type filter from specific methods
            filtered_results = raw_results
            
        # Deduplicate (simplistic, assumes SearchResultItem is hashable or we convert to tuple)
        # A more robust deduplication would consider all fields.
        # Example: unique_results_map = { (r.type, r.full_path_or_id, r.match_location): r for r in filtered_results }
        # all_results = list(unique_results_map.values())
        # For now, just pass through, assuming _search_metadata_inner handles some deduplication.
        all_results = []
        seen = set()
        for item in filtered_results:
            # Create a unique tuple signature for each item
            # This needs to be adjusted based on actual SearchResultItem fields
            sig = (item.type, item.project_id, item.dataset_id, getattr(item, 'table_id', None), getattr(item, 'column_name', None), item.match_location)
            if sig not in seen:
                all_results.append(item)
                seen.add(sig)
        
        # Apply limit and offset to the final, deduplicated list
        paginated_and_filtered_results = all_results[offset : offset + limit]
        self.logger.info(f"Execute search for '{query}' (type: {search_type_filter or 'any'}) completed. {len(all_results)} total, returning {len(paginated_and_filtered_results)}.")
        return paginated_and_filtered_results


    # ISearchRepository methods
    async def search_tables(self, project_id: str, query: str, limit: int = 10, offset: int = 0) -> List[Any]: # Replace Any with SearchResultItem
        return await self._execute_search(project_id, query, limit, offset, search_type_filter="table")

    async def search_routines(self, project_id: str, query: str, limit: int = 10, offset: int = 0) -> List[Any]: # Replace Any with SearchResultItem
        # Assuming routines search is similar to tables, but filters for type "routine"
        # This requires that routines are indexed and SearchResultItem can represent them.
        # The current _search_metadata_inner does not explicitly handle routines.
        # This would need to be added to _search_metadata_inner.
        # For now, returning empty or adapting existing logic if routines are like tables/datasets.
        self.logger.info("Routine search requested. Current implementation primarily focuses on datasets, tables, columns.")
        # Placeholder:
        # return await self._execute_search(project_id, query, limit, offset, search_type_filter="routine")
        return [] # Or implement routine search in _search_metadata_inner

    async def search_columns(self, project_id: str, query: str, limit: int = 10, offset: int = 0) -> List[Any]: # Replace Any with SearchResultItem
        return await self._execute_search(project_id, query, limit, offset, search_type_filter="column")

    async def index_table(self, project_id: str, dataset_id: str, table_id: str, table_data: Dict[str, Any]) -> None:
        # This method implies an active indexing process, separate from relying on CacheManager's cache.
        # If the search is purely over whatever CacheManager provides, this might be a no-op
        # or trigger a cache update for the specific table.
        # For a true search engine, this would involve sending data to an indexing backend (e.g., Elasticsearch).
        self.logger.info(f"Indexing table {project_id}.{dataset_id}.{table_id}. Data: {table_data}")
        # Example: Trigger a cache update for this specific table if that's how "indexing" is handled.
        # This is a conceptual placeholder. Actual implementation depends on search backend.
        # await self.cache_manager.update_specific_table_cache(project_id, dataset_id, table_id, table_data)
        # Or, if using a separate search index:
        # self.search_index_backend.index_document(type='table', id=f"{project_id}.{dataset_id}.{table_id}", document=table_data)
        await asyncio.sleep(0) # Simulate async work
        pass

    async def index_routine(self, project_id: str, dataset_id: str, routine_id: str, routine_data: Dict[str, Any]) -> None:
        self.logger.info(f"Indexing routine {project_id}.{dataset_id}.{routine_id}. Data: {routine_data}")
        # Similar to index_table, depends on the search backend.
        # self.search_index_backend.index_document(type='routine', id=f"{project_id}.{dataset_id}.{routine_id}", document=routine_data)
        await asyncio.sleep(0) # Simulate async work
        pass

    async def delete_table_index(self, project_id: str, dataset_id: str, table_id: str) -> None:
        self.logger.info(f"Deleting table index for {project_id}.{dataset_id}.{table_id}")
        # self.search_index_backend.delete_document(type='table', id=f"{project_id}.{dataset_id}.{table_id}")
        await asyncio.sleep(0) # Simulate async work
        pass

# Old global functions (search_metadata_inner, multi_split, search_metadata) are now
# methods of SearchEngine (_search_metadata_inner, _multi_split, and the public search_* methods).
# Callers should instantiate SearchEngine and use its methods.
# The main entry point `search_metadata` is effectively replaced by the specific search_* methods
# of the ISearchRepository interface. If a general search is still needed, a new method
# on SearchEngine could be added that calls _execute_search without a type filter.
