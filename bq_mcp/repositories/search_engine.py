# search_engine.py: Provides search functionality over cached metadata
from typing import List, Optional
from bq_mcp.repositories import log
from bq_mcp.core.entities import (
    CachedData,
    SearchResultItem,
    ColumnSchema,
)
from bq_mcp.repositories import cache_manager


def _search_columns(
    columns: List[ColumnSchema],
    keyword: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> List[SearchResultItem]:
    """Recursively searches within the specified column list."""
    results: List[SearchResultItem] = []
    lower_keyword = keyword.lower()

    for column in columns:
        # Search by column name
        if lower_keyword in column.name.lower():
            results.append(
                SearchResultItem(
                    type="column",
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=table_id,
                    column_name=column.name,
                    match_location="name",
                )
            )
        # Search by column description
        if column.description and lower_keyword in column.description.lower():
            # Check if same column name is not already added (avoid duplicates)
            if not any(
                r.type == "column"
                and r.column_name == column.name
                and r.match_location == "description"
                for r in results
            ):
                results.append(
                    SearchResultItem(
                        type="column",
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table_id,
                        column_name=column.name,
                        match_location="description",
                    )
                )

        # Recursively search nested fields
        if column.fields:
            results.extend(
                _search_columns(
                    column.fields, keyword, project_id, dataset_id, table_id
                )
            )

    return results


def _is_duplicate_result(
    result: SearchResultItem, existing_results: List[SearchResultItem]
) -> bool:
    """Check if search result is duplicate with existing results"""
    for existing in existing_results:
        if (
            result.type == existing.type
            and result.project_id == existing.project_id
            and result.dataset_id == existing.dataset_id
            and result.table_id == existing.table_id
            and result.column_name == existing.column_name
            and result.match_location == existing.match_location
        ):
            return True
    return False


def _search_datasets(cached_data: CachedData, keyword: str) -> List[SearchResultItem]:
    """Search datasets"""
    results = []
    lower_keyword = keyword.lower()

    for project_id, datasets in cached_data.datasets.items():
        for dataset in datasets:
            # Search by dataset ID
            if lower_keyword in dataset.dataset_id.lower():
                result = SearchResultItem(
                    type="dataset",
                    project_id=project_id,
                    dataset_id=dataset.dataset_id,
                    match_location="name",
                )
                if not _is_duplicate_result(result, results):
                    results.append(result)

            # Search by dataset description
            if dataset.description and lower_keyword in dataset.description.lower():
                result = SearchResultItem(
                    type="dataset",
                    project_id=project_id,
                    dataset_id=dataset.dataset_id,
                    match_location="description",
                )
                if not _is_duplicate_result(result, results):
                    results.append(result)

    return results


def _search_tables(cached_data: CachedData, keyword: str) -> List[SearchResultItem]:
    """Search tables"""
    results = []
    lower_keyword = keyword.lower()

    for project_id, datasets_tables in cached_data.tables.items():
        for dataset_id, tables in datasets_tables.items():
            for table in tables:
                # Search by table ID
                if lower_keyword in table.table_id.lower():
                    result = SearchResultItem(
                        type="table",
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table.table_id,
                        match_location="name",
                    )
                    if not _is_duplicate_result(result, results):
                        results.append(result)

                # Search by table description
                if table.description and lower_keyword in table.description.lower():
                    result = SearchResultItem(
                        type="table",
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table.table_id,
                        match_location="description",
                    )
                    if not _is_duplicate_result(result, results):
                        results.append(result)

    return results


def _search_table_columns(
    cached_data: CachedData, keyword: str
) -> List[SearchResultItem]:
    """Search table columns"""
    results = []

    for project_id, datasets_tables in cached_data.tables.items():
        for dataset_id, tables in datasets_tables.items():
            for table in tables:
                if table.schema_:
                    column_results = _search_columns(
                        table.schema_.columns,
                        keyword,
                        project_id,
                        dataset_id,
                        table.table_id,
                    )
                    for col_res in column_results:
                        if not _is_duplicate_result(col_res, results):
                            results.append(col_res)

    return results


async def search_metadata_inner(keyword: str) -> List[SearchResultItem]:
    """
    Searches for items matching keywords from the entire cached metadata.
    Targets dataset names, table names, column names, and their descriptions for search.

    Args:
        keyword: Search keyword.

    Returns:
        List of search results
    """
    logger = log.get_logger()
    logger.info(f"Executing metadata search: keyword='{keyword}'")

    cached_data: Optional[CachedData] = await cache_manager.get_cached_data()
    if not cached_data:
        logger.warning("No cache data available for search.")
        return []

    # Search each type in parallel
    dataset_results = _search_datasets(cached_data, keyword)
    table_results = _search_tables(cached_data, keyword)
    column_results = _search_table_columns(cached_data, keyword)

    # Merge results (check for duplicates across all)
    all_results = []
    for result_list in [dataset_results, table_results, column_results]:
        for result in result_list:
            if not _is_duplicate_result(result, all_results):
                all_results.append(result)

    logger.info(f"Search completed. Found {len(all_results)} hits.")
    return all_results


def multi_split(text: str, delimiters: List[str]) -> List[str]:
    """
    Function to split a string with multiple delimiters

    Args:
        text (str): String to split
        delimiters (List[str]): Set of delimiter characters

    Returns:
        list: List of split strings
    """
    # Initial result is the original string
    result = [text]

    # Process each delimiter
    for delimiter in delimiters:
        # Temporary result list
        temp_result = []
        # For each element in current result list
        for item in result:
            # Split by delimiter and add to temporary list
            temp_result.extend(item.split(delimiter))
        # Update result
        result = temp_result

    # Remove empty strings
    return [item.strip() for item in result if item]


async def search_metadata(keyword: str) -> List[SearchResultItem]:
    keywords = multi_split(keyword, [" ", ",", "."])
    results = []
    for k in keywords:
        if not k:
            continue
        k = k.replace('"', "").replace("`", "")
        results += await search_metadata_inner(k)
    return results
