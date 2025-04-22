from mcp.server.fastmcp import FastMCP
from typing import Optional


mcp = FastMCP(
    "BigQuery Metadata API",
    description="Provides access to BigQuery dataset, table, and schema information.",
    instructions="""Use search_metadata to search for metadata.
Use get_datasets to retrieve a list of datasets and get_tables to retrieve a list of tables.""",
    version="0.1.0",
)
from bq_meta_api import log
log.init_logger(log_to_console=False)
from bq_meta_api import converter, logic, search_engine


@mcp.tool("get_datasets")
def get_datasets():
    """
    Get list of all datasets
    """
    datasets = logic.get_datasets()
    markdown_content = converter.convert_datasets_to_markdown(datasets.datasets)
    return markdown_content


@mcp.tool("get_tables")
def get_tables(dataset_id: str, project_id: Optional[str] = None):
    """
    Get list of all tables in a dataset
    """
    tables = logic.get_tables(dataset_id, project_id)
    markdown_content = converter.convert_tables_to_markdown(tables)
    return markdown_content


@mcp.tool("search_metadata")
def search_metadata(key: str):
    """
    Search metadata for datasets, tables, and columns
    """
    results = search_engine.search_metadata(key)
    markdown_content = converter.convert_search_results_to_markdown(key, results)
    return markdown_content


if __name__ == "__main__":
    mcp.run()