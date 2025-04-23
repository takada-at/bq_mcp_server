from pydantic_ai import Agent
from pydantic_ai.models.gemini import GeminiModel
from bq_meta_api import log

log.init_logger(log_to_console=False)
from bq_meta_api import converter, logic, search_engine


model = GeminiModel('gemini-2.5-pro-preview-03-25', provider='google-vertex')
agent = Agent(model,
              system_prompt="""あなたのタスクは、ツールを使ってメタデータを検索しながら、BigQueryのSQLを書くことです。
あなたは、BigQueryのデータセット、テーブル、カラムのメタデータを検索するためのツールを持っています。ツールを積極的に使用しましょう。
データセットやテーブル名は必ず、ツールを使って確認するようにしてください。
""")


@agent.tool
async def get_datasets():
    """
    Get list of all datasets
    """
    datasets = logic.get_datasets()
    markdown_content = converter.convert_datasets_to_markdown(datasets.datasets)
    return markdown_content


@agent.tool
async def get_tables(dataset_id: str, project_id: str = None):
    """
    Get list of all tables in a dataset
    """
    tables = logic.get_tables(dataset_id, project_id)
    markdown_content = converter.convert_tables_to_markdown(tables)
    return markdown_content


@agent.tool
async def search_metadata(key: str):
    """
    Search metadata for datasets, tables, and columns
    """
    results = search_engine.search_metadata(key)
    markdown_content = converter.convert_search_results_to_markdown(key, results)
    return markdown_content
