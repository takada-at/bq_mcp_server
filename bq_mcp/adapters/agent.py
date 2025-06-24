from pydantic_ai import Agent, RunContext
from pydantic_ai.models.gemini import GeminiModel
import asyncio


from bq_mcp.core import converter, logic
from bq_mcp.core.entities import ApplicationContext
from bq_mcp.repositories import cache_manager, config, log, search_engine


model = GeminiModel("gemini-2.5-pro-preview-03-25", provider="google-vertex")
agent = Agent(
    model,
    deps_type=ApplicationContext,
    system_prompt="""あなたのタスクは、ツールを使ってメタデータを検索しながら、BigQueryのSQLを書くことです。
あなたは、BigQueryのデータセット、テーブル、カラムのメタデータを検索するためのツールを持っています。ツールを積極的に使用しましょう。
データセットやテーブル名は必ず、ツールを使って確認するようにしてください。
""",
)


@agent.tool
async def get_datasets(ctx: RunContext[ApplicationContext]):
    """
    Get list of all datasets
    """
    datasets = await logic.get_datasets()
    markdown_content = converter.convert_datasets_to_markdown(datasets.datasets)
    return markdown_content


@agent.tool
async def get_tables(
    ctx: RunContext[ApplicationContext], dataset_id: str, project_id: str = None
):
    """
    Get list of all tables in a dataset
    """
    tables = await logic.get_tables(dataset_id, project_id)
    markdown_content = converter.convert_tables_to_markdown(tables)
    return markdown_content


@agent.tool
async def search_metadata(ctx: RunContext[ApplicationContext], key: str):
    """
    Search metadata for datasets, tables, and columns
    """
    results = await search_engine.search_metadata(key)
    markdown_content = converter.convert_search_results_to_markdown(key, results)
    return markdown_content


async def main():
    log_setting = log.init_logger()
    setting = config.init_setting()
    cache_data = await cache_manager.get_cached_data()
    context = ApplicationContext(
        settings=setting,
        log_setting=log_setting,
        cache_data=cache_data,
    )
    print("run agent")
    result = await agent.run(
        "copilot_metricsテーブルのcountを取得するSQLを書いてください", deps=context
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
