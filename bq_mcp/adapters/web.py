# main.py: FastAPI application entry point
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Path as FastApiPath
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from typing import List, Optional, Literal

from bq_mcp.core import converter, logic
from bq_mcp.core.entities import (
    ApplicationContext,
    DatasetListResponse,
    QueryExecutionRequest,
    QueryExecutionResult,
    SearchResponse,
    TableListResponse,
    TableMetadata,
)
from bq_mcp.repositories import cache_manager, config, log, search_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle events"""
    log_setting = log.init_logger()
    settings = config.init_setting()
    cache_data = await cache_manager.get_cached_data()
    ApplicationContext(
        settings=settings,
        log_setting=log_setting,
        cache_data=cache_data,
    )

    logger = log.get_logger()
    logger.info(f"Starting API server at {settings.api_host}:{settings.api_port}.")
    logger.info(f"Monitored projects: {settings.project_ids}")
    logger.info(f"Cache TTL: {settings.cache_ttl_seconds} seconds")
    logger.info(f"Cache file: {settings.cache_file_base_dir}")
    logger.info(
        f"Query execution settings - Max scan bytes: {settings.max_scan_bytes} bytes, Default LIMIT: {settings.default_query_limit}"
    )
    yield


app = FastAPI(
    title="BigQuery MCP Server",
    description="Provides access to cached BigQuery dataset, table, and schema information.",
    version="0.1.0",
    lifespan=lifespan,
)


# --- API Endpoints ---


@app.get(
    "/datasets",
    response_model=DatasetListResponse,
    summary="Get list of all datasets",
    description="Returns a list of all datasets across all configured projects from the cache.",
)
async def get_datasets():
    """Returns a list of all datasets across all projects"""
    logger = log.get_logger()
    try:
        return await logic.get_datasets()
    except Exception as e:
        logger.error(f"Error in /datasets endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal error occurred while retrieving dataset list.",
        )


@app.get(
    "/{dataset_id}/tables",
    responses={
        200: {
            "content": {
                "application/json": {"model": TableListResponse},
                "text/markdown": {
                    "example": "### Table: `prj-example.example_dataset.users`\n\n| Column Name | Data Type | Mode | Description |\n|---------|---------|--------|------|\n| user_id | INTEGER | REQUIRED | User unique identifier |\n| name | STRING | NULLABLE | User name |\n| created_at | TIMESTAMP | REQUIRED | Creation timestamp |"
                },
            }
        }
    },
    summary="Get list of tables in a dataset",
    description="Returns a list of tables for the specified dataset ID from the cache. Searches across all configured projects. Format can be either 'json' or 'markdown'.",
)
async def get_tables_in_dataset(
    dataset_id: str = FastApiPath(
        ..., description="The ID of the dataset to retrieve tables for."
    ),
    project_id: Optional[str] = Query(
        None, description="The ID of the project to filter datasets by."
    ),
    format: Optional[Literal["json", "markdown"]] = Query(
        "markdown", description="Response format: 'json' or 'markdown'."
    ),
):
    """Returns a list of tables belonging to the specified dataset ID"""
    logger = log.get_logger()
    try:
        found_tables: List[TableMetadata] = await logic.get_tables(
            dataset_id, project_id=project_id
        )
        if format == "markdown":
            # Generate markdown format response
            markdown_content = converter.convert_tables_to_markdown(found_tables)
            return Response(content=markdown_content, media_type="text/markdown")
        else:
            # Generate JSON format response
            return TableListResponse(tables=found_tables)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in /{dataset_id}/tables endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Internal error occurred while retrieving table list.",
        )


@app.get(
    "/search",
    responses={
        200: {
            "content": {
                "application/json": {"model": SearchResponse},
                "text/markdown": {
                    "example": "## Search Results: `user`\n\n**3** hits found.\n\n### Tables\n- **project-id.dataset_id.users** (name match)\n\n### Columns\n- **project-id.dataset_id.users.user_id** (name match)\n- **project-id.dataset_id.logs.user_name** (name match)"
                },
            }
        }
    },
    summary="Search metadata",
    description="Searches dataset names, table names, column names, and descriptions in the cache for the given keyword. Format can be either 'json' or 'markdown'.",
)
async def search_items(
    key: str = Query(..., description="Keyword to search for in metadata."),
    format: Optional[Literal["json", "markdown"]] = Query(
        "markdown", description="Response format: 'json' or 'markdown'."
    ),
):
    """Search metadata based on keywords"""
    logger = log.get_logger()
    if not key:
        raise HTTPException(
            status_code=400, detail="Please specify search keyword 'key'."
        )
    try:
        search_result = await search_engine.search_metadata(key)
        # Process response format
        if format == "markdown":
            search_response = converter.convert_search_results_to_markdown(
                query=key, results=search_result
            )
            return PlainTextResponse(
                content=search_response, media_type="text/markdown"
            )
        else:
            return SearchResponse(query=key, results=search_result)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error in /search endpoint (key={key}): {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal error occurred while searching metadata."
        )


@app.post(
    "/cache/update",
    summary="Force update cache",
    description="Forces an update of the local metadata cache by fetching fresh data from BigQuery.",
    status_code=202,  # Accepted
)
async def force_update_cache():
    """Endpoint to trigger manual cache update"""
    logger = log.get_logger()
    logger.info("Received manual cache update request.")
    # Here we wait for completion instead of running update in background
    # For large scale, using BackgroundTasks would be better
    try:
        updated_cache = await cache_manager.update_cache()
        if updated_cache:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Cache update completed successfully.",
                    "last_updated": updated_cache.last_updated.isoformat(),
                },
            )
        else:
            raise HTTPException(status_code=500, detail="Cache update failed.")
    except Exception as e:
        logger.error(f"Error in /cache/update endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error occurred during cache update: {e}"
        )


@app.post(
    "/query/execute",
    response_model=QueryExecutionResult,
    summary="Execute BigQuery SQL",
    description="Executes a BigQuery SQL with safety checks. Automatically adds/modifies LIMIT clause and checks scan amount unless dry_run is True or force execution is requested.",
)
async def execute_query(request: QueryExecutionRequest):
    """Execute BigQuery query safely"""
    try:
        result = await logic.execute_query(request.sql, request.project_id, False)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        log.get_logger().error(f"Error in /query/execute endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal error occurred during query execution."
        )


# --- Configuration for running with uvicorn ---
# Start uvicorn when this file is executed directly
if __name__ == "__main__":
    # To specify main:app, do not execute this file directly,
    # but run from command line like `uvicorn bq_mcp.main:app --reload --host 0.0.0.0 --port 8000`
    print("To start the server, execute the following command:")
    print("uvicorn bq_mcp.adapters.web:app --reload")
