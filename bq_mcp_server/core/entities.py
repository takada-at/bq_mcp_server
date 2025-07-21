# models.py: Defines Pydantic models for data structures and API responses
import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class LogSetting(BaseModel):
    """Model indicating log configuration completion"""

    log_to_console: bool = Field(False, description="Whether to output logs to console")
    enable_file_logging: bool = Field(
        False, description="Whether to enable file logging"
    )


class ColumnSchema(BaseModel):
    """Model representing BigQuery table column schema"""

    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Data type (e.g., STRING, INTEGER, TIMESTAMP)")
    mode: str = Field(..., description="Mode (NULLABLE, REQUIRED, REPEATED)")
    description: Optional[str] = Field(None, description="Column description")
    fields: Optional[List["ColumnSchema"]] = Field(
        None, description="Nested fields for RECORD type"
    )  # Recursive definition


class TableSchema(BaseModel):
    """Model representing entire BigQuery table schema"""

    columns: List[ColumnSchema] = Field(..., description="List of table columns")


class TableMetadata(BaseModel):
    """Model representing BigQuery table metadata"""

    project_id: str = Field(..., description="Project ID")
    dataset_id: str = Field(..., description="Dataset ID")
    table_id: str = Field(..., description="Table ID")
    full_table_id: str = Field(..., description="Full table ID (project.dataset.table)")
    schema_: Optional[TableSchema] = Field(
        None, description="Table schema"
    )  # 'schema' is reserved in BaseModel, so using alias
    description: Optional[str] = Field(None, description="Table description")
    num_rows: Optional[int] = Field(None, description="Number of table rows")
    num_bytes: Optional[int] = Field(None, description="Table size in bytes")
    created_time: Optional[datetime.datetime] = Field(None, description="Creation time")
    last_modified_time: Optional[datetime.datetime] = Field(
        None, description="Last modified time"
    )
    # Add other necessary metadata if needed


class DatasetMetadata(BaseModel):
    """Model representing BigQuery dataset metadata"""

    project_id: str = Field(..., description="Project ID")
    dataset_id: str = Field(..., description="Dataset ID")
    description: Optional[str] = Field(None, description="Dataset description")
    location: Optional[str] = Field(None, description="Dataset location")
    # Add other necessary metadata if needed


# --- API Response Models ---


class DatasetListResponse(BaseModel):
    """Response model for /datasets endpoint"""

    datasets: List[DatasetMetadata] = Field(..., description="List of dataset metadata")


class TableListResponse(BaseModel):
    """Response model for /<dataset>/tables endpoint"""

    tables: List[TableMetadata] = Field(
        ..., description="List of table metadata"
    )  # Simplified version without schema


class SearchResultItem(BaseModel):
    """Search result item"""

    type: str = Field(..., description="Item type ('dataset', 'table', 'column')")
    project_id: str
    dataset_id: str
    table_id: Optional[str] = None  # For table or column
    column_name: Optional[str] = None  # For column
    match_location: str = Field(
        ..., description="Location where keyword matched ('name', 'description')"
    )
    # Add other information (description, etc.) as needed


class SearchResponse(BaseModel):
    """Response model for /search endpoint"""

    query: str = Field(..., description="Executed search keyword")
    results: List[SearchResultItem] = Field(..., description="Search result list")


class QueryExecutionRequest(BaseModel):
    """Query execution request model"""

    sql: str = Field(..., description="SQL query to execute")
    project_id: Optional[str] = Field(
        None, description="Target project ID for execution"
    )


class QueryDryRunResult(BaseModel):
    """Dry run execution result model"""

    total_bytes_processed: int = Field(
        ..., description="Expected bytes to be processed"
    )
    total_bytes_billed: int = Field(..., description="Expected bytes to be billed")
    is_safe: bool = Field(..., description="Whether the query can be executed safely")
    modified_sql: str = Field(..., description="SQL with modified LIMIT clause")
    error_message: Optional[str] = Field(None, description="Error message")


class QueryExecutionResult(BaseModel):
    """Query execution result model"""

    success: bool = Field(..., description="Whether the query executed successfully")
    rows: Optional[List[Dict]] = Field(None, description="Query result row data")
    total_rows: Optional[int] = Field(None, description="Total number of rows")
    total_bytes_processed: Optional[int] = Field(None, description="Bytes processed")
    total_bytes_billed: Optional[int] = Field(None, description="Bytes billed")
    execution_time_ms: Optional[int] = Field(
        None, description="Execution time in milliseconds"
    )
    error_message: Optional[str] = Field(None, description="Error message")
    job_id: Optional[str] = Field(None, description="BigQuery job ID")


# --- Cache Data Structures ---
class CachedData(BaseModel):
    """Model for entire data stored in cache"""

    datasets: Dict[str, List[DatasetMetadata]] = Field(
        default_factory=dict, description="Dataset list by project ID"
    )  # key: project_id
    tables: Dict[str, Dict[str, List[TableMetadata]]] = Field(
        default_factory=dict,
        description="Table list by project and dataset (including schema)",
    )  # key1: project_id, key2: dataset_id
    last_updated: Optional[datetime.datetime] = Field(
        None, description="Cache last updated time"
    )


class Settings(BaseModel):
    """Class for managing application settings"""

    # GCP related settings
    gcp_service_account_key_path: Optional[str] = Field(
        None, description="GCP service account key path (uses ADC if not set)"
    )
    project_ids: List[str] = Field(
        ...,
        description="List of GCP project IDs",
    )
    dataset_filters: List[str] = Field(
        default_factory=list,
        description="List of dataset filters (e.g., pj1.*,pj2.dataset1)",
    )
    # Cache settings
    cache_ttl_seconds: int = Field(
        3600,
        description="Cache TTL in seconds",
    )
    cache_file_base_dir: str = Field(
        ".bq_metadata_cache", description="Cache file storage directory"
    )

    # API server settings (for uvicorn Web API)
    api_host: str = Field("127.0.0.1", description="API server hostname")
    api_port: int = Field(8000, description="API server port number")

    # Query execution settings
    query_execution_project_id: Optional[str] = Field(
        None,
        description="Project ID to use for query execution (defaults to first project in project_ids if not set)",
    )
    max_scan_bytes: int = Field(
        1024 * 1024 * 1024,  # 1GB
        description="Maximum scan bytes for queries",
    )
    default_query_limit: int = Field(100, description="Default query result limit")
    query_timeout_seconds: int = Field(
        300,  # 5 minutes
        description="Query timeout in seconds",
    )
    # Logging settings
    enable_file_logging: bool = Field(
        False, description="Whether to enable file logging"
    )


@dataclass
class ApplicationContext:
    """Class for holding application context"""

    settings: Settings
    log_setting: LogSetting
    cache_data: CachedData
