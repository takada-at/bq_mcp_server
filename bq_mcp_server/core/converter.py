# converter.py: Converts BigQuery metadata to various formats
from typing import List

from bq_mcp_server.core.entities import (
    ColumnSchema,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    QuerySaveResult,
    SearchResultItem,
    TableMetadata,
)


def _format_bytes(bytes_count: int | float) -> str:
    """Format byte count to human-readable format"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_count < 1024.0:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.1f} PB"


def _create_markdown_header(title: str, level: int = 2) -> str:
    """Create markdown header with specified level"""
    prefix = "#" * level
    return f"{prefix} {title}\n"


def _add_optional_field(
    result: List[str], label: str, value: str | None, format_func=None
) -> None:
    """Add optional field to result list if value exists"""
    if value:
        formatted_value = format_func(value) if format_func else value
        result.append(f"**{label}:** {formatted_value}\n")


def convert_datasets_to_markdown(datasets: List[DatasetMetadata]) -> str:
    """Convert dataset metadata list to markdown format"""
    result = []
    for dataset in datasets:
        # Add dataset name as header
        result.append(
            _create_markdown_header(
                f"Dataset: `{dataset.project_id}.{dataset.dataset_id}`"
            )
        )

        # Add dataset description if exists
        if dataset.description:
            result.append(f"{dataset.description}\n")

        # Add dataset location if exists
        _add_optional_field(result, "Location", dataset.location)

    return "\n".join(result)


def _create_column_table_row(column: ColumnSchema) -> str:
    """Create a table row for a column schema"""
    desc = column.description or ""
    return f"| {column.name} | {column.type} | {column.mode} | {desc} |"


def convert_tables_to_markdown(tables: List[TableMetadata]) -> str:
    """Convert table metadata list to markdown format"""
    result = []

    for table in tables:
        # Add table name as header
        result.append(_create_markdown_header(f"Table: `{table.full_table_id}`", 3))

        # Add table description if exists
        if table.description:
            result.append(f"{table.description}\n")

        # Display schema information in table format if exists
        if (
            hasattr(table, "schema_")
            and table.schema_
            and hasattr(table.schema_, "columns")
            and table.schema_.columns
        ):
            result.append("| Column Name | Data Type | Mode | Details |")
            result.append("|---------|---------|--------|------|")

            for column in table.schema_.columns:
                result.append(_create_column_table_row(column))

                # Special display for nested fields
                if column.fields:
                    nested_fields_md = _convert_nested_fields_to_markdown(column.fields)
                    result.append(nested_fields_md)

            result.append("\n")  # Empty line after table

        # Separator between tables
        result.append("\n")

    return "\n".join(result)


def _convert_nested_fields_to_markdown(
    fields: List[ColumnSchema], indent_level: int = 0
) -> str:
    """Convert nested fields to markdown collapsible section"""
    result = []
    indent = "  " * indent_level

    # Start collapsible section
    result.append("<details><summary>▶︎ View nested fields</summary>\n")

    # Display each nested field as bullet point
    for field in fields:
        description = f": {field.description}" if field.description else ""
        result.append(
            f"{indent}- **{field.name}** ({field.type}, {field.mode}){description}"
        )

        # Recursively process if further nesting exists
        if field.fields:
            result.append(
                f"{indent}  <details><summary>▶︎ {field.name} details</summary>\n"
            )

            # Second level and deeper nesting
            for nested_field in field.fields:
                nested_desc = (
                    f": {nested_field.description}" if nested_field.description else ""
                )
                result.append(
                    f"{indent}  - **{nested_field.name}** ({nested_field.type}, {nested_field.mode}){nested_desc}"
                )

                # Third level and deeper nesting (recursive processing possible if needed)
                if nested_field.fields:
                    # In current version, third level and deeper are displayed as simple list
                    for deep_nested in nested_field.fields:
                        deep_desc = (
                            f": {deep_nested.description}"
                            if deep_nested.description
                            else ""
                        )
                        result.append(
                            f"{indent}    - {deep_nested.name} ({deep_nested.type}, {deep_nested.mode}){deep_desc}"
                        )

            result.append(f"\n{indent}  </details>\n")

    # End collapsible section
    result.append("\n</details>\n")

    return "\n".join(result)


def convert_search_results_to_markdown(
    query: str, results: List[SearchResultItem]
) -> str:
    """Convert search results to markdown format"""
    result = []

    # Display search query as search results header
    result.append(f"## Search Results: `{query}`\n")
    result.append(f"Found **{len(results)}** results.\n")

    # Group and display by datasets, tables, and columns
    datasets = [r for r in results if r.type == "dataset"]
    tables = [r for r in results if r.type == "table"]
    columns = [r for r in results if r.type == "column"]

    # Dataset search results
    if datasets:
        result.append("### Datasets\n")
        for item in datasets:
            match_info = "name" if item.match_location == "name" else "description"
            result.append(
                f"- **{item.project_id}.{item.dataset_id}** (matched in {match_info})"
            )
        result.append("")

    # Table search results
    if tables:
        result.append("### Tables\n")
        for item in tables:
            match_info = "name" if item.match_location == "name" else "description"
            result.append(
                f"- **{item.project_id}.{item.dataset_id}.{item.table_id}** (matched in {match_info})"
            )
        result.append("")

    # Column search results
    if columns:
        result.append("### Columns\n")
        for item in columns:
            match_info = "name" if item.match_location == "name" else "description"
            result.append(
                f"- **{item.project_id}.{item.dataset_id}.{item.table_id}.{item.column_name}** (matched in {match_info})"
            )
        result.append("")

    return "\n".join(result)


def _create_query_result_table(rows: List[dict]) -> str:
    """Create markdown table from query result rows"""
    if not rows:
        return ""

    # Get column names from first row
    columns = list(rows[0].keys())

    # Create table header
    table_content = "## Query Results\n\n"
    table_content += "| " + " | ".join(columns) + " |\n"
    table_content += "| " + " | ".join(["---"] * len(columns)) + " |\n"

    # Add rows (limited to first 20 rows for readability)
    for row in rows[:20]:
        values = []
        for col in columns:
            value = row.get(col, "")
            # Convert to string and truncate if too long
            str_value = str(value) if value is not None else ""
            if len(str_value) > 50:
                str_value = str_value[:47] + "..."
            values.append(str_value)
        table_content += "| " + " | ".join(values) + " |\n"

    if len(rows) > 20:
        table_content += f"\n*... {len(rows) - 20} more rows*\n"

    return table_content


def _create_execution_info_section(
    result, project_id: str | None, success: bool
) -> str:
    """Create query information section"""
    status_icon = "✅ Success" if success else "❌ Failed"
    return f"""## Query Information
- **Status**: {status_icon}
- **Project ID**: {project_id or "Default"}
- **Job ID**: {getattr(result, "job_id", None) or "N/A"}
- **Execution Time**: {getattr(result, "execution_time_ms", None) or 0} ms
"""


def _create_resource_usage_section(result) -> str:
    """Create resource usage section for successful queries"""
    scan_size = _format_bytes(result.total_bytes_processed or 0)
    bill_size = _format_bytes(result.total_bytes_billed or 0)

    return f"""## Resource Usage
- **Bytes Processed**: {scan_size} ({result.total_bytes_processed or 0:,} bytes)
- **Bytes Billed**: {bill_size} ({result.total_bytes_billed or 0:,} bytes)
- **Rows Returned**: {result.total_rows or 0:,}
"""


def _create_error_section(error_message: str | None) -> str:
    """Create error details section"""
    return f"""## Error Details
```
{error_message or "Unknown error"}
```

## Recommendations
- Check SQL syntax
- Verify that table and column names exist
- Ensure you have proper permissions
- Test with a smaller dataset first
"""


def convert_query_result_to_markdown(
    result: QueryExecutionResult, project_id: str | None = None
) -> str:
    """Convert query execution result to markdown format"""
    markdown_content = "# BigQuery Query Execution Result\n\n"

    if result.success:
        markdown_content += _create_execution_info_section(result, project_id, True)
        markdown_content += "\n" + _create_resource_usage_section(result)

        if result.rows:
            table_content = _create_query_result_table(result.rows)
            markdown_content += "\n" + table_content
    else:
        markdown_content += _create_execution_info_section(result, project_id, False)
        markdown_content += "\n" + _create_error_section(result.error_message)

    return markdown_content


def _get_dry_run_status(result: QueryDryRunResult) -> tuple[str, str]:
    """Get status icon and text for dry run result"""
    if result.error_message:
        return "❌", "Fail"
    elif not result.is_safe:
        return "⚠️", "Caution Required"
    else:
        return "✅", "Safe"


def _create_safety_assessment_section(result: QueryDryRunResult) -> str:
    """Create safety assessment section for dry run"""
    if result.is_safe:
        return """## Safety Assessment
✅ **This query can be executed safely**
- Scan amount is below the configured limit
- It can be executed without issues

## Recommendations
- Execute using the `execute_query` tool if needed
"""
    else:
        return """## Safety Assessment
⚠️ **This query will scan a large amount of data**
- Scan amount exceeds the configured limit
- Consider the following points before execution

## Recommendations
- Add WHERE clause to filter data
- For partitioned tables, specify date range
- Specify only necessary columns in SELECT clause
- Add LIMIT clause to restrict result rows
"""


def convert_dry_run_result_to_markdown(
    result: QueryDryRunResult, project_id: str | None = None
) -> str:
    """Convert dry run result to markdown format"""
    markdown_content = "# BigQuery Query Scan Amount Check Result\n\n"

    if result.error_message:
        markdown_content += f"""## Query Information
- **Status**: ❌ Failed
- **Project ID**: {project_id or "Default"}

{_create_error_section(result.error_message)}"""
    else:
        status_icon, status_text = _get_dry_run_status(result)
        scan_size = _format_bytes(result.total_bytes_processed or 0)
        bill_size = _format_bytes(result.total_bytes_billed or 0)

        markdown_content += f"""## Check Information
- **Status**: {status_icon} {status_text}
- **Project ID**: {project_id or "Default"}

## Expected Resource Usage
- **Bytes to be Processed**: {scan_size} ({result.total_bytes_processed or 0:,} bytes)
- **Bytes to be Billed**: {bill_size} ({result.total_bytes_billed or 0:,} bytes)

{_create_safety_assessment_section(result)}"""

    return markdown_content


def convert_save_result_to_markdown(result: QuerySaveResult) -> str:
    """Convert query save result to markdown format"""
    if result.success:
        # Success case
        file_size = _format_bytes(result.file_size_bytes)
        processed_bytes = ""
        if result.query_bytes_processed is not None:
            processed_bytes = f"- **Data Processed**: {_format_bytes(result.query_bytes_processed)} ({result.query_bytes_processed:,} bytes)\n"

        markdown_content = f"""# Query Result Saved Successfully

- **File Path**: `{result.output_path}`
- **Format**: {result.format.upper()}
- **Rows Saved**: {result.total_rows:,}
- **File Size**: {file_size} ({result.file_size_bytes:,} bytes)
- **Execution Time**: {result.execution_time_ms:,} ms
{processed_bytes}"""
    else:
        # Error case
        markdown_content = f"""# Query Result Save Failed

- **Error**: {result.error_message}
- **Output Path**: `{result.output_path}`
- **Format**: {result.format.upper()}
- **Execution Time**: {result.execution_time_ms:,} ms"""

    return markdown_content
