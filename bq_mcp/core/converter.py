# converter.py: Converts BigQuery metadata to various formats
from typing import List

from bq_mcp.core.entities import (
    ColumnSchema,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    SearchResultItem,
    TableMetadata,
)


def convert_datasets_to_markdown(datasets: List[DatasetMetadata]) -> str:
    """Convert dataset metadata list to markdown format"""
    result = []
    for dataset in datasets:
        # Add dataset name as header
        result.append(f"## Dataset: `{dataset.project_id}.{dataset.dataset_id}`\n")

        # Add dataset description if exists
        if dataset.description:
            result.append(f"{dataset.description}\n")

        # Add dataset location if exists
        if dataset.location:
            result.append(f"**Location:** {dataset.location}\n")

    return "\n".join(result)


def convert_tables_to_markdown(tables: List[TableMetadata]) -> str:
    """Convert table metadata list to markdown format"""
    result = []

    for table in tables:
        # Add table name as header
        result.append(f"### Table: `{table.full_table_id}`\n")

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
                desc = column.description or ""

                # Special display for nested fields
                if column.fields:
                    result.append(
                        f"| {column.name} | {column.type} | {column.mode} | {desc} |"
                    )
                    # Display nested fields as collapsible section
                    nested_fields_md = _convert_nested_fields_to_markdown(column.fields)
                    result.append(nested_fields_md)
                else:
                    result.append(
                        f"| {column.name} | {column.type} | {column.mode} | {desc} |"
                    )

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


def convert_query_result_to_markdown(
    result: QueryExecutionResult, project_id: str = None
) -> str:
    """Convert query execution result to markdown format"""

    def format_bytes(bytes_count: int) -> str:
        """Format byte count to human-readable format"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"

    if result.success:
        # Format results on success
        scan_size = format_bytes(result.total_bytes_processed or 0)
        bill_size = format_bytes(result.total_bytes_billed or 0)

        # Create query result table
        table_content = ""
        if result.rows and len(result.rows) > 0:
            # Get column names from first row
            columns = list(result.rows[0].keys())

            # Create table header
            table_content = "## Query Results\n\n"
            table_content += "| " + " | ".join(columns) + " |\n"
            table_content += "| " + " | ".join(["---"] * len(columns)) + " |\n"

            # Add rows (limited to first 20 rows for readability)
            for row in result.rows[:20]:
                values = []
                for col in columns:
                    value = row.get(col, "")
                    # Convert to string and truncate if too long
                    str_value = str(value) if value is not None else ""
                    if len(str_value) > 50:
                        str_value = str_value[:47] + "..."
                    values.append(str_value)
                table_content += "| " + " | ".join(values) + " |\n"

            if len(result.rows) > 20:
                table_content += f"\n*... {len(result.rows) - 20} more rows*\n"

        markdown_content = f"""# BigQuery Query Execution Result

## Query Information
- **Status**: ✅ Success
- **Project ID**: {project_id or "Default"}
- **Job ID**: {result.job_id or "N/A"}
- **Execution Time**: {result.execution_time_ms or 0} ms

## Resource Usage
- **Bytes Processed**: {scan_size} ({result.total_bytes_processed or 0:,} bytes)
- **Bytes Billed**: {bill_size} ({result.total_bytes_billed or 0:,} bytes)
- **Rows Returned**: {result.total_rows or 0:,}

{table_content}
"""
    else:
        # Format results on error
        markdown_content = f"""# BigQuery Query Execution Result

## Query Information
- **Status**: ❌ Failed
- **Project ID**: {project_id or "Default"}
- **Execution Time**: {result.execution_time_ms or 0} ms

## Error Details
```
{result.error_message or "Unknown error"}
```

## Recommendations
- Check SQL syntax
- Verify that table and column names exist
- Ensure you have proper permissions
- Test with a smaller dataset first
"""

    return markdown_content


def convert_dry_run_result_to_markdown(
    result: QueryDryRunResult, project_id: str = None
) -> str:
    """Convert dry run result to markdown format"""

    def format_bytes(bytes_count: int) -> str:
        """Format byte count to human-readable format"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"

    scan_size = format_bytes(result.total_bytes_processed or 0)
    bill_size = format_bytes(result.total_bytes_billed or 0)

    if result.error_message:
        status_icon = "❌"
        status_text = "Fail"
    elif not result.is_safe:
        status_icon = "⚠️"
        status_text = "Caution Required"
    else:
        status_icon = "✅"
        status_text = "Safe"

    if result.error_message:
        return f"""# BigQuery Query Scan Amount Check Result

## Query Information
- **Status**: ❌ Failed
- **Project ID**: {project_id or "Default"}

## Error Details
```
{result.error_message or "Unknown error"}
```

## Recommendations
- Check SQL syntax
- Verify that table and column names exist
- Ensure you have proper permissions
- Test with a smaller dataset first"""
    else:
        markdown_content = f"""# BigQuery Query Scan Amount Check Result

## Check Information
- **Status**: {status_icon} {status_text}
- **Project ID**: {project_id or "Default"}

## Expected Resource Usage
- **Bytes to be Processed**: {scan_size} ({result.total_bytes_processed or 0:,} bytes)
- **Bytes to be Billed**: {bill_size} ({result.total_bytes_billed or 0:,} bytes)

## Safety Assessment
"""

    if result.is_safe:
        markdown_content += """✅ **This query can be executed safely**
- Scan amount is below the configured limit
- It can be executed without issues

## Recommendations
- Execute using the `execute_query` tool if needed
"""
    else:
        markdown_content += """⚠️ **This query will scan a large amount of data**
- Scan amount exceeds the configured limit
- Consider the following points before execution

## Recommendations
- Add WHERE clause to filter data
- For partitioned tables, specify date range
- Specify only necessary columns in SELECT clause
- Add LIMIT clause to restrict result rows
"""

    return markdown_content
