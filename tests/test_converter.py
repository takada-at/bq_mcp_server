"""Tests for core/converter.py - markdown conversion functionality"""

from bq_mcp.core import converter
from bq_mcp.core.entities import (
    ColumnSchema,
    DatasetMetadata,
    QueryDryRunResult,
    QueryExecutionResult,
    SearchResultItem,
    TableMetadata,
    TableSchema,
)


def test_convert_tables_to_markdown_empty_list():
    """Test converting empty table list"""
    tables = []
    result = converter.convert_tables_to_markdown(tables)
    assert result == ""


def test_convert_tables_to_markdown_single_table_without_schema():
    """Test converting simple table without schema"""
    table = TableMetadata(
        project_id="test-project",
        dataset_id="test_dataset",
        table_id="test_table",
        full_table_id="test-project.test_dataset.test_table",
        description="Test table",
    )
    result = converter.convert_tables_to_markdown([table])
    assert "### Table: `test-project.test_dataset.test_table`" in result
    assert "Test table" in result


def test_convert_tables_to_markdown_table_with_simple_schema():
    """Test converting table with simple schema"""
    columns = [
        ColumnSchema(name="id", type="INTEGER", mode="REQUIRED", description="ID"),
        ColumnSchema(name="name", type="STRING", mode="NULLABLE", description="Name"),
    ]
    table = TableMetadata(
        project_id="test-project",
        dataset_id="test_dataset",
        table_id="test_table",
        full_table_id="test-project.test_dataset.test_table",
        description="Test table",
        schema_=TableSchema(columns=columns),
    )

    result = converter.convert_tables_to_markdown([table])

    # Check basic information
    assert "### Table: `test-project.test_dataset.test_table`" in result
    assert "Test table" in result

    # Check schema table header
    assert "| Column Name | Data Type | Mode | Details |" in result
    assert "|---------|---------|--------|------|" in result

    # Check column information
    assert "| id | INTEGER | REQUIRED | ID |" in result
    assert "| name | STRING | NULLABLE | Name |" in result


def test_convert_tables_to_markdown_with_nested_fields():
    """Test converting table with nested fields"""
    # Column with nested fields
    nested_fields = [
        ColumnSchema(
            name="street", type="STRING", mode="NULLABLE", description="Street name"
        ),
        ColumnSchema(name="city", type="STRING", mode="NULLABLE", description="City"),
    ]

    # Further nested fields
    deep_nested_fields = [
        ColumnSchema(
            name="first", type="STRING", mode="NULLABLE", description="First name"
        ),
        ColumnSchema(
            name="last", type="STRING", mode="NULLABLE", description="Last name"
        ),
    ]

    columns = [
        ColumnSchema(name="id", type="INTEGER", mode="REQUIRED", description="ID"),
        ColumnSchema(
            name="address",
            type="RECORD",
            mode="NULLABLE",
            description="Address information",
            fields=nested_fields,
        ),
        ColumnSchema(
            name="name",
            type="RECORD",
            mode="NULLABLE",
            description="Full name",
            fields=[
                ColumnSchema(
                    name="full_name",
                    type="RECORD",
                    mode="NULLABLE",
                    description="Full name details",
                    fields=deep_nested_fields,
                )
            ],
        ),
    ]

    table = TableMetadata(
        project_id="test-project",
        dataset_id="test_dataset",
        table_id="test_table",
        full_table_id="test-project.test_dataset.test_table",
        description="Test table",
        schema_=TableSchema(columns=columns),
    )

    result = converter.convert_tables_to_markdown([table])

    # Check basic information
    assert "### Table: `test-project.test_dataset.test_table`" in result

    # Check display of nested fields
    assert "<details><summary>▶︎ View nested fields</summary>" in result
    assert "- **street** (STRING, NULLABLE): Street name" in result
    assert "- **city** (STRING, NULLABLE): City" in result

    # Test deep nesting
    assert "<details><summary>▶︎ full_name details</summary>" in result
    assert "- **first** (STRING, NULLABLE): First name" in result
    assert "- **last** (STRING, NULLABLE): Last name" in result


def test_convert_nested_fields_to_markdown():
    """Unit test for _convert_nested_fields_to_markdown method"""
    fields = [
        ColumnSchema(
            name="item_id",
            type="INTEGER",
            mode="REQUIRED",
            description="Item ID",
        ),
        ColumnSchema(
            name="item_name",
            type="STRING",
            mode="NULLABLE",
            description="Item name",
        ),
    ]

    result = converter._convert_nested_fields_to_markdown(fields)

    assert "<details><summary>▶︎ View nested fields</summary>" in result
    assert "- **item_id** (INTEGER, REQUIRED): Item ID" in result
    assert "- **item_name** (STRING, NULLABLE): Item name" in result
    assert "</details>" in result


def test_convert_nested_fields_to_markdown_with_indentation():
    """Test _convert_nested_fields_to_markdown method with indentation level"""
    fields = [
        ColumnSchema(
            name="test_field",
            type="STRING",
            mode="NULLABLE",
            description="Test field",
        )
    ]

    # Test with indentation level 2
    result = converter._convert_nested_fields_to_markdown(fields, indent_level=2)

    assert "    - **test_field** (STRING, NULLABLE): Test field" in result


def test_convert_tables_to_markdown_multiple_tables():
    """Test conversion of multiple tables"""
    table1 = TableMetadata(
        project_id="test-project",
        dataset_id="test_dataset",
        table_id="table1",
        full_table_id="test-project.test_dataset.table1",
        description="Table 1",
    )

    table2 = TableMetadata(
        project_id="test-project",
        dataset_id="test_dataset",
        table_id="table2",
        full_table_id="test-project.test_dataset.table2",
        description="Table 2",
    )

    result = converter.convert_tables_to_markdown([table1, table2])

    assert "### Table: `test-project.test_dataset.table1`" in result
    assert "Table 1" in result
    assert "### Table: `test-project.test_dataset.table2`" in result
    assert "Table 2" in result


class TestFormatBytes:
    """Test _format_bytes function"""

    def test_format_bytes_small_values(self):
        """Test formatting small byte values"""
        assert converter._format_bytes(100) == "100.0 B"
        assert converter._format_bytes(500) == "500.0 B"
        assert converter._format_bytes(1023) == "1023.0 B"

    def test_format_bytes_kilobytes(self):
        """Test formatting kilobyte values"""
        assert converter._format_bytes(1024) == "1.0 KB"
        assert converter._format_bytes(1536) == "1.5 KB"
        assert converter._format_bytes(2048) == "2.0 KB"

    def test_format_bytes_megabytes(self):
        """Test formatting megabyte values"""
        assert converter._format_bytes(1024 * 1024) == "1.0 MB"
        assert converter._format_bytes(1024 * 1024 * 1.5) == "1.5 MB"

    def test_format_bytes_gigabytes(self):
        """Test formatting gigabyte values"""
        assert converter._format_bytes(1024 * 1024 * 1024) == "1.0 GB"
        assert converter._format_bytes(1024 * 1024 * 1024 * 2.5) == "2.5 GB"

    def test_format_bytes_terabytes(self):
        """Test formatting terabyte values"""
        assert converter._format_bytes(1024 * 1024 * 1024 * 1024) == "1.0 TB"
        assert converter._format_bytes(1024 * 1024 * 1024 * 1024 * 3.2) == "3.2 TB"

    def test_format_bytes_petabytes(self):
        """Test formatting petabyte values"""
        huge_value = 1024 * 1024 * 1024 * 1024 * 1024
        assert converter._format_bytes(huge_value) == "1.0 PB"
        assert converter._format_bytes(huge_value * 1.7) == "1.7 PB"

    def test_format_bytes_float_input(self):
        """Test formatting with float input"""
        assert converter._format_bytes(1024.5) == "1.0 KB"
        assert converter._format_bytes(1536.7) == "1.5 KB"

    def test_format_bytes_zero(self):
        """Test formatting zero bytes"""
        assert converter._format_bytes(0) == "0.0 B"


class TestAddOptionalField:
    """Test _add_optional_field function"""

    def test_add_optional_field_with_value(self):
        """Test adding field when value exists"""
        result = []
        converter._add_optional_field(result, "Location", "US")
        assert result == ["**Location:** US\n"]

    def test_add_optional_field_with_none(self):
        """Test not adding field when value is None"""
        result = []
        converter._add_optional_field(result, "Location", None)
        assert result == []

    def test_add_optional_field_with_empty_string(self):
        """Test not adding field when value is empty string"""
        result = []
        converter._add_optional_field(result, "Location", "")
        assert result == []

    def test_add_optional_field_with_format_func(self):
        """Test adding field with format function"""
        result = []
        converter._add_optional_field(
            result, "Size", 1024, format_func=converter._format_bytes
        )
        assert result == ["**Size:** 1.0 KB\n"]

    def test_add_optional_field_multiple_calls(self):
        """Test adding multiple fields"""
        result = []
        converter._add_optional_field(result, "Location", "US")
        converter._add_optional_field(result, "Description", "Test dataset")
        converter._add_optional_field(result, "Empty", None)
        assert result == ["**Location:** US\n", "**Description:** Test dataset\n"]


class TestConvertDatasetsToMarkdown:
    """Test convert_datasets_to_markdown function"""

    def test_convert_empty_datasets(self):
        """Test converting empty dataset list"""
        result = converter.convert_datasets_to_markdown([])
        assert result == ""

    def test_convert_single_dataset_minimal(self):
        """Test converting single dataset with minimal fields"""
        dataset = DatasetMetadata(project_id="test_project", dataset_id="test_dataset")
        result = converter.convert_datasets_to_markdown([dataset])
        expected = "## Dataset: `test_project.test_dataset`\n"
        assert result == expected

    def test_convert_single_dataset_with_description(self):
        """Test converting single dataset with description"""
        dataset = DatasetMetadata(
            project_id="test_project",
            dataset_id="test_dataset",
            description="Test dataset description",
        )
        result = converter.convert_datasets_to_markdown([dataset])
        expected = (
            "## Dataset: `test_project.test_dataset`\n\nTest dataset description\n"
        )
        assert result == expected

    def test_convert_single_dataset_with_location(self):
        """Test converting single dataset with location"""
        dataset = DatasetMetadata(
            project_id="test_project", dataset_id="test_dataset", location="US"
        )
        result = converter.convert_datasets_to_markdown([dataset])
        expected = "## Dataset: `test_project.test_dataset`\n\n**Location:** US\n"
        assert result == expected

    def test_convert_single_dataset_complete(self):
        """Test converting single dataset with all fields"""
        dataset = DatasetMetadata(
            project_id="test_project",
            dataset_id="test_dataset",
            description="Complete test dataset",
            location="EU",
        )
        result = converter.convert_datasets_to_markdown([dataset])
        expected = "## Dataset: `test_project.test_dataset`\n\nComplete test dataset\n\n**Location:** EU\n"
        assert result == expected

    def test_convert_multiple_datasets(self):
        """Test converting multiple datasets"""
        datasets = [
            DatasetMetadata(
                project_id="project1",
                dataset_id="dataset1",
                description="First dataset",
            ),
            DatasetMetadata(
                project_id="project2", dataset_id="dataset2", location="US"
            ),
        ]
        result = converter.convert_datasets_to_markdown(datasets)
        expected = (
            "## Dataset: `project1.dataset1`\n\n"
            "First dataset\n\n"
            "## Dataset: `project2.dataset2`\n\n"
            "**Location:** US\n"
        )
        assert result == expected


class TestConvertSearchResultsToMarkdown:
    """Test convert_search_results_to_markdown function"""

    def test_convert_empty_search_results(self):
        """Test converting empty search results"""
        result = converter.convert_search_results_to_markdown("test_query", [])
        expected = "## Search Results: `test_query`\n\nFound **0** results.\n"
        assert result == expected

    def test_convert_dataset_search_results(self):
        """Test converting dataset search results"""
        results = [
            SearchResultItem(
                type="dataset",
                project_id="project1",
                dataset_id="dataset1",
                match_location="name",
            ),
            SearchResultItem(
                type="dataset",
                project_id="project2",
                dataset_id="dataset2",
                match_location="description",
            ),
        ]
        result = converter.convert_search_results_to_markdown("test", results)
        expected = (
            "## Search Results: `test`\n\n"
            "Found **2** results.\n\n"
            "### Datasets\n\n"
            "- **project1.dataset1** (matched in name)\n"
            "- **project2.dataset2** (matched in description)\n"
            ""
        )
        assert result == expected

    def test_convert_table_search_results(self):
        """Test converting table search results"""
        results = [
            SearchResultItem(
                type="table",
                project_id="project1",
                dataset_id="dataset1",
                table_id="table1",
                match_location="name",
            )
        ]
        result = converter.convert_search_results_to_markdown("table", results)
        expected = (
            "## Search Results: `table`\n\n"
            "Found **1** results.\n\n"
            "### Tables\n\n"
            "- **project1.dataset1.table1** (matched in name)\n"
            ""
        )
        assert result == expected

    def test_convert_column_search_results(self):
        """Test converting column search results"""
        results = [
            SearchResultItem(
                type="column",
                project_id="project1",
                dataset_id="dataset1",
                table_id="table1",
                column_name="column1",
                match_location="description",
            )
        ]
        result = converter.convert_search_results_to_markdown("col", results)
        expected = (
            "## Search Results: `col`\n\n"
            "Found **1** results.\n\n"
            "### Columns\n\n"
            "- **project1.dataset1.table1.column1** (matched in description)\n"
            ""
        )
        assert result == expected

    def test_convert_mixed_search_results(self):
        """Test converting mixed search results"""
        results = [
            SearchResultItem(
                type="dataset",
                project_id="project1",
                dataset_id="dataset1",
                match_location="name",
            ),
            SearchResultItem(
                type="table",
                project_id="project1",
                dataset_id="dataset1",
                table_id="table1",
                match_location="description",
            ),
            SearchResultItem(
                type="column",
                project_id="project1",
                dataset_id="dataset1",
                table_id="table1",
                column_name="column1",
                match_location="name",
            ),
        ]
        result = converter.convert_search_results_to_markdown("test", results)
        expected = (
            "## Search Results: `test`\n\n"
            "Found **3** results.\n\n"
            "### Datasets\n\n"
            "- **project1.dataset1** (matched in name)\n"
            "\n"
            "### Tables\n\n"
            "- **project1.dataset1.table1** (matched in description)\n"
            "\n"
            "### Columns\n\n"
            "- **project1.dataset1.table1.column1** (matched in name)\n"
            ""
        )
        assert result == expected


class TestCreateQueryResultTable:
    """Test _create_query_result_table function"""

    def test_create_query_result_table_empty(self):
        """Test creating table from empty results"""
        result = converter._create_query_result_table([])
        assert result == ""

    def test_create_query_result_table_single_row(self):
        """Test creating table from single row"""
        rows = [{"name": "John", "age": 30}]
        result = converter._create_query_result_table(rows)
        expected = "## Query Results\n\n| name | age |\n| --- | --- |\n| John | 30 |\n"
        assert result == expected

    def test_create_query_result_table_multiple_rows(self):
        """Test creating table from multiple rows"""
        rows = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25},
            {"name": "Bob", "age": 35},
        ]
        result = converter._create_query_result_table(rows)
        expected = (
            "## Query Results\n\n"
            "| name | age |\n"
            "| --- | --- |\n"
            "| John | 30 |\n"
            "| Jane | 25 |\n"
            "| Bob | 35 |\n"
        )
        assert result == expected

    def test_create_query_result_table_with_none_values(self):
        """Test creating table with None values"""
        rows = [{"name": "John", "age": None}]
        result = converter._create_query_result_table(rows)
        expected = "## Query Results\n\n| name | age |\n| --- | --- |\n| John |  |\n"
        assert result == expected

    def test_create_query_result_table_with_long_values(self):
        """Test creating table with values longer than 50 characters"""
        long_value = "a" * 60
        rows = [{"name": "John", "description": long_value}]
        result = converter._create_query_result_table(rows)
        expected = (
            "## Query Results\n\n"
            "| name | description |\n"
            "| --- | --- |\n"
            f"| John | {'a' * 47}... |\n"
        )
        assert result == expected

    def test_create_query_result_table_with_missing_columns(self):
        """Test creating table with missing column values"""
        rows = [{"name": "John", "age": 30}, {"name": "Jane"}]
        result = converter._create_query_result_table(rows)
        expected = (
            "## Query Results\n\n"
            "| name | age |\n"
            "| --- | --- |\n"
            "| John | 30 |\n"
            "| Jane |  |\n"
        )
        assert result == expected

    def test_create_query_result_table_more_than_20_rows(self):
        """Test creating table with more than 20 rows (should truncate)"""
        rows = [{"id": i, "name": f"Person{i}"} for i in range(25)]
        result = converter._create_query_result_table(rows)

        # Should only contain first 20 rows
        assert result.count("Person") == 20
        assert "*... 5 more rows*" in result
        assert "Person24" not in result
        assert "Person19" in result


class TestConvertQueryResultToMarkdown:
    """Test convert_query_result_to_markdown function"""

    def test_convert_successful_query_result(self):
        """Test converting successful query result"""
        result = QueryExecutionResult(
            success=True,
            rows=[{"name": "John", "age": 30}],
            total_rows=1,
            total_bytes_processed=1024,
            total_bytes_billed=1024,
            execution_time_ms=100,
            job_id="test_job_123",
            schema=[],
        )
        markdown = converter.convert_query_result_to_markdown(result, "test_project")

        assert "# BigQuery Query Execution Result" in markdown
        assert "✅ Success" in markdown
        assert "test_project" in markdown
        assert "test_job_123" in markdown
        assert "100 ms" in markdown
        assert "1.0 KB" in markdown
        assert "1,024 bytes" in markdown
        assert "1" in markdown  # total_rows
        assert "| name | age |" in markdown
        assert "| John | 30 |" in markdown

    def test_convert_failed_query_result(self):
        """Test converting failed query result"""
        result = QueryExecutionResult(
            success=False,
            error_message="Table not found",
            rows=[],
            total_rows=0,
            schema=[],
        )
        markdown = converter.convert_query_result_to_markdown(result)

        assert "# BigQuery Query Execution Result" in markdown
        assert "❌ Failed" in markdown
        assert "Default" in markdown  # No project_id provided
        assert "Table not found" in markdown
        assert "Check SQL syntax" in markdown
        assert "Verify that table and column names exist" in markdown

    def test_convert_query_result_no_rows(self):
        """Test converting successful query result with no rows"""
        result = QueryExecutionResult(
            success=True,
            rows=[],
            total_rows=0,
            total_bytes_processed=0,
            total_bytes_billed=0,
            schema=[],
        )
        markdown = converter.convert_query_result_to_markdown(result)

        assert "# BigQuery Query Execution Result" in markdown
        assert "✅ Success" in markdown
        assert "0 bytes" in markdown
        assert "0" in markdown  # total_rows
        # Should not contain query results table
        assert "## Query Results" not in markdown


class TestConvertDryRunResultToMarkdown:
    """Test convert_dry_run_result_to_markdown function"""

    def test_convert_safe_dry_run_result(self):
        """Test converting safe dry run result"""
        result = QueryDryRunResult(
            total_bytes_processed=1024,
            total_bytes_billed=1024,
            is_safe=True,
            modified_sql="SELECT * FROM table LIMIT 10",
        )
        markdown = converter.convert_dry_run_result_to_markdown(result, "test_project")

        assert "# BigQuery Query Scan Amount Check Result" in markdown
        assert "✅ Safe" in markdown
        assert "test_project" in markdown
        assert "1.0 KB" in markdown
        assert "1,024 bytes" in markdown
        assert "This query can be executed safely" in markdown
        assert "execute_query" in markdown

    def test_convert_unsafe_dry_run_result(self):
        """Test converting unsafe dry run result"""
        result = QueryDryRunResult(
            total_bytes_processed=1024 * 1024 * 1024,  # 1 GB
            total_bytes_billed=1024 * 1024 * 1024,
            is_safe=False,
            modified_sql="SELECT * FROM large_table",
        )
        markdown = converter.convert_dry_run_result_to_markdown(result)

        assert "# BigQuery Query Scan Amount Check Result" in markdown
        assert "⚠️ Caution Required" in markdown
        assert "Default" in markdown  # No project_id
        assert "1.0 GB" in markdown
        assert "This query will scan a large amount of data" in markdown
        assert "Add WHERE clause" in markdown
        assert "Add LIMIT clause" in markdown

    def test_convert_failed_dry_run_result(self):
        """Test converting failed dry run result"""
        result = QueryDryRunResult(
            error_message="Invalid SQL syntax",
            total_bytes_processed=0,
            total_bytes_billed=0,
            is_safe=False,
            modified_sql="",
        )
        markdown = converter.convert_dry_run_result_to_markdown(result, "test_project")

        assert "# BigQuery Query Scan Amount Check Result" in markdown
        assert "❌ Failed" in markdown
        assert "test_project" in markdown
        assert "Invalid SQL syntax" in markdown
        assert "Check SQL syntax" in markdown
        assert "Verify that table and column names exist" in markdown

    def test_convert_dry_run_result_zero_bytes(self):
        """Test converting dry run result with zero bytes"""
        result = QueryDryRunResult(
            total_bytes_processed=0,
            total_bytes_billed=0,
            is_safe=True,
            modified_sql="SELECT 1",
        )
        markdown = converter.convert_dry_run_result_to_markdown(result)

        assert "0.0 B" in markdown
        assert "0 bytes" in markdown
        assert "✅ Safe" in markdown


class TestDeepNestedFields:
    """Test deeply nested field handling (3+ levels)"""

    def test_convert_table_with_deeply_nested_fields(self):
        """Test converting table with 3+ levels of nested fields"""
        # Create a deeply nested column structure
        deep_nested_field = ColumnSchema(
            name="deep_field",
            type="STRING",
            mode="NULLABLE",
            description="Deep nested field",
        )

        nested_field = ColumnSchema(
            name="nested_field",
            type="RECORD",
            mode="REPEATED",
            description="Nested field",
            fields=[deep_nested_field],
        )

        top_level_field = ColumnSchema(
            name="top_field",
            type="RECORD",
            mode="NULLABLE",
            description="Top level field",
            fields=[nested_field],
        )

        table = TableMetadata(
            project_id="test_project",
            dataset_id="test_dataset",
            table_id="test_table",
            full_table_id="test_project.test_dataset.test_table",
            table_type="TABLE",
            schema_=TableSchema(columns=[top_level_field]),
        )

        result = converter.convert_tables_to_markdown([table])

        # Should contain all three levels of nesting
        assert "| top_field | RECORD | NULLABLE | Top level field |" in result
        assert "**nested_field**" in result
        assert "**deep_field**" in result
        assert "Deep nested field" in result
        assert "<details>" in result
        assert "</details>" in result
        # Third level should be displayed as simple list (not with ** formatting)
        assert "- **deep_field** (STRING, NULLABLE): Deep nested field" in result
