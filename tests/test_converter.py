from bq_mcp.core import converter
from bq_mcp.core.entities import TableMetadata, TableSchema, ColumnSchema


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
