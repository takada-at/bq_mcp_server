import pytest
from bq_meta_api.models import TableMetadata, TableSchema, ColumnSchema
from bq_meta_api.converter import MarkdownConverter


class TestMarkdownConverter:
    def test_convert_tables_to_markdown_empty_list(self):
        """空のテーブルリストを変換するテスト"""
        tables = []
        result = MarkdownConverter.convert_tables_to_markdown(tables)
        assert result == ""

    def test_convert_tables_to_markdown_single_table_without_schema(self):
        """スキーマがないシンプルなテーブルを変換するテスト"""
        table = TableMetadata(
            project_id="test-project",
            dataset_id="test_dataset",
            table_id="test_table",
            full_table_id="test-project.test_dataset.test_table",
            description="テスト用テーブル",
        )
        result = MarkdownConverter.convert_tables_to_markdown([table])
        assert "### Table: `test-project.test_dataset.test_table`" in result
        assert "テスト用テーブル" in result

    def test_convert_tables_to_markdown_table_with_simple_schema(self):
        """シンプルなスキーマを持つテーブルを変換するテスト"""
        columns = [
            ColumnSchema(name="id", type="INTEGER", mode="REQUIRED", description="ID"),
            ColumnSchema(
                name="name", type="STRING", mode="NULLABLE", description="名前"
            ),
        ]
        table = TableMetadata(
            project_id="test-project",
            dataset_id="test_dataset",
            table_id="test_table",
            full_table_id="test-project.test_dataset.test_table",
            description="テスト用テーブル",
            schema_=TableSchema(columns=columns),
        )

        result = MarkdownConverter.convert_tables_to_markdown([table])

        # 基本情報の確認
        assert "### Table: `test-project.test_dataset.test_table`" in result
        assert "テスト用テーブル" in result

        # スキーマテーブルヘッダーの確認
        assert "| カラム名 | データ型 | モード | 詳細 |" in result
        assert "|---------|---------|--------|------|" in result

        # カラム情報の確認
        assert "| id | INTEGER | REQUIRED | ID |" in result
        assert "| name | STRING | NULLABLE | 名前 |" in result

    def test_convert_tables_to_markdown_with_nested_fields(self):
        """ネストされたフィールドを持つテーブルを変換するテスト"""
        # ネストされたフィールドを持つカラム
        nested_fields = [
            ColumnSchema(
                name="street", type="STRING", mode="NULLABLE", description="通り名"
            ),
            ColumnSchema(
                name="city", type="STRING", mode="NULLABLE", description="市区町村"
            ),
        ]

        # さらにネストされたフィールド
        deep_nested_fields = [
            ColumnSchema(
                name="first", type="STRING", mode="NULLABLE", description="名"
            ),
            ColumnSchema(name="last", type="STRING", mode="NULLABLE", description="姓"),
        ]

        columns = [
            ColumnSchema(name="id", type="INTEGER", mode="REQUIRED", description="ID"),
            ColumnSchema(
                name="address",
                type="RECORD",
                mode="NULLABLE",
                description="住所情報",
                fields=nested_fields,
            ),
            ColumnSchema(
                name="name",
                type="RECORD",
                mode="NULLABLE",
                description="氏名",
                fields=[
                    ColumnSchema(
                        name="full_name",
                        type="RECORD",
                        mode="NULLABLE",
                        description="フルネーム",
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
            description="テスト用テーブル",
            schema_=TableSchema(columns=columns),
        )

        result = MarkdownConverter.convert_tables_to_markdown([table])

        # 基本情報の確認
        assert "### Table: `test-project.test_dataset.test_table`" in result

        # ネストされたフィールドの表示を確認
        assert "<details><summary>▶︎ ネストを見る</summary>" in result
        assert "- **street** (STRING, NULLABLE): 通り名" in result
        assert "- **city** (STRING, NULLABLE): 市区町村" in result

        # 深いネストのテスト
        assert "<details><summary>▶︎ full_name の詳細</summary>" in result
        assert "- **first** (STRING, NULLABLE): 名" in result
        assert "- **last** (STRING, NULLABLE): 姓" in result

    def test_convert_nested_fields_to_markdown(self):
        """_convert_nested_fields_to_markdownメソッドの単体テスト"""
        fields = [
            ColumnSchema(
                name="item_id",
                type="INTEGER",
                mode="REQUIRED",
                description="アイテムID",
            ),
            ColumnSchema(
                name="item_name",
                type="STRING",
                mode="NULLABLE",
                description="アイテム名",
            ),
        ]

        result = MarkdownConverter._convert_nested_fields_to_markdown(fields)

        assert "<details><summary>▶︎ ネストを見る</summary>" in result
        assert "- **item_id** (INTEGER, REQUIRED): アイテムID" in result
        assert "- **item_name** (STRING, NULLABLE): アイテム名" in result
        assert "</details>" in result

    def test_convert_nested_fields_to_markdown_with_indentation(self):
        """インデントレベル付きの_convert_nested_fields_to_markdownメソッドのテスト"""
        fields = [
            ColumnSchema(
                name="test_field",
                type="STRING",
                mode="NULLABLE",
                description="テストフィールド",
            )
        ]

        # インデントレベル2でテスト
        result = MarkdownConverter._convert_nested_fields_to_markdown(
            fields, indent_level=2
        )

        assert "    - **test_field** (STRING, NULLABLE): テストフィールド" in result

    def test_convert_tables_to_markdown_multiple_tables(self):
        """複数テーブルの変換をテスト"""
        table1 = TableMetadata(
            project_id="test-project",
            dataset_id="test_dataset",
            table_id="table1",
            full_table_id="test-project.test_dataset.table1",
            description="テーブル1",
        )

        table2 = TableMetadata(
            project_id="test-project",
            dataset_id="test_dataset",
            table_id="table2",
            full_table_id="test-project.test_dataset.table2",
            description="テーブル2",
        )

        result = MarkdownConverter.convert_tables_to_markdown([table1, table2])

        assert "### Table: `test-project.test_dataset.table1`" in result
        assert "テーブル1" in result
        assert "### Table: `test-project.test_dataset.table2`" in result
        assert "テーブル2" in result
