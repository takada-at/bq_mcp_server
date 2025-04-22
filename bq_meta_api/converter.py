# converter.py: Converts BigQuery metadata to various formats
from typing import List, Optional
from bq_meta_api.models import TableMetadata, ColumnSchema

class MarkdownConverter:
    """BigQueryメタデータをマークダウン形式に変換するためのクラス"""
    
    @staticmethod
    def convert_tables_to_markdown(tables: List[TableMetadata]) -> str:
        """テーブルメタデータリストをマークダウン形式に変換する"""
        result = []
        
        for table in tables:
            # テーブル名をヘッダーとして追加
            result.append(f"### Table: `{table.full_table_id}`\n")
            
            # テーブルの説明があれば追加
            if table.description:
                result.append(f"{table.description}\n")
            
            # スキーマ情報があればテーブル形式で表示
            if hasattr(table, 'schema_') and table.schema_ and hasattr(table.schema_, 'columns') and table.schema_.columns:
                result.append("| カラム名 | データ型 | モード | 詳細 |")
                result.append("|---------|---------|--------|------|")
                
                for column in table.schema_.columns:
                    desc = column.description or ""
                    
                    # ネストされたフィールドがある場合は特別な表示
                    if column.fields:
                        result.append(f"| {column.name} | {column.type} | {column.mode} | {desc} |")
                        # 折りたたみセクションとしてネストを表示
                        nested_fields_md = MarkdownConverter._convert_nested_fields_to_markdown(column.fields)
                        result.append(nested_fields_md)
                    else:
                        result.append(f"| {column.name} | {column.type} | {column.mode} | {desc} |")
                
                result.append("\n")  # テーブル後の空行
            
            # テーブル間の区切り
            result.append("\n")
        
        return "\n".join(result)
    
    @staticmethod
    def _convert_nested_fields_to_markdown(fields: List[ColumnSchema], indent_level: int = 0) -> str:
        """ネストされたフィールドをマークダウン形式の折りたたみセクションに変換する"""
        result = []
        indent = "  " * indent_level

        # 折りたたみセクションの開始
        result.append("<details><summary>▶︎ ネストを見る</summary>\n")
        
        # ネストされた各フィールドを箇条書きで表示
        for field in fields:
            description = f": {field.description}" if field.description else ""
            result.append(f"{indent}- **{field.name}** ({field.type}, {field.mode}){description}")
            
            # さらにネストがある場合は再帰的に処理
            if field.fields:
                result.append(f"{indent}  <details><summary>▶︎ {field.name} の詳細</summary>\n")
                
                # 2階層目以降のネスト
                for nested_field in field.fields:
                    nested_desc = f": {nested_field.description}" if nested_field.description else ""
                    result.append(f"{indent}  - **{nested_field.name}** ({nested_field.type}, {nested_field.mode}){nested_desc}")
                    
                    # 3階層目以降のネスト（必要に応じて再帰処理も可能）
                    if nested_field.fields:
                        # 現在のバージョンでは3階層目以降は単純なリストで表示
                        for deep_nested in nested_field.fields:
                            deep_desc = f": {deep_nested.description}" if deep_nested.description else ""
                            result.append(f"{indent}    - {deep_nested.name} ({deep_nested.type}, {deep_nested.mode}){deep_desc}")
                
                result.append(f"\n{indent}  </details>\n")
        
        # 折りたたみセクションの終了
        result.append("\n</details>\n")
        
        return "\n".join(result)