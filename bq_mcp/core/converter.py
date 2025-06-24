# converter.py: Converts BigQuery metadata to various formats
from typing import List
from bq_mcp.core.entities import (
    DatasetMetadata,
    TableMetadata,
    ColumnSchema,
    SearchResultItem,
    QueryExecutionResult,
    QueryDryRunResult,
)


def convert_datasets_to_markdown(datasets: List[DatasetMetadata]) -> str:
    """データセットメタデータリストをマークダウン形式に変換する"""
    result = []
    for dataset in datasets:
        # データセット名をヘッダーとして追加
        result.append(f"## Dataset: `{dataset.project_id}.{dataset.dataset_id}`\n")

        # データセットの説明があれば追加
        if dataset.description:
            result.append(f"{dataset.description}\n")

        # データセットのロケーションがあれば追加
        if dataset.location:
            result.append(f"**Location:** {dataset.location}\n")

    return "\n".join(result)


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
        if (
            hasattr(table, "schema_")
            and table.schema_
            and hasattr(table.schema_, "columns")
            and table.schema_.columns
        ):
            result.append("| カラム名 | データ型 | モード | 詳細 |")
            result.append("|---------|---------|--------|------|")

            for column in table.schema_.columns:
                desc = column.description or ""

                # ネストされたフィールドがある場合は特別な表示
                if column.fields:
                    result.append(
                        f"| {column.name} | {column.type} | {column.mode} | {desc} |"
                    )
                    # 折りたたみセクションとしてネストを表示
                    nested_fields_md = _convert_nested_fields_to_markdown(column.fields)
                    result.append(nested_fields_md)
                else:
                    result.append(
                        f"| {column.name} | {column.type} | {column.mode} | {desc} |"
                    )

            result.append("\n")  # テーブル後の空行

        # テーブル間の区切り
        result.append("\n")

    return "\n".join(result)


def _convert_nested_fields_to_markdown(
    fields: List[ColumnSchema], indent_level: int = 0
) -> str:
    """ネストされたフィールドをマークダウン形式の折りたたみセクションに変換する"""
    result = []
    indent = "  " * indent_level

    # 折りたたみセクションの開始
    result.append("<details><summary>▶︎ ネストを見る</summary>\n")

    # ネストされた各フィールドを箇条書きで表示
    for field in fields:
        description = f": {field.description}" if field.description else ""
        result.append(
            f"{indent}- **{field.name}** ({field.type}, {field.mode}){description}"
        )

        # さらにネストがある場合は再帰的に処理
        if field.fields:
            result.append(
                f"{indent}  <details><summary>▶︎ {field.name} の詳細</summary>\n"
            )

            # 2階層目以降のネスト
            for nested_field in field.fields:
                nested_desc = (
                    f": {nested_field.description}" if nested_field.description else ""
                )
                result.append(
                    f"{indent}  - **{nested_field.name}** ({nested_field.type}, {nested_field.mode}){nested_desc}"
                )

                # 3階層目以降のネスト（必要に応じて再帰処理も可能）
                if nested_field.fields:
                    # 現在のバージョンでは3階層目以降は単純なリストで表示
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

    # 折りたたみセクションの終了
    result.append("\n</details>\n")

    return "\n".join(result)


def convert_search_results_to_markdown(
    query: str, results: List[SearchResultItem]
) -> str:
    """検索結果をマークダウン形式に変換する"""
    result = []

    # 検索結果のヘッダーとして検索クエリを表示
    result.append(f"## 検索結果: `{query}`\n")
    result.append(f"**{len(results)}** 件のヒットがありました。\n")

    # データセット、テーブル、カラムごとにグループ化して表示
    datasets = [r for r in results if r.type == "dataset"]
    tables = [r for r in results if r.type == "table"]
    columns = [r for r in results if r.type == "column"]

    # データセットの検索結果
    if datasets:
        result.append("### データセット\n")
        for item in datasets:
            match_info = "名前" if item.match_location == "name" else "説明"
            result.append(
                f"- **{item.project_id}.{item.dataset_id}** ({match_info}に一致)"
            )
        result.append("")

    # テーブルの検索結果
    if tables:
        result.append("### テーブル\n")
        for item in tables:
            match_info = "名前" if item.match_location == "name" else "説明"
            result.append(
                f"- **{item.project_id}.{item.dataset_id}.{item.table_id}** ({match_info}に一致)"
            )
        result.append("")

    # カラムの検索結果
    if columns:
        result.append("### カラム\n")
        for item in columns:
            match_info = "名前" if item.match_location == "name" else "説明"
            result.append(
                f"- **{item.project_id}.{item.dataset_id}.{item.table_id}.{item.column_name}** ({match_info}に一致)"
            )
        result.append("")

    return "\n".join(result)


def convert_query_result_to_markdown(
    result: QueryExecutionResult, project_id: str = None
) -> str:
    """クエリ実行結果をマークダウン形式に変換する"""

    def format_bytes(bytes_count: int) -> str:
        """バイト数を人間が読みやすい形式にフォーマットする"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"

    if result.success:
        # 成功時の結果をフォーマット
        scan_size = format_bytes(result.total_bytes_processed or 0)
        bill_size = format_bytes(result.total_bytes_billed or 0)

        # クエリ結果のテーブルを作成
        table_content = ""
        if result.rows and len(result.rows) > 0:
            # 最初の行からカラム名を取得
            columns = list(result.rows[0].keys())

            # テーブルヘッダーを作成
            table_content = "## クエリ結果\n\n"
            table_content += "| " + " | ".join(columns) + " |\n"
            table_content += "| " + " | ".join(["---"] * len(columns)) + " |\n"

            # 行を追加（可読性のため最初の20行に制限）
            for row in result.rows[:20]:
                values = []
                for col in columns:
                    value = row.get(col, "")
                    # 文字列に変換し、長すぎる場合は切り詰める
                    str_value = str(value) if value is not None else ""
                    if len(str_value) > 50:
                        str_value = str_value[:47] + "..."
                    values.append(str_value)
                table_content += "| " + " | ".join(values) + " |\n"

            if len(result.rows) > 20:
                table_content += f"\n*... さらに {len(result.rows) - 20} 行*\n"

        markdown_content = f"""# BigQuery クエリ実行結果

## クエリ情報
- **ステータス**: ✅ 成功
- **プロジェクトID**: {project_id or "デフォルト"}
- **ジョブID**: {result.job_id or "N/A"}
- **実行時間**: {result.execution_time_ms or 0} ms

## リソース使用量
- **処理バイト数**: {scan_size} ({result.total_bytes_processed or 0:,} bytes)
- **課金バイト数**: {bill_size} ({result.total_bytes_billed or 0:,} bytes)
- **返された行数**: {result.total_rows or 0:,}

{table_content}
"""
    else:
        # エラー時の結果をフォーマット
        markdown_content = f"""# BigQuery クエリ実行結果

## クエリ情報
- **ステータス**: ❌ 失敗
- **プロジェクトID**: {project_id or "デフォルト"}
- **実行時間**: {result.execution_time_ms or 0} ms

## エラー詳細
```
{result.error_message or "不明なエラー"}
```

## 推奨事項
- SQLの構文を確認してください
- テーブル名とカラム名が存在することを確認してください
- 適切な権限があることを確認してください
- まず小さなデータセットでテストしてください
"""

    return markdown_content


def convert_dry_run_result_to_markdown(
    result: QueryDryRunResult, project_id: str = None
) -> str:
    """ドライラン結果をマークダウン形式に変換する"""

    def format_bytes(bytes_count: int) -> str:
        """バイト数を人間が読みやすい形式にフォーマットする"""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} PB"

    scan_size = format_bytes(result.total_bytes_processed or 0)
    bill_size = format_bytes(result.total_bytes_billed or 0)

    status_icon = "✅" if result.is_safe else "⚠️"
    status_text = "安全" if result.is_safe else "注意が必要"

    markdown_content = f"""# BigQuery クエリ スキャン量チェック結果

## チェック情報
- **ステータス**: {status_icon} {status_text}
- **プロジェクトID**: {project_id or "デフォルト"}

## 予想リソース使用量
- **処理予定バイト数**: {scan_size} ({result.total_bytes_processed or 0:,} bytes)
- **課金予定バイト数**: {bill_size} ({result.total_bytes_billed or 0:,} bytes)

## 安全性評価
"""

    if result.is_safe:
        markdown_content += """✅ **このクエリは安全に実行できます**
- スキャン量が設定された制限値以下です
- そのまま実行しても問題ありません

## 推奨事項
- 必要に応じて `execute_query` ツールで実行してください
"""
    else:
        markdown_content += """⚠️ **このクエリは大量のデータをスキャンします**
- スキャン量が設定された制限値を超えています
- 実行前に以下の点を検討してください

## 推奨事項
- WHERE句を追加してデータ量を絞り込む
- パーティション化されたテーブルの場合、日付範囲を指定する
- SELECT句で必要なカラムのみを指定する
- LIMIT句を追加して結果行数を制限する
"""

    return markdown_content
