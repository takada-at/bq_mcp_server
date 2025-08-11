# BigQuery MCP Server

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI-green.svg)](https://fastapi.tiangolo.com/)

Google Cloud BigQueryのデータセット、テーブル、スキーマ情報を取得し、ローカルにキャッシュしてMCP経由で提供するPython製のMCPサーバーです。生成AIなどがBigQueryの構造を迅速に把握し、安全にクエリを実行できるように設計されています。

## 主な機能

- **メタデータ管理**: BigQueryのデータセット、テーブル、カラム情報の取得とキャッシュ
- **キーワード検索**: キャッシュされたメタデータに対するキーワード検索
- **安全なクエリ実行**: 自動的なLIMIT句の付与とコスト制御を備えたSQL実行機能
- **MCP対応**: Model Context Protocol経由でのツール提供

## MCPサーバー

利用可能なツール:

1. `get_datasets` - 全データセットの一覧を取得
2. `get_tables` - 指定データセット内の全テーブルを取得（dataset_idが必須、project_idは任意）
3. `search_metadata` - データセット、テーブル、カラムのメタデータを検索
4. `execute_query` - BigQuery SQLクエリを安全に実行（自動LIMIT付与、コスト制御付き）
5. `check_query_scan_amount` - BigQuery SQLのスキャン量を取得
6. `save_query_result` - BigQuery SQLクエリを実行し、その結果をローカルファイル（CSVまたはJSONL形式）に保存する

### ツール詳細

#### `save_query_result`

`save_query_result` ツールは、ファイル出力機能を備えた高度なクエリ実行機能を提供する：

**パラメータ:**
- `sql`（必須）: 実行するSQLクエリ
- `output_path`（必須）: 結果を保存するローカルファイルパス
- `format`（オプション）: 出力形式 - `"csv"`（デフォルト）または `"jsonl"`
- `project_id`（オプション）: 対象のGCPプロジェクトID
- `include_header`（オプション）: CSV出力時にヘッダー行を含めるかどうか（デフォルト: true）

**主な特徴:**
- **自動LIMIT処理なし**: `execute_query` とは異なり、このツールはSQLクエリに自動的にLIMIT句を追加しない
- **コスト管理**: スキャン量の上限（デフォルト: 1GB）を維持し、高額なクエリを防ぐための安全チェックを実装
- **セキュリティ**: パス検証によりディレクトリトラバーサル攻撃を防止
- **柔軟な出力形式**: CSVとJSONLの両方の出力形式をサポート
- **大規模データセット対応**: スキャン量制限内で効率的に大規模クエリ結果を処理可能

**使用例:**
```sql
-- LIMIT制限なしで全行をエクスポート（ただしスキャン量制限の対象）
SELECT customer_id, order_date, total_amount 
FROM `project.dataset.orders` 
WHERE order_date >= '2024-01-01'
```

**注意:** このツールはLIMIT句を追加しないが、コスト保護のためスキャン量制限は適用される。設定された上限値（デフォルト: 1GB）を超えるスキャン量を必要とするクエリは拒否される。

## インストールと環境設定

### 前提条件

- Python 3.11以上
- Google Cloud Platformアカウント
- BigQuery APIが有効化されたGCPプロジェクト

### インストール
uv

```bash
uv add bq_mcp_server
```

pip

```bash
pip install bq_mcp_server
```

### 依存関係のインストール

本プロジェクトではパッケージ管理に`uv`を使用しています：

```bash
# uvのインストール（未インストールの場合）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 依存関係のインストール
uv sync
```

### 設定値

設定値のリストは以下を参照。

[docs/settings.ja.md](./docs/settings.ja.md)


## MCP設定

Claude Code

```shell
claude mcp add bq_mcp_server -- uvx --from git+https://github.com/takada-at/bq_mcp_server bq_mcp_server --project-ids <your project ids>
```

JSON

```json
{
    "mcpServers": {
        "bq_mcp_server": {
            "command": "uvx",
            "args": [
                "--from",
                "git+https://github.com/takada-at/bq_mcp_server",
                "bq_mcp_server",
                "--project-ids",
                "<your project ids>"
            ]
        }
    }
}
```

## テスト実行

### 全テストの実行

```bash
pytest
```

### 特定のテストファイルを実行

```bash
pytest tests/test_logic.py
```

### 特定のテスト関数を実行

```bash
pytest -k test_function_name
```

### テストカバレッジを確認

```bash
pytest --cov=bq_mcp_server
```

## ローカル開発

### MCPサーバーの起動

```bash
uv run bq_mcp_server
```

### FastAPI REST APIサーバーの起動

```bash
uvicorn bq_mcp_server.adapters.web:app --reload
```

### 開発用コマンド

#### コードフォーマットとリンティング

```bash
# コードフォーマット
ruff format

# リンティングチェック
ruff check

# 自動修正
ruff check --fix
```

#### 依存関係管理

```bash
# 新しい依存関係を追加
uv add <package>

# 開発用依存関係を追加
uv add --dev <package>

# 依存関係を更新
uv sync
```
