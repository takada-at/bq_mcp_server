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

## インストールと環境設定

### 前提条件

- Python 3.11以上
- Google Cloud Platformアカウント
- BigQuery APIが有効化されたGCPプロジェクト

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

```json
{
    "mcpServers": {
        "bq_mcp_server": {
            "command": "uv",
            "args": [
                "run",
                "--directory",
                "<your install directory>",
                "mcp_server"
            ],
            "env": {
                "PYTHONPATH": "<your install directory>",
                "PROJECT_IDS": "<your project id>"
            }
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
uv run mcp_server
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
