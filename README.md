# BigQuery MCP Server

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI-green.svg)](https://fastapi.tiangolo.com/)

Google Cloud BigQueryのデータセット、テーブル、スキーマ情報を取得し、ローカルにキャッシュしてMCP経由で提供するPythonのMCPサーバーです。生成AIなどがBigQueryの構造を迅速に把握し、安全にクエリを実行することを目的としています。

## 主な機能

- **メタデータ管理**: BigQueryのデータセット、テーブル、カラム情報の取得とキャッシュ
- **全文検索**: キャッシュされたメタデータに対するキーワード検索
- **安全なクエリ実行**: 自動的なLIMIT句付与とコスト制御を備えたSQL実行機能
- **MCP対応**: Model Context Protocol経由でのツール提供
- **REST API**: FastAPIベースのWebエンドポイント

## MCPサーバー

利用可能なツール:

1. `get_datasets` - 全データセットのリストを取得
2. `get_tables` - 指定データセット内の全テーブルを取得（dataset_id必須、project_id任意）
3. `search_metadata` - データセット、テーブル、カラムのメタデータを検索
4. `execute_query` - BigQuery SQLクエリの安全な実行（自動LIMIT付与、コスト制御付き）
5. `check_query_scan_amount` - BigQuery SQLのスキャン量取得

## インストールと環境設定

### 前提条件

- Python 3.11以上
- Google Cloud Platform アカウント
- BigQuery API が有効化されたGCPプロジェクト

### 依存関係のインストール

このプロジェクトでは `uv` を使用してパッケージ管理を行います：

```bash
# uvのインストール（未インストールの場合）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 依存関係のインストール
uv sync
```

### 環境変数の設定

```bash
# 環境変数ファイルを生成
python scripts/generate_env_example.py

# .env.exampleを.envにコピーして編集
cp .env.example .env
```

必要な環境変数：

- `PROJECT_IDS` - カンマ区切りのGCPプロジェクトIDリスト

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
pytest --cov=bq_meta_api
```

## ローカル開発

### MCPサーバーの起動

```bash
python -m bq_meta_api.adapters.mcp_server
```

### FastAPI REST APIサーバーの起動

```bash
python -m bq_meta_api.adapters.web
```

### Gradio Webインターフェースの起動

```bash
python -m bq_meta_api.adapters.bq_agent_gradio
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

#### 複雑度解析

```bash
# 循環的複雑度の分析
lizard
```

#### 依存関係管理

```bash
# 新しい依存関係を追加
uv add <package>

# 開発用依存関係を追加
uv add --dev <package>

# 依存関係の更新
uv sync
```
