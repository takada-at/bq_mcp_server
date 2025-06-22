# BigQuery Metadata API Server

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI-green.svg)](https://fastapi.tiangolo.com/)

Google Cloud BigQueryのデータセット、テーブル、スキーマ情報を取得し、ローカルにキャッシュしてMCP経由で提供するPythonサーバーです。生成AIなどがBigQueryの構造を迅速に把握し、安全にクエリを実行することを目的としています。

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

