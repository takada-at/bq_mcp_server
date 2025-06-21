# BigQuery Metadata API Server

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI-green.svg)](https://fastapi.tiangolo.com/)

Google Cloud BigQueryのデータセット、テーブル、スキーマ情報を取得し、ローカルにキャッシュしてMCP経由で提供するPythonサーバーです。生成AIなどがBigQueryの構造を迅速に把握することを目的としています。

## MCPサーバー

利用可能なツール:

1. `get_datasets` - 全データセットのリストを取得
2. `get_tables` - 指定データセット内の全テーブルを取得（dataset_id必須、project_id任意）
3. `search_metadata` - データセット、テーブル、カラムのメタデータを検索

