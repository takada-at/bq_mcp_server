# BQ MCPサーバー設定

BQ MCPサーバーで利用可能なすべての設定項目について説明します。
設定値はコマンドライン引数または環境変数によって指定可能です。

## 設定の優先順位

コマンドライン引数と環境変数の両方で設定値が指定された場合、コマンドライン引数の設定値が優先されます。

## 設定項目

### --gcp-service-account-key-path

**説明**: GCPサービスアカウントのJSONキーファイルのパス (省略時はApplication Default Credentialsを使用)

**環境変数**: `GCP_SERVICE_ACCOUNT_KEY_PATH`

**使用方法**:
```bash
python -m bq_mcp_server.adapters.mcp_server --gcp-service-account-key-path "value"
```

### --project-ids

**説明**: GCPプロジェクトIDのカンマ区切りリスト（例: 'project1,project2'）

**環境変数**: `PROJECT_IDS`
  - デフォルト値: 必須（デフォルト値なし）

**使用方法**:
```bash
python -m bq_mcp_server.adapters.mcp_server --project-ids "value"
```

### --dataset-filters

**説明**: データセットフィルタのカンマ区切りリスト（例: 'project1.*,project2.dataset1'）

**環境変数**: `DATASET_FILTERS`

**使用方法**:
```bash
python -m bq_mcp_server.adapters.mcp_server --dataset-filters "value"
```

### --cache-ttl-seconds

**説明**: キャッシュの有効期限（秒単位）（デフォルト: 3600秒）

**型**: `int`

**環境変数**: `CACHE_TTL_SECONDS`
  - デフォルト値: `3600秒`

**使用方法**:
```bash
python -m bq_mcp_server.adapters.mcp_server --cache-ttl-seconds 3600
```

### --cache-file-base-dir

**説明**: キャッシュファイルのベースディレクトリ

**環境変数**: `CACHE_FILE_BASE_DIR`
  - デフォルト値: `.bq_metadata_cache`

**使用方法**:
```bash
python -m bq_mcp_server.adapters.mcp_server --cache-file-base-dir "value"
```

### --query-execution-project-id

**説明**: クエリ実行時に使用すべきプロジェクトID（デフォルトではproject-idsで最初に指定されたプロジェクトを使用）

**環境変数**: `QUERY_EXECUTION_PROJECT_ID`

**使用方法**:
```bash
python -m bq_mcp_server.adapters.mcp_server --query-execution-project-id "value"
```

## 環境変数リファレンス

BQ MCPサーバーでは以下の環境変数が使用されます:

| 変数名 | 説明 | 型 | デフォルト値 |
| --- | --- | --- | --- |
| `API_HOST` | APIサーバーのホスト名 | str | `127.0.0.1` |
| `API_PORT` | APIサーバーのポート番号 | int | `8000` |
| `CACHE_FILE_BASE_DIR` | キャッシュファイルのベースディレクトリ | str | `.bq_metadata_cache` |
| `CACHE_TTL_SECONDS` | キャッシュの有効期限（秒単位） | int | `3600秒` |
| `DATASET_FILTERS` | データセットフィルタのカンマ区切りリスト（例: 'project1.*,project2.dataset1'） | list[str] | `None` |
| `DEFAULT_QUERY_LIMIT` | デフォルトのクエリ結果制限件数 | int | `100` |
| `GCP_SERVICE_ACCOUNT_KEY_PATH` | GCPサービスアカウントのJSONキーファイルパス | str | `None` |
| `MAX_SCAN_BYTES` | クエリ実行時の最大スキャンバイト数 | int | `1GB（1,073,741,824バイト）` |
| `PROJECT_IDS` | GCPプロジェクトIDのカンマ区切りリスト（例: 'project1,project2'） | list[str] | `必須` |
| `QUERY_EXECUTION_PROJECT_ID` | クエリ実行時に使用すべきプロジェクトID（デフォルトではproject-idsで最初に指定されたプロジェクトを使用） | str | `None` |
| `QUERY_TIMEOUT_SECONDS` | クエリのタイムアウト時間（秒単位） | int | `300秒` |
