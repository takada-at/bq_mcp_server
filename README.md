# BigQuery Metadata API Server

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI-green.svg)](https://fastapi.tiangolo.com/)

Google Cloud BigQueryのデータセット、テーブル、スキーマ情報を取得し、ローカルにキャッシュしてAPI経由で提供するPythonサーバーです。生成AIなどがBigQueryの構造を迅速に把握することを目的としています。

## 機能

*   **メタデータ取得:** 指定された複数のGCPプロジェクトからメタデータを取得します。
*   **ローカルキャッシュ:** 取得したメタデータをJSONファイルにキャッシュし、高速なレスポンスを提供します。キャッシュは設定されたTTL（Time To Live）に基づいて自動更新されます（APIアクセス時）。手動更新用のエンドポイントも提供します。
*   **APIエンドポイント:**
    *   `/datasets`: 全プロジェクトのデータセット一覧を取得します。
    *   `/<dataset_id>/tables`: 指定されたデータセットのテーブル一覧（メタデータのみ）を取得します。
    *   `/search?key=<keyword>`: データセット名、テーブル名、カラム名、およびそれらの説明からキーワード検索を行います。
    *   `/cache/update` (POST): キャッシュを手動で強制更新します。

## 必要なもの

*   Python 3.8 以降
*   pip (Python パッケージインストーラー)
*   Google Cloud Platform (GCP) アカウントと、BigQuery APIが有効になっているプロジェクト。
*   GCP認証情報:
    *   サービスアカウントキー (JSONファイル) または
    *   Application Default Credentials (ADC) (例: `gcloud auth application-default login` で設定)

## セットアップ

1.  **リポジトリをクローン:**
    ```bash
    git clone <repository_url>
    cd bigquery_ai
    ```

2.  **仮想環境の作成と有効化 (推奨):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Linux/macOS
    # venv\Scripts\activate  # Windows
    ```

3.  **依存関係のインストール:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **設定ファイルの作成:**
    `.env.example` をコピーして `.env` ファイルを作成します。
    ```bash
    cp .env.example .env
    ```
    作成した `.env` ファイルを編集し、環境に合わせて設定値を入力します。

    ```dotenv
    # .env - BigQuery Metadata API Server Configuration

    # --- GCP Settings ---
    # Optional: Path to your GCP service account key JSON file.
    # If not set, the application will try to use Application Default Credentials (ADC).
    # Example: GCP_SERVICE_ACCOUNT_KEY_PATH=/path/to/your/keyfile.json
    GCP_SERVICE_ACCOUNT_KEY_PATH=

    # Required: Comma-separated list of GCP project IDs to scan.
    # Example: PROJECT_IDS=your-project-id-1,your-project-id-2
    PROJECT_IDS=your-gcp-project-id

    # --- Cache Settings ---
    # Optional: Time-to-live for the cache in seconds. Defaults to 3600 (1 hour).
    CACHE_TTL_SECONDS=3600

    # Optional: Path to the file where the cache will be stored. Defaults to 'bq_metadata_cache.json'.
    CACHE_FILE_PATH=bq_metadata_cache.json

    # --- API Server Settings (for uvicorn) ---
    # Optional: Host for the API server. Defaults to '127.0.0.1'.
    API_HOST=127.0.0.1

    # Optional: Port for the API server. Defaults to 8000.
    API_PORT=8000
    ```
    *   `GCP_SERVICE_ACCOUNT_KEY_PATH`: (オプション) サービスアカウントキーJSONファイルのパス。設定しない場合はADCが試行されます。
    *   `PROJECT_IDS`: (必須) メタデータを取得したいGCPプロジェクトIDをカンマ区切りで指定します。
    *   `CACHE_TTL_SECONDS`: (オプション) キャッシュの有効期間（秒）。デフォルトは3600秒（1時間）。
    *   `CACHE_FILE_PATH`: (オプション) キャッシュを保存するファイルパス。デフォルトは `bq_metadata_cache.json`。
    *   `API_HOST`, `API_PORT`: (オプション) APIサーバーがリッスンするホストとポート。

## 実行

以下のコマンドでAPIサーバーを起動します。

```bash
uvicorn bq_meta_api.main:app --host <your_host> --port <your_port> --reload
```

*   `<your_host>` と `<your_port>` は `.env` で設定した値（またはデフォルト値）に置き換えてください (例: `127.0.0.1`, `8000`)。
*   `--reload` オプションは開発中に便利ですが、本番環境では外してください。

サーバーが起動すると、指定したホストとポートでAPIにアクセスできるようになります。
例: `http://127.0.0.1:8000/docs` にアクセスすると、Swagger UIでAPIドキュメントを確認・試行できます。

## APIエンドポイント詳細

*   **GET /datasets**:
    *   キャッシュされている全てのプロジェクトのデータセットメタデータリストを返します。
*   **GET /{dataset_id}/tables**:
    *   指定された `dataset_id` に一致するテーブルのメタデータリスト（スキーマ情報は含まない）を返します。キャッシュ内の全プロジェクトから検索されます。
*   **GET /search?key={keyword}**:
    *   `keyword` を使用して、キャッシュ内のデータセット名、テーブル名、カラム名、およびそれらの説明を検索します。
*   **POST /cache/update**:
    *   BigQueryから最新のメタデータを取得し、ローカルキャッシュを強制的に更新します。

## 設計

詳細な設計については `design.md` を参照してください。