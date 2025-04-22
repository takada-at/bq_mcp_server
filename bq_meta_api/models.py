# models.py: Defines Pydantic models for data structures and API responses
from typing import List, Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field
import datetime
# 循環インポートを修正: MarkdownConverterのインポートを削除


class ColumnSchema(BaseModel):
    """BigQueryテーブルのカラムスキーマを表すモデル"""

    name: str = Field(..., description="カラム名")
    type: str = Field(..., description="データ型 (例: STRING, INTEGER, TIMESTAMP)")
    mode: str = Field(..., description="モード (NULLABLE, REQUIRED, REPEATED)")
    description: Optional[str] = Field(None, description="カラムの説明")
    fields: Optional[List["ColumnSchema"]] = Field(
        None, description="RECORD型の場合のネストされたフィールド"
    )  # 再帰的な定義


class TableSchema(BaseModel):
    """BigQueryテーブルのスキーマ全体を表すモデル"""

    columns: List[ColumnSchema] = Field(..., description="テーブルのカラムリスト")


class TableMetadata(BaseModel):
    """BigQueryテーブルのメタデータを表すモデル"""

    project_id: str = Field(..., description="プロジェクトID")
    dataset_id: str = Field(..., description="データセットID")
    table_id: str = Field(..., description="テーブルID")
    full_table_id: str = Field(
        ..., description="完全なテーブルID (project.dataset.table)"
    )
    schema_: Optional[TableSchema] = Field(
        None, description="テーブルスキーマ"
    )  # 'schema'はBaseModelで予約語のためエイリアスを使用
    description: Optional[str] = Field(None, description="テーブルの説明")
    num_rows: Optional[int] = Field(None, description="テーブルの行数")
    num_bytes: Optional[int] = Field(None, description="テーブルのサイズ (バイト)")
    created_time: Optional[datetime.datetime] = Field(None, description="作成日時")
    last_modified_time: Optional[datetime.datetime] = Field(
        None, description="最終更新日時"
    )
    # 他に必要なメタデータがあれば追加


class DatasetMetadata(BaseModel):
    """BigQueryデータセットのメタデータを表すモデル"""

    project_id: str = Field(..., description="プロジェクトID")
    dataset_id: str = Field(..., description="データセットID")
    description: Optional[str] = Field(None, description="データセットの説明")
    location: Optional[str] = Field(None, description="データセットのロケーション")
    # 他に必要なメタデータがあれば追加


# --- APIレスポンスモデル ---


class DatasetListResponse(BaseModel):
    """/datasets エンドポイントのレスポンスモデル"""

    datasets: List[DatasetMetadata] = Field(
        ..., description="データセットメタデータのリスト"
    )


class TableListResponse(BaseModel):
    """/<dataset>/tables エンドポイントのレスポンスモデル"""

    tables: List[TableMetadata] = Field(
        ..., description="テーブルメタデータのリスト"
    )  # スキーマは含まない簡易版


class SearchResultItem(BaseModel):
    """検索結果のアイテム"""

    type: str = Field(..., description="アイテムの種類 ('dataset', 'table', 'column')")
    project_id: str
    dataset_id: str
    table_id: Optional[str] = None  # table or columnの場合
    column_name: Optional[str] = None  # columnの場合
    match_location: str = Field(
        ..., description="キーワードがマッチした場所 ('name', 'description')"
    )
    # 必要に応じて他の情報（説明など）も追加


class SearchResponse(BaseModel):
    """/search エンドポイントのレスポンスモデル"""

    query: str = Field(..., description="実行された検索キーワード")
    results: List[SearchResultItem] = Field(..., description="検索結果リスト")


# --- キャッシュ用データ構造 ---
class CachedData(BaseModel):
    """キャッシュに保存するデータ全体のモデル"""

    datasets: Dict[str, List[DatasetMetadata]] = Field(
        default_factory=dict, description="プロジェクトIDごとのデータセットリスト"
    )  # key: project_id
    tables: Dict[str, Dict[str, List[TableMetadata]]] = Field(
        default_factory=dict,
        description="プロジェクト・データセットごとのテーブルリスト（スキーマ含む）",
    )  # key1: project_id, key2: dataset_id
    last_updated: Optional[datetime.datetime] = Field(
        None, description="キャッシュの最終更新日時"
    )
