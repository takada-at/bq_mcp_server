# クリーンアーキテクチャのレイヤー

## クリーンアーキテクチャの概要

クリーンアーキテクチャは、設計要素をリング状のレベルに分離するソフトウェア設計哲学です。クリーンアーキテクチャの主なルールは、コードの依存関係が外側から内側にしか移動できないことです。内側のレベルのコードは、外側のレベルの関数について知っていてはいけません。

## 4つの主要なレイヤー

クリーンアーキテクチャは、4つの主要なレイヤーで構成されます：

1.  **エンティティ（Entities）**: アプリケーションのコアビジネスオブジェクトです。最も一般的で高レベルなルールをカプセル化します。外部の変更によって変更される可能性が最も低い部分です。
2.  **ユースケース（インタラクタ）（Use Cases / Interactors）**: アプリケーション固有のビジネスルールを含みます。エンティティへのデータの流れを調整し、エンティティにエンタープライズ全体のビジネスルールを使ってユースケースの目標を達成させます。
3.  **インターフェースアダプタ（Interface Adapters）**: データをユースケースやエンティティにとって最も便利な形式から、データベースやWebなどの外部エージェンシーにとって最も便利な形式に変換するアダプタのセットです。GUIのMVCアーキテクチャもこのレイヤーに含まれます。
4.  **フレームワーク＆ドライバ（Frameworks & Drivers）**: データベースやWebフレームワークなどのフレームワークやツールで構成されるレイヤーです。詳細はすべてこのレイヤーに置きます。Webもデータベースも詳細です。これらは外側に置き、内側に悪影響を及ぼさないようにします。

## 依存性ルール

同心円はソフトウェアの異なる領域を表します。一般的に、内側に行くほどソフトウェアのレベルは高くなります。外側の円はメカニズム、内側の円はポリシーです。

このアーキテクチャを機能させる最重要ルールは**依存性ルール**です。このルールは「ソースコードの依存関係は内側にしか向かない」と定めています。内側の円のものは、外側の円のものについて何も知っていてはいけません。特に、外側の円で宣言された名前（関数、クラス、変数など）は内側の円で参照してはいけません。

同様に、外側の円で使われるデータフォーマットは内側の円で使うべきではありません。特にフレームワークが生成したデータフォーマットは避けるべきです。外側の円のものが内側の円に影響を与えないようにします。

## アプリケーションコンポーネントのクリーンアーキテクチャレイヤーへのマッピング

このセクションでは、既存のアプリケーションコンポーネントをクリーンアーキテクチャのレイヤーにマッピングします。

### エンティティ

エンティティはアプリケーションのコアビジネスオブジェクトです。`bq_meta_api`アプリケーションでは、エンティティは`bq_meta_api/models.py`のPydanticモデルで定義されています。これらのモデル（例：`Table`, `Dataset`, `Routine`, `TableReference`, `Column`）は、BigQueryから取得したメタデータオブジェクトの構造とバリデーションルールをカプセル化します。特定のフレームワークやデータベース技術には依存しません。

### ユースケース（インタラクタ）

ユースケースはアプリケーション固有のビジネスルールを含み、データの流れを調整します。ユースケースとなるコアアプリケーションロジックには以下が含まれます：

*   **`FetchBigQueryTableMetadata`**: 特定のBigQueryテーブルの詳細なメタデータを取得します。
*   **`FetchBigQueryRoutineMetadata`**: 特定のBigQueryルーチンの詳細なメタデータを取得します。
*   **`ListDatasets`**: プロジェクト内のすべてのデータセットを一覧表示します。
*   **`ListTablesInDataset`**: 特定のデータセット内のすべてのテーブルを一覧表示します。
*   **`ListRoutinesInDataset`**: 特定のデータセット内のすべてのルーチンを一覧表示します。
*   **`SearchMetadata`**: クエリに基づいてデータセット、テーブル、ルーチンのメタデータを横断検索します。

これらのユースケースに寄与する現在のモジュールは：

*   `logic.py`: メタデータ取得・処理の主要なオーケストレーションロジックを含みます。今後は各ユースケースクラスにリファクタリングされます。
*   `bigquery_client.py`: BigQueryからの生データを提供し、ユースケースで処理されます。
*   `cache_manager.py`: キャッシュ処理を担当し、ユースケースがパフォーマンス向上のために利用します。
*   `search_engine.py`: 検索機能を提供し、`SearchMetadata`ユースケースで利用されます。
*   `main.py`: 一部のFastAPIエンドポイントハンドラにユースケースに移すべきロジックが含まれています。

### インターフェースアダプタ

インターフェースアダプタは、ユースケースやエンティティに適した形式と外部エージェンシー（Webフレームワークやデータベースなど）に適した形式の間でデータを変換します。

*   **コントローラ／プレゼンター**:
    *   **コントローラ**: `main.py`のFastAPIパスオペレーション関数がコントローラとして機能します。HTTPリクエストを受け取り、ユースケースに委譲します。例：
        *   `GET /projects/{project_id}/datasets`
        *   `GET /projects/{project_id}/datasets/{dataset_id}/tables`
        *   `GET /projects/{project_id}/datasets/{dataset_id}/tables/{table_id}`
        *   `GET /projects/{project_id}/datasets/{dataset_id}/routines`
        *   `GET /projects/{project_id}/datasets/{dataset_id}/routines/{routine_id}`
        *   `GET /search`
    *   **プレゼンター**: `converter.py`や`logic.py`の一部にあるデータ変換ロジックは、プレゼンターとして形式化されます。ユースケースの出力（エンティティや単純なデータ構造）をHTTPレスポンス（APIレスポンス用のPydanticモデル）に整形します。

*   **ゲートウェイ（リポジトリ）**:
    ゲートウェイはデータ取得や保存方法を定義するインターフェースで、基盤となるデータソースを抽象化します。
    *   **`BigQueryMetadataRepository`（インターフェース）**: BigQueryからメタデータを取得するメソッドを定義します（例：`get_table(table_id)`, `list_tables(dataset_id)`）。
        *   *実装*: `bigquery_client.py`がこのインターフェースを実装するようリファクタリングされます。
    *   **`CacheRepository`（インターフェース）**: キャッシュ操作のメソッドを定義します（例：`get(key)`, `set(key, value)`）。
        *   *実装*: `cache_manager.py`がこのインターフェースを実装するようリファクタリングされます。
    *   **`SearchRepository`（インターフェース）**: 検索インデックスとのやり取りのメソッドを定義します（例：`search_documents(query)`, `add_document(document)`）。
        *   *実装*: `search_engine.py`がこのインターフェースを実装するようリファクタリングされます。

### フレームワーク＆ドライバ（外部インターフェース＆DB）

このレイヤーには、外部フレームワーク、ツール、外部インターフェースの具体的な実装が含まれます。

*   **FastAPI（`main.py`）**: APIエンドポイントを公開するWebフレームワーク。
*   **Google BigQueryクライアントライブラリ（`bigquery_client.py`）**: Google BigQueryとやり取りするためのドライバ。
*   **キャッシュ機構（`cache_manager.py`）**: キャッシュの具体的な実装（例：インメモリ、Redis）。
*   **設定管理（`config.py`）**: アプリケーション設定を管理。
*   **検索エンジンライブラリ（`search_engine.py`）**: 検索エンジンの具体的な実装（例：Whoosh）。
*   **Pydantic（`models.py`, `main.py`）**: データバリデーションとシリアライズに使用され、外部とのデータ交換フォーマット（HTTPリクエスト／レスポンス）として機能します。

これらのコンポーネントは詳細であり、内側のレイヤー（エンティティ、ユースケース）はこれらに依存しません。

## データフローの例：「メタデータ検索」ユースケース

このセクションでは、「メタデータ検索」ユースケースのデータフローを示し、各レイヤーの連携を説明します。

1.  **フレームワーク＆ドライバ（FastAPI & ユーザー）**:
    *   ユーザーが`/search`エンドポイント（例：`/search?query=customer&project_id=my_project`）にHTTP GETリクエストを送信します。
    *   FastAPI（フレームワーク）がリクエストを受信し、クエリパラメータ（`query`, `project_id`, `limit`, `offset`）をエンドポイント用のPydanticモデル（例：`SearchRequestModel`）でバリデーションします。

2.  **インターフェースアダプタ（コントローラ）**:
    *   FastAPIがバリデーション済みリクエストとデータ（例：`SearchRequestModel`インスタンス）を対応するコントローラ関数（リファクタ後は`main.py`の`search_metadata_controller`など）にルーティングします。
    *   コントローラが`SearchRequestModel`から必要なデータ（検索クエリ文字列、プロジェクトIDなど）を抽出します。

3.  **ユースケース（インタラクタ - `SearchMetadataUseCase`）**:
    *   コントローラが`SearchMetadataUseCase`の`execute`メソッドを呼び出し、検索クエリやプロジェクトIDなどの単純なデータ型を渡します。
    *   `SearchMetadataUseCase`は`SearchRepository`と`CacheRepository`のインスタンスを（コンストラクタ経由で）必要とします。

4.  **インターフェースアダプタ（リポジトリ - `SearchRepository`, `CacheRepository`）**:
    *   **キャッシュチェック（任意だが推奨）**: `SearchMetadataUseCase`はまず`CacheRepository`の`get(cache_key)`メソッドを呼び出します。`cache_key`は検索クエリとプロジェクトIDから生成されます。
        *   キャッシュにデータがあれば、`CacheRepository`（`cache_manager.py`で実装）がキャッシュデータ（`SearchResultItem`エンティティのリストや辞書）を返します。ユースケースは6または7にスキップする場合があります。
    *   **検索実行**: キャッシュがなければ、`SearchMetadataUseCase`は`SearchRepository`の`search_documents(query, project_id, ...)`メソッドを呼び出します。
        *   `SearchRepository`インターフェースは`search_engine.py`で実装され、Whooshなどの検索エンジンライブラリを使ってインデックスを検索します。
        *   実装は生の検索結果（辞書や検索ライブラリ固有のオブジェクト）を返す場合があります。

5.  **エンティティ**:
    *   `SearchRepository`の実装は、検索エンジンライブラリからの生の検索結果を`SearchResultItem`エンティティ（または同等のドメイン固有表現）にマッピングします。これらのエンティティは`models.py`で定義されています（例：`SearchResultItem(name, type, project_id, dataset_id, description)`）。これにより、ユースケースレイヤーはインフラ固有のデータ構造ではなくドメインオブジェクトを扱えます。

6.  **ユースケース（インタラクタ - `SearchMetadataUseCase` 続き）**:
    *   `SearchMetadataUseCase`は`SearchRepository`から`SearchResultItem`エンティティのリストを受け取ります。
    *   追加のビジネスロジック（フィルタリング、結果の結合、アクセス制御など）を実行します（アクセス制御は横断的関心事として分離される場合もあります）。
    *   検索結果がキャッシュからでなければ、`CacheRepository`の`set(cache_key, search_results)`で結果をキャッシュします。
    *   ユースケースは検索結果（通常は`SearchResultItem`エンティティのリスト）をコントローラに返します。これはDTOやエンティティのリストであることが多いです。

7.  **インターフェースアダプタ（プレゼンター）**:
    *   コントローラはユースケースから`SearchResultItem`エンティティ（またはDTO）のリストを受け取ります。
    *   コントローラはこのデータをプレゼンター（例：`SearchPresenter`）に渡します。
    *   プレゼンターは`SearchResultItem`エンティティを`SearchResponseModel`（APIレスポンス用のPydanticモデル、`main.py`や`response_models.py`などで定義）に整形します。必要に応じてフィールド選択やデータ構造の再構成、ページネーション情報の付加などを行います。

8.  **フレームワーク＆ドライバ（FastAPI）**:
    *   プレゼンターが`SearchResponseModel`インスタンスをコントローラに返します。
    *   コントローラがこの`SearchResponseModel`インスタンスを返します。
    *   FastAPIがPydanticの`SearchResponseModel`をJSON HTTPレスポンスにシリアライズし、ユーザーに返します。

**やり取りされるデータオブジェクト：**

*   **HTTPリクエスト → コントローラ**: `SearchRequestModel`（FastAPIエンドポイント用のPydanticモデル）
*   **コントローラ → ユースケース**: 単純なデータ型（例：`query: str`, `project_id: str`）
*   **ユースケース ↔ リポジトリ**:
    *   `SearchRepository`へ: 検索パラメータ（例：`query: str`, `project_id: str`）
    *   `SearchRepository`から: `SearchResultItem`エンティティのリスト（`models.py`で定義）
    *   `CacheRepository`へ／から: キャッシュキー（`str`）、キャッシュデータ（`SearchResultItem`エンティティのリストや辞書）
*   **ユースケース → プレゼンター／コントローラ**: `SearchResultItem`エンティティのリストやそれを含むDTO
*   **プレゼンター → コントローラ／FastAPI**: `SearchResponseModel`（APIレスポンス用のPydanticモデル）
*   **FastAPI → HTTPレスポンス**: `SearchResponseModel`のJSON表現

このフローにより、依存関係は内向きになります。コントローラはユースケースに依存し、ユースケースはリポジトリインターフェース（実装ではなく）とエンティティに依存します。フレームワーク＆ドライバは最も外側にあり、エンティティはコアで、外側レイヤーに依存しません。

## リファクタリング手順（ハイレベル）

`bq_meta_api`アプリケーションをクリーンアーキテクチャに沿わせるため、以下のハイレベルなリファクタリング手順を提案します。主な目的は**レイヤー間の明確な境界を確立し、依存性ルールを守ること**です。

1.  **コアドメインの定義（エンティティ＆ユースケースインターフェース）**:
    *   **エンティティ**: `models.py`を見直します。Pydanticモデルがエンティティとして十分ならそのまま使い、Webフレームワークやインフラ依存がある場合はDTOとして外側レイヤーに移し、純粋なPythonオブジェクトをコアエンティティとします。本アプリケーションでは、Pydanticモデルがデータとバリデーションのみを持つならエンティティとして許容されます。
    *   **ユースケースインターフェース（抽象ポート）**: 各ユースケースの抽象基底クラスやインターフェース（例：`ISearchMetadataUseCase`, `IFetchTableDetailsUseCase`）を定義します。これらはドメインレイヤーに配置します。

2.  **アプリケーションロジックの実装（ユースケース実装）**:
    *   各ユースケースインターフェースの具体的な実装（例：`SearchMetadataUseCaseImpl`）を作成します。これらの実装は、現在`logic.py`や`main.py`の一部にあるコアアプリケーションロジックを含みます。
    *   ユースケースはデータアクセスのためにリポジトリインターフェースに依存します。

3.  **リポジトリインターフェースの導入（データアクセスの抽象ポート）**:
    *   データアクセスや外部サービスを抽象化するリポジトリインターフェースを定義します。
        *   `IBigQueryRepository`: BigQueryメタデータアクセス用
        *   `ICacheRepository`: キャッシュ機構用
        *   `ISearchRepository`: 検索エンジン用
    *   これらのインターフェースはユースケースレイヤーまたはドメイン内の"ports"サブモジュールに配置します。

4.  **インフラストラクチャレイヤーのリファクタ（リポジトリ実装＆外部サービス）**:
    *   既存の`bigquery_client.py`, `cache_manager.py`, `search_engine.py`を`infrastructure`レイヤーに移動します。
    *   これらのモジュールを対応するリポジトリインターフェースを実装するようリファクタします。例：`bigquery_client.py`の`BigQueryClient`が`IBigQueryRepository`を実装。
    *   このレイヤーにはフレームワーク固有の設定やドライバも含まれます。

5.  **インターフェースアダプタの分離（コントローラ、プレゼンター、ゲートウェイ実装）**:
    *   **コントローラ**: `main.py`のFastAPIパスオペレーション関数をスリム化し、主に以下を担当させます：
        *   リクエストのパースとバリデーション（FastAPIがPydanticで自動処理）
        *   適切なユースケースへの単純な入力データの受け渡し
        *   ユースケース出力をプレゼンターに渡す
    *   **プレゼンター**: レスポンス用データ変換を形式化。ユースケースからのデータ（エンティティやDTO）をHTTPレスポンスモデル（APIレスポンス用Pydanticモデル）にマッピング。`converter.py`のロジックを統合。
    *   **ゲートウェイ実装**: リファクタ後の`bigquery_client.py`, `cache_manager.py`などはゲートウェイ実装ですが、ユースケースはインターフェース定義に依存します。

6.  **ディレクトリ構成の整理**:
    クリーンアーキテクチャを反映した新しいディレクトリ構成例：

    ```
    bq_meta_api/
    ├── domain/                   # コアビジネスロジック（フレームワーク非依存）
    │   ├── entities.py           # コアエンティティ定義（models.pyでも可）
    │   ├── use_cases/            # アプリケーション固有ビジネスルール
    │   │   ├── __init__.py
    │   │   ├── interfaces.py     # ユースケースの抽象インターフェース
    │   │   └── search_metadata_use_case.py # 例：ユースケース
    │   └── repositories/         # データアクセスの抽象インターフェース（ポート）
    │       ├── __init__.py
    │       ├── bigquery_repository.py
    │       └── cache_repository.py
    │       └── search_repository.py
    │
    ├── application/              # ユースケース実装（オーケストレーション）
    │   ├── __init__.py
    │   ├── services/             # ユースケース実装
    │   │   └── search_service.py # 例：SearchMetadataUseCaseImpl
    │   └── dto.py                # ユースケースで使うDTO（必要なら）
    │
    ├── infrastructure/           # フレームワーク、ドライバ、外部ツール
    │   ├── __init__.py
    │   ├── bigquery/             # BigQueryクライアント実装
    │   │   └── client.py         # IBigQueryRepository実装
    │   ├── cache/                # キャッシュ実装
    │   │   └── redis_cache.py    # ICacheRepository実装
    │   ├── search/               # 検索エンジン実装
    │   │   └── whoosh_search.py  # ISearchRepository実装
    │   └── config.py             # アプリケーション設定
    │
    ├── interfaces/               # 外部とのアダプタ（API, CLI等）
    │   ├── api/                  # Web API関連
    │   │   ├── __init__.py
    │   │   ├── main.py           # FastAPIアプリセットアップ、DI
    │   │   ├── controllers/      # FastAPIパスオペレーション関数
    │   │   │   └── search_controller.py
    │   │   ├── presenters.py     # レスポンス用データ整形
    │   │   └── request_models.py # リクエスト用Pydanticモデル
    │   │   └── response_models.py # レスポンス用Pydanticモデル
    │   └── cli/                  # CLI（必要なら）
    │
    └── models.py                 # 既存Pydanticモデル（domain/entities.pyまたはinterfaces/api/に分割検討）
    ```
    *注：既存の`models.py`は分割または移動の可能性あり。Pydanticモデルが純粋なデータ構造とバリデーションのみなら`domain/entities.py`、フレームワーク依存やリクエスト／レスポンス整形用なら`interfaces/api/request_models.py`や`interfaces/api/response_models.py`に配置。*

7.  **依存性注入**:
    *   具体的な実装（`infrastructure`や`application/services`）をインターフェースに依存するコンポーネントに提供するため、依存性注入を実装します。FastAPIのDIシステムを活用できます。

このリファクタリングにより、クリーンアーキテクチャの原則に従った、よりモジュール化されテストしやすく保守しやすいアプリケーションになります。各ステップは段階的に進め、都度テストを行うことが推奨されます。

## クリーンアーキテクチャ適用のメリット

`bq_meta_api`アプリケーションにクリーンアーキテクチャを適用することで、以下のような大きな利点があります：

*   **テスト容易性**: コアビジネスロジック（ユースケース）やエンティティは、Webフレームワーク（FastAPI）やデータベース（BigQueryクライアント）など外部サービスなしで単体テストできます。これによりユニットテストが簡単・高速・信頼性向上。インターフェースで定義された依存のモックも容易です。

*   **保守性**: 関心の分離が明確になり、コードベースの理解・ナビゲーション・デバッグが容易になります。あるレイヤーの変更（例：FastAPIのバージョンアップ）が他レイヤーに影響しにくくなり、意図しない副作用のリスクが減ります。

*   **フレームワーク非依存性**: コアアプリケーション（エンティティとユースケース）は特定のフレームワーク（FastAPIなど）やデータストレージ（BigQuery）、UIに依存しません。これにより、将来的な外部コンポーネントのアップグレードや置き換えもコアビジネスロジックへの影響を最小限に抑えられます。

*   **スケーラビリティ**: モジュール設計により、アプリケーションの各部分を独立して開発・デプロイ・スケールできます。特定のユースケースがボトルネックになった場合も、他部分に影響せず最適化やスケールが可能です。

*   **再利用性**: ユースケースにカプセル化されたコアビジネスロジックは、異なるインターフェースやアプリケーション間で再利用可能です。例えば、同じ検索ユースケースをREST API、GraphQL API、CLIなどで最小限の変更で利用できます。

このアーキテクチャリファクタリングにより、`bq_meta_api`はより堅牢で将来の変更に適応しやすく、チームが管理・拡張しやすいシステムとなります。
