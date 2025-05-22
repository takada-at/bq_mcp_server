# search_engine.py: Provides search functionality over cached metadata
from typing import List, Optional
from bq_meta_api.repositories import log
from bq_meta_api.core.entities import (
    CachedData,
    SearchResultItem,
    ColumnSchema,
)
from bq_meta_api.repositories import cache_manager


def _search_columns(
    columns: List[ColumnSchema],
    keyword: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> List[SearchResultItem]:
    """指定されたカラムリスト内を再帰的に検索します。"""
    results: List[SearchResultItem] = []
    lower_keyword = keyword.lower()

    for column in columns:
        # カラム名で検索
        if lower_keyword in column.name.lower():
            results.append(
                SearchResultItem(
                    type="column",
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=table_id,
                    column_name=column.name,
                    match_location="name",
                )
            )
        # カラムの説明で検索
        if column.description and lower_keyword in column.description.lower():
            # 同じカラム名がすでに追加されていないかチェック（重複を避ける）
            if not any(
                r.type == "column"
                and r.column_name == column.name
                and r.match_location == "description"
                for r in results
            ):
                results.append(
                    SearchResultItem(
                        type="column",
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table_id,
                        column_name=column.name,
                        match_location="description",
                    )
                )

        # ネストされたフィールドを再帰的に検索
        if column.fields:
            results.extend(
                _search_columns(
                    column.fields, keyword, project_id, dataset_id, table_id
                )
            )

    return results


async def search_metadata_inner(keyword: str) -> List[SearchResultItem]:
    """
    キャッシュされたメタデータ全体からキーワードに一致する項目を検索します。
    データセット名、テーブル名、カラム名、およびそれらの説明を検索対象とします。

    Args:
        keyword: 検索キーワード。

    Returns:
        検索結果。formatパラメータに応じて、SearchResultItemのリスト
    """
    logger = log.get_logger()
    logger.info(f"メタデータ検索を実行中: keyword='{keyword}', format='{format}'")
    results: List[SearchResultItem] = []
    cached_data: Optional[CachedData] = await cache_manager.get_cached_data()

    if not cached_data:
        logger.warning("検索対象のキャッシュデータがありません。")
        return results

    lower_keyword = keyword.lower()

    # 1. データセットを検索
    for project_id, datasets in cached_data.datasets.items():
        for dataset in datasets:
            # データセットIDで検索
            if lower_keyword in dataset.dataset_id.lower():
                results.append(
                    SearchResultItem(
                        type="dataset",
                        project_id=project_id,
                        dataset_id=dataset.dataset_id,
                        match_location="name",
                    )
                )
            # データセットの説明で検索
            if dataset.description and lower_keyword in dataset.description.lower():
                # 同じデータセットがすでに追加されていないかチェック
                if not any(
                    r.type == "dataset"
                    and r.dataset_id == dataset.dataset_id
                    and r.match_location == "description"
                    for r in results
                ):
                    results.append(
                        SearchResultItem(
                            type="dataset",
                            project_id=project_id,
                            dataset_id=dataset.dataset_id,
                            match_location="description",
                        )
                    )

    # 2. テーブルとカラムを検索
    for project_id, datasets_tables in cached_data.tables.items():
        for dataset_id, tables in datasets_tables.items():
            for table in tables:
                # テーブルIDで検索
                if lower_keyword in table.table_id.lower():
                    results.append(
                        SearchResultItem(
                            type="table",
                            project_id=project_id,
                            dataset_id=dataset_id,
                            table_id=table.table_id,
                            match_location="name",
                        )
                    )
                # テーブルの説明で検索
                if table.description and lower_keyword in table.description.lower():
                    # 同じテーブルがすでに追加されていないかチェック
                    if not any(
                        r.type == "table"
                        and r.table_id == table.table_id
                        and r.match_location == "description"
                        for r in results
                    ):
                        results.append(
                            SearchResultItem(
                                type="table",
                                project_id=project_id,
                                dataset_id=dataset_id,
                                table_id=table.table_id,
                                match_location="description",
                            )
                        )

                # カラムを検索 (スキーマが存在する場合)
                if table.schema_:
                    column_results = _search_columns(
                        table.schema_.columns,
                        keyword,
                        project_id,
                        dataset_id,
                        table.table_id,
                    )
                    # 重複を避けるため、既存の結果に含まれていないものだけ追加
                    for col_res in column_results:
                        is_duplicate = any(
                            r.type == "column"
                            and r.project_id == col_res.project_id
                            and r.dataset_id == col_res.dataset_id
                            and r.table_id == col_res.table_id
                            and r.column_name == col_res.column_name
                            and r.match_location == col_res.match_location
                            for r in results
                        )
                        if not is_duplicate:
                            results.append(col_res)

    logger.info(f"検索完了。 {len(results)} 件のヒットがありました。")
    return results


def multi_split(text: str, delimiters: List[str]) -> List[str]:
    """
    複数の区切り文字で文字列を分割する関数

    Args:
        text (str): 分割する文字列
        delimiters (List[str]): 区切り文字の集合

    Returns:
        list: 分割された文字列のリスト
    """
    # 初期結果は元の文字列
    result = [text]

    # 各区切り文字について処理
    for delimiter in delimiters:
        # 一時的な結果リスト
        temp_result = []
        # 現在の結果リストの各要素について
        for item in result:
            # 区切り文字で分割して一時リストに追加
            temp_result.extend(item.split(delimiter))
        # 結果を更新
        result = temp_result

    # 空の文字列を除去
    return [item.strip() for item in result if item]


async def search_metadata(keyword: str) -> List[SearchResultItem]:
    keywords = multi_split(keyword, [" ", ",", "."])
    results = []
    for k in keywords:
        if not k:
            continue
        k = k.replace('"', "").replace("`", "")
        results += await search_metadata_inner(k)
    return results
