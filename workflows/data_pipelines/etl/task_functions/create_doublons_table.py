import logging

from airflow.sdk import get_current_context, task

from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.doublons import (
    download_doublons,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.helpers import (
    create_table_model,
    create_unique_index,
    get_table_count,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.queries.doublons import (
    create_table_doublons_query,
)

logger = logging.getLogger(__name__)


@task
def create_doublons_table():
    table_name = "doublons"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_doublons_query,
        create_index_func=create_unique_index,
        index_name="index_doublons_siren_doublon",
        index_column="siren_doublon",
    )

    df_doublons = download_doublons()
    df_doublons.to_sql(
        table_name, sqlite_client.db_conn, if_exists="append", index=False
    )

    ti = get_current_context()["ti"]
    for count_doublons in sqlite_client.execute(get_table_count(table_name)):
        logger.debug(
            f"************ {count_doublons} total records have been added "
            f"to the {table_name} table!"
        )
        ti.xcom_push(key="count_doublons", value=count_doublons)

    sqlite_client.commit_and_close_conn()
