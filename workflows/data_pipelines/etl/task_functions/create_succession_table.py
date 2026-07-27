import logging

from airflow.sdk import get_current_context, task

from data_pipelines_annuaire.workflows.data_pipelines.etl.data_fetch_clean.succession import (
    preprocess_succession_df,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.helpers import (
    create_index,
    create_table_model,
    get_table_count,
)
from data_pipelines_annuaire.workflows.data_pipelines.etl.sqlite.queries.succession import (
    create_table_liens_succession_query,
)


@task
def create_succession_table():
    table_name = "liens_succession"
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=create_table_liens_succession_query,
    )

    for df_liens in preprocess_succession_df():
        df_liens.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )

        for row in sqlite_client.execute(get_table_count(table_name)):
            logging.debug(
                f"************ {row} total records have been added "
                f"to the {table_name} table!"
            )

    sqlite_client.execute(
        create_index(
            "index_succession_siret_successeur", table_name, "siret_successeur"
        )
    )
    sqlite_client.execute(
        create_index(
            "index_succession_siret_predecesseur", table_name, "siret_predecesseur"
        )
    )

    ti = get_current_context()["ti"]
    for count_succession in sqlite_client.execute(get_table_count(table_name)):
        ti.xcom_push(key="count_liens_succession", value=count_succession)

    sqlite_client.commit_and_close_conn()
