import logging
from dag_datalake_sirene.sqlite.queries.helpers import (
    get_table_count,
    create_index,
)
from dag_datalake_sirene.sqlite.queries.create_table_siret_siege import (
    create_table_siret_siege_query,
)
from dag_datalake_sirene.sqlite.queries.populate_table_siret_siege import (
    populate_table_siret_siege_query,
)
from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    create_table_model,
)


def create_siege_only_table(**kwargs):
    sqlite_client = create_table_model(
        table_name="siretsiege",
        create_table_query=create_table_siret_siege_query,
        create_index_func=create_index,
        index_name="index_siret_siren",
        index_column="siren",
    )
    sqlite_client.execute(populate_table_siret_siege_query)
    for count_sieges in sqlite_client.execute(get_table_count("siretsiege")):
        logging.info(
            f"************ {count_sieges} total records have been added to the "
            f"unité légale table!"
        )
    kwargs["ti"].xcom_push(key="count_sieges", value=count_sieges[0])
    sqlite_client.commit_and_close_conn()
