import logging

from dag_datalake_sirene.data_preprocessing.unite_legale import (
    preprocess_unite_legale_data,
)

from dag_datalake_sirene.sqlite.queries.helpers import (
    get_table_count,
    create_unique_index,
)
from dag_datalake_sirene.sqlite.queries.create_table_flux_unite_legale import (
    create_table_flux_unite_legale_query,
)
from dag_datalake_sirene.sqlite.queries.create_table_unite_legale import (
    create_table_unite_legale_query,
)


from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    create_table_model,
)

from dag_datalake_sirene.task_functions.global_variables import DATA_DIR


def create_table(query, table_name, index, sirene_file_type):
    sqlite_client = create_table_model(
        table_name=table_name,
        create_table_query=query,
        create_index_func=create_unique_index,
        index_name=index,
        index_column="siren",
    )
    for df_unite_legale in preprocess_unite_legale_data(DATA_DIR, sirene_file_type):
        df_unite_legale.to_sql(
            table_name, sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count(table_name)):
            logging.debug(
                f"************ {row} total records have been added "
                f"to the {table_name} table!"
            )

    del df_unite_legale

    for count_unites_legales in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {count_unites_legales} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()
    return count_unites_legales[0]


def create_unite_legale_table(**kwargs):
    counts = create_table(
        create_table_unite_legale_query,
        "unite_legale",
        "index_siren",
        "stock",
    )
    kwargs["ti"].xcom_push(key="count_unites_legales", value=counts)


def create_flux_unite_legale_table(**kwargs):
    counts = create_table(
        create_table_flux_unite_legale_query,
        "flux_unite_legale",
        "index_flux_siren",
        "flux",
    )
    kwargs["ti"].xcom_push(key="count_flux_unites_legales", value=counts)
