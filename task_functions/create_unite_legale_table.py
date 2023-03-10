import logging

from dag_datalake_sirene.data_preprocessing.unite_legale import (
    preprocess_unite_legale_data,
)

from dag_datalake_sirene.sqlite.queries.helpers import (
    get_table_count,
    create_unique_index,
)
from dag_datalake_sirene.sqlite.queries.create_table_unite_legale import (
    create_table_unite_legale_query,
)


from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    create_table_model,
)

from dag_datalake_sirene.task_functions.global_variables import DATA_DIR


def create_unite_legale_table(**kwargs):
    sqlite_client = create_table_model(
        table_name="unite_legale",
        create_table_query=create_table_unite_legale_query,
        create_index_func=create_unique_index,
        index_name="index_siren",
        index_column="siren",
    )
    for df_unite_legale in preprocess_unite_legale_data(DATA_DIR):
        df_unite_legale.to_sql(
            "unite_legale", sqlite_client.db_conn, if_exists="append", index=False
        )
        for row in sqlite_client.execute(get_table_count("unite_legale")):
            logging.info(
                f"************ {row} total records have been added "
                f"to the unité légale table!"
            )

    del df_unite_legale

    for count_unites_legales in sqlite_client.execute(get_table_count("unite_legale")):
        logging.info(
            f"************ {count_unites_legales} total records have been added to the "
            f"unité légale table!"
        )
    kwargs["ti"].xcom_push(key="count_unites_legales", value=count_unites_legales[0])
    sqlite_client.commit_and_close_conn()
