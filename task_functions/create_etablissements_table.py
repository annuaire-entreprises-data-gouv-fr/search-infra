import logging

from dag_datalake_sirene.data_preprocessing.etablissements import (
    preprocess_etablissements_data,
)
from dag_datalake_sirene.labels.departements import all_deps

from dag_datalake_sirene.sqlite.queries.helpers import (
    get_table_count,
    create_index,
)
from dag_datalake_sirene.sqlite.queries.create_table_etabs import (
    create_table_etablissements_query,
)


from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    create_table_model,
)


def create_etablissements_table():
    sqlite_client = create_table_model(
        table_name="siret",
        create_table_query=create_table_etablissements_query,
        create_index_func=create_index,
        index_name="index_siret",
        index_column="siren",
    )
    # Upload geo data by departement
    for dep in all_deps:
        df_dep = preprocess_etablissements_data(dep)
        df_dep.to_sql("siret", sqlite_client.db_conn, if_exists="append", index=False)
        for row in sqlite_client.execute(get_table_count("siret")):
            logging.info(
                f"************ {row} total records have been added to the "
                f"`établissements` table!"
            )
    del df_dep
    sqlite_client.commit_and_close_conn()
