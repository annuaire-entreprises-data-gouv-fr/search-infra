from dag_datalake_sirene.sqlite.queries.replace_table_etablissement import (
    replace_table_etablissement_query,
)

from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    replace_table_model,
)


def replace_etablissements_table():
    sqlite_client = replace_table_model(
        replace_table_query=replace_table_etablissement_query,
    )
    sqlite_client.commit_and_close_conn()
