from dag_datalake_sirene.sqlite.queries.helpers import (
    create_unique_index,
)
from dag_datalake_sirene.sqlite.queries.create_table_count_etabs import (
    create_table_count_etablissements_query,
)


from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    create_table_model,
)

from dag_datalake_sirene.sqlite.queries.count_nombre_etabs import (
    count_nombre_etablissements_query,
)


def count_nombre_etablissements():
    sqlite_client = create_table_model(
        table_name="count_etab",
        create_table_query=create_table_count_etablissements_query,
        create_index_func=create_unique_index,
        index_name="index_count_siren",
        index_column="siren",
    )

    sqlite_client.execute(count_nombre_etablissements_query)
    sqlite_client.commit_and_close_conn()
