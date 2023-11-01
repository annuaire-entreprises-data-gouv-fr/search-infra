from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    create_and_fill_table_model,
)
from dag_datalake_sirene.data_preprocessing.rna import (
    preprocess_rna,
)
from dag_datalake_sirene.sqlite.queries.create_table_rna import (
    create_table_rna_query,
)

from dag_datalake_sirene.sqlite.queries.helpers import (
    create_unique_index,
)


def create_rna_table():
    return create_and_fill_table_model(
        table_name="rna",
        create_table_query=create_table_rna_query,
        create_index_func=create_unique_index,
        index_name="index_rna",
        index_column="identifiant_association",
        preprocess_table_data=preprocess_rna,
    )
