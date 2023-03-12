from dag_datalake_sirene.data_preprocessing.agence_bio import preprocess_agence_bio_data
from dag_datalake_sirene.data_preprocessing.collectivite_territoriale import (
    preprocess_colter_data,
    preprocess_elus_data,
)
from dag_datalake_sirene.data_preprocessing.convention_collective import (
    preprocess_convcollective_data,
)
from dag_datalake_sirene.data_preprocessing.entrepreneur_spectacle import (
    preprocess_spectacle_data,
)
from dag_datalake_sirene.data_preprocessing.finess import preprocess_finess_data
from dag_datalake_sirene.data_preprocessing.organisme_formation import (
    preprocess_organisme_formation_data,
)
from dag_datalake_sirene.data_preprocessing.rge import preprocess_rge_data
from dag_datalake_sirene.data_preprocessing.uai import preprocess_uai_data

from dag_datalake_sirene.sqlite.queries.helpers import (
    create_unique_index,
    create_index,
)

from dag_datalake_sirene.sqlite.queries.create_table_agence_bio import (
    create_table_agence_bio_query,
)
from dag_datalake_sirene.sqlite.queries.create_table_convention_collective import (
    create_table_convention_collective_query,
)
from dag_datalake_sirene.sqlite.queries.create_table_rge import create_table_rge_query

from dag_datalake_sirene.sqlite.queries.create_table_finess import (
    create_table_finess_query,
)
from dag_datalake_sirene.sqlite.queries.create_table_organisme_formation import (
    create_table_organisme_formation_query,
)
from dag_datalake_sirene.sqlite.queries.create_table_spectacle import (
    create_table_spectacle_query,
)

from dag_datalake_sirene.sqlite.queries.create_table_elus import create_table_elus_query
from dag_datalake_sirene.sqlite.queries.create_table_colter import (
    create_table_colter_query,
)

from dag_datalake_sirene.sqlite.queries.create_table_uai import create_table_uai_query


from dag_datalake_sirene.task_functions.create_and_fill_table_model import (
    create_and_fill_table_model,
)


def create_convention_collective_table():
    return create_and_fill_table_model(
        table_name="convention_collective",
        create_table_query=create_table_convention_collective_query,
        create_index_func=create_unique_index,
        index_name="index_convention_collective",
        index_column="siret",
        preprocess_table_data=preprocess_convcollective_data,
    )


def create_rge_table():
    return create_and_fill_table_model(
        table_name="rge",
        create_table_query=create_table_rge_query,
        create_index_func=create_index,
        index_name="index_rge",
        index_column="siret",
        preprocess_table_data=preprocess_rge_data,
    )


def create_uai_table():
    return create_and_fill_table_model(
        table_name="uai",
        create_table_query=create_table_uai_query,
        create_index_func=create_index,
        index_name="index_uai",
        index_column="siret",
        preprocess_table_data=preprocess_uai_data,
    )


def create_finess_table():
    return create_and_fill_table_model(
        table_name="finess",
        create_table_query=create_table_finess_query,
        create_index_func=create_index,
        index_name="index_finess",
        index_column="siret",
        preprocess_table_data=preprocess_finess_data,
    )


def create_agence_bio_table():
    return create_and_fill_table_model(
        table_name="agence_bio",
        create_table_query=create_table_agence_bio_query,
        create_index_func=create_index,
        index_name="index_agence_bio",
        index_column="siret",
        preprocess_table_data=preprocess_agence_bio_data,
    )


def create_organisme_formation_table():
    return create_and_fill_table_model(
        table_name="organisme_formation",
        create_table_query=create_table_organisme_formation_query,
        create_index_func=create_index,
        index_name="index_organisme_formation",
        index_column="siret",
        preprocess_table_data=preprocess_organisme_formation_data,
    )


def create_spectacle_table():
    return create_and_fill_table_model(
        table_name="spectacle",
        create_table_query=create_table_spectacle_query,
        create_index_func=create_index,
        index_name="index_spectacle",
        index_column="siren",
        preprocess_table_data=preprocess_spectacle_data,
    )


def create_colter_table():
    return create_and_fill_table_model(
        table_name="colter",
        create_table_query=create_table_colter_query,
        create_index_func=create_index,
        index_name="index_colter",
        index_column="siren",
        preprocess_table_data=preprocess_colter_data,
    )


def create_elu_table():
    return create_and_fill_table_model(
        table_name="elus",
        create_table_query=create_table_elus_query,
        create_index_func=create_index,
        index_name="index_elus",
        index_column="siren",
        preprocess_table_data=preprocess_elus_data,
    )
