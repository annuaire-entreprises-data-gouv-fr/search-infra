from dag_datalake_sirene.workflows.data_pipelines.etl.data_fetch_clean import (
    agence_bio as bio,
    collectivite_territoriale as ct,
    bilan_financier as bf,
    convention_collective as cc,
    egapro,
    entrepreneur_spectacle as es,
    ess_france as ess,
    finess,
    organisme_formation as of,
    rge,
    uai,
    marche_inclusion as mi,
)

from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.helpers import (
    create_unique_index,
    create_index,
    create_and_fill_table_model,
    create_only_index,
    execute_query,
)

from dag_datalake_sirene.workflows.data_pipelines.etl.sqlite.queries import (
    agence_bio as q_bio,
    bilan_financier as q_bf,
    convention_collective as q_cc,
    egapro as q_egapro,
    rge as q_rge,
    ess_france as q_ess,
    finess as q_finess,
    organisme_formation as q_of,
    entrepreneur_spectacle as q_es,
    colter as q_ct,
    uai as q_uai,
    marche_inclusion as q_mi,
)


def create_bilan_financier_table():
    create_and_fill_table_model(
        table_name="bilan_financier",
        create_table_query=q_bf.create_table_bilan_financier_query,
        create_index_func=create_unique_index,
        index_name="index_bilan_financier",
        index_column="siren",
        preprocess_table_data=bf.preprocess_bilan_financier_data,
    )


def create_convention_collective_table():
    create_and_fill_table_model(
        table_name="convention_collective",
        create_table_query=q_cc.create_table_convention_collective_query,
        create_index_func=create_unique_index,
        index_name="index_convention_collective",
        index_column="siret",
        preprocess_table_data=cc.preprocess_convcollective_data,
    )
    create_only_index(
        table_name="convention_collective",
        create_index_func=create_index,
        index_name="index_siren_convention_collective",
        index_column="siren",
    )


def create_rge_table():
    create_and_fill_table_model(
        table_name="rge",
        create_table_query=q_rge.create_table_rge_query,
        create_index_func=create_index,
        index_name="index_rge",
        index_column="siret",
        preprocess_table_data=rge.preprocess_rge_data,
    )


def create_uai_table():
    create_and_fill_table_model(
        table_name="uai",
        create_table_query=q_uai.create_table_uai_query,
        create_index_func=create_index,
        index_name="index_uai",
        index_column="siret",
        preprocess_table_data=uai.preprocess_uai_data,
    )


def create_finess_table():
    create_and_fill_table_model(
        table_name="finess",
        create_table_query=q_finess.create_table_finess_query,
        create_index_func=create_index,
        index_name="index_finess",
        index_column="siret",
        preprocess_table_data=finess.preprocess_finess_data,
    )


def create_agence_bio_table():
    create_and_fill_table_model(
        table_name="agence_bio",
        create_table_query=q_bio.create_table_agence_bio_query,
        create_index_func=create_index,
        index_name="index_agence_bio",
        index_column="siret",
        preprocess_table_data=bio.preprocess_agence_bio_data,
    )
    create_only_index(
        table_name="agence_bio",
        create_index_func=create_index,
        index_name="index_siren_agence_bio",
        index_column="siren",
    )


def create_organisme_formation_table():
    create_and_fill_table_model(
        table_name="organisme_formation",
        create_table_query=q_of.create_table_organisme_formation_query,
        create_index_func=create_index,
        index_name="index_organisme_formation",
        index_column="siren",
        preprocess_table_data=of.preprocess_organisme_formation_data,
    )


def create_spectacle_table():
    create_and_fill_table_model(
        table_name="spectacle",
        create_table_query=q_es.create_table_spectacle_query,
        create_index_func=create_index,
        index_name="index_spectacle",
        index_column="siren",
        preprocess_table_data=es.preprocess_spectacle_data,
    )


def create_egapro_table():
    create_and_fill_table_model(
        table_name="egapro",
        create_table_query=q_egapro.create_table_egapro_query,
        create_index_func=create_index,
        index_name="index_egapro",
        index_column="siren",
        preprocess_table_data=egapro.preprocess_egapro_data,
    )


def create_colter_table():
    create_and_fill_table_model(
        table_name="colter",
        create_table_query=q_ct.create_table_colter_query,
        create_index_func=create_index,
        index_name="index_colter",
        index_column="siren",
        preprocess_table_data=ct.preprocess_colter_data,
    )


def create_elu_table():
    create_and_fill_table_model(
        table_name="elus",
        create_table_query=q_ct.create_table_elus_query,
        create_index_func=create_index,
        index_name="index_elus",
        index_column="siren",
        preprocess_table_data=ct.preprocess_elus_data,
    )
    execute_query(q_ct.delete_duplicates_elus_query)


def create_ess_table():
    create_and_fill_table_model(
        table_name="ess_france",
        create_table_query=q_ess.create_table_ess_query,
        create_index_func=create_index,
        index_name="index_ess",
        index_column="siren",
        preprocess_table_data=ess.preprocess_ess_france_data,
    )


def create_marche_inclusion_table():
    create_and_fill_table_model(
        table_name="marche_inclusion",
        create_table_query=q_mi.create_table_marche_inclusion_query,
        create_index_func=create_index,
        index_name="index_marche_inclusion",
        index_column="siren",
        preprocess_table_data=mi.preprocess_marche_inclusion_data,
    )
