import pandas as pd

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.data_enrichment import (
    map_roles,
)

# fmt: on


def preprocess_personne_physique(query):
    cols = [column[0] for column in query.description]
    pp_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    pp_chunk.sort_values(
        by=[
            "siren",
            "nom",
            "nom_usage",
            "prenoms",
            "role",
            "date_mise_a_jour",
            "date_de_naissance",
            "nationalite",
        ],
        inplace=True,
        ascending=[True, True, True, True, True, True, False, False],
    )

    pp_chunk.drop_duplicates(
        subset=[
            "siren",
            "nom",
            "nom_usage",
            "prenoms",
            "date_de_naissance",
            "role",
            "date_mise_a_jour",
            "nationalite",
        ],
        keep="first",
        inplace=True,
    )

    # List of columns to convert to uppercase
    columns_to_uppercase = ["nom", "nom_usage", "prenoms"]
    for column in columns_to_uppercase:
        pp_chunk[column] = pp_chunk[column].str.upper()

    # Map role numbers to descriptions
    pp_chunk["role_description"] = map_roles(pp_chunk["role"])

    pp_clean = (
        pp_chunk.groupby(
            [
                "siren",
                "nom",
                "prenoms",
                "nom_usage",
                "date_de_naissance",
                "nationalite",
                "date_mise_a_jour",
            ],
            dropna=False,
        )["role_description"]
        .apply(lambda x: ", ".join(str(val) for val in x if val is not None))
        .reset_index()
    )
    return pp_clean


def preprocess_dirigeant_pm(query):
    cols = [column[0] for column in query.description]
    dirig_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
    dirig_chunk = dirig_chunk.applymap(lambda x: None if pd.isna(x) else x)
    dirig_chunk.sort_values(
        by=[
            "siren",
            "date_mise_a_jour",
            "denomination",
            "siren_dirigeant",
            "role",
            "forme_juridique",
        ],
        inplace=True,
        ascending=[True, True, False, False, False, False],
    )
    dirig_chunk.drop_duplicates(
        subset=["siren", "date_mise_a_jour", "denomination", "siren_dirigeant", "role"],
        keep="first",
        inplace=True,
    )
    dirig_chunk["denomination"] = dirig_chunk["denomination"].str.upper()

    # Map role numbers to descriptions
    dirig_chunk["role_description"] = map_roles(dirig_chunk["role"])

    dirig_clean = (
        dirig_chunk.groupby(
            by=["siren", "siren_dirigeant", "denomination"], dropna=False
        )["role_description"]
        .apply(lambda x: ", ".join(str(val) for val in x if val is not None))
        .reset_index()
    )
    return dirig_clean
