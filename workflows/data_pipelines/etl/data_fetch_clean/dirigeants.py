import pandas as pd

from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.data_enrichment import (
    map_roles,
)

DIRIGEANT_PP_KEYS = [
    "siren",
    "nom",
    "prenoms",
    "nom_usage",
    "date_de_naissance",
    "nationalite",
    "date_mise_a_jour",
]
DIRIGEANT_PM_KEYS = ["siren", "siren_dirigeant", "denomination"]


def concatenate_roles(roles):
    return ", ".join(str(role) for role in roles if pd.notna(role))


def group_roles(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Aggregate the roles of each dirigeant into a single row. But if a dirigeant has
    multiple time the same role then each occurrence of that role goes to a separate row.
    Those may be fraud and need to be shown as duplicates.
    Example of an extreme case :
        Roles of a dirigeant = 75, 30, 30, 40, 40, 40, 40
        Occurences (cumcount) = 0,  0,  1,  0,  1,  2,  3
        Ouput :
            - Row 0 : 75, 30, 40
            - Row 1 : 30, 40
            - Row 2 : 40
            - Row 3 : 40
    """
    non_role_columns = [
        column
        for column in df.columns
        if column not in keys and column not in ("role", "role_description")
    ]
    df = df.sort_values(by=keys + ["role"])
    df["occurrence"] = df.groupby(keys + ["role"], dropna=False).cumcount()
    return (
        df.groupby(keys + ["occurrence"], dropna=False, sort=False)
        .agg(
            role=("role", concatenate_roles),
            role_description=("role_description", concatenate_roles),
            **{column: (column, "first") for column in non_role_columns},
        )
        .reset_index()
        .drop(columns="occurrence")
    )


def preprocess_dirigeant_pp(query):
    cols = [column[0] for column in query.description]
    pp_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    for column in ["nom", "prenoms", "nom_usage"]:
        pp_chunk[column] = pp_chunk[column].str.upper()

    pp_chunk["role_description"] = map_roles(pp_chunk["role"])
    return group_roles(pp_chunk, DIRIGEANT_PP_KEYS)


def preprocess_dirigeant_pm(query):
    cols = [column[0] for column in query.description]
    dirig_chunk = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)

    dirig_chunk["denomination"] = dirig_chunk["denomination"].str.upper()

    dirig_chunk["role_description"] = map_roles(dirig_chunk["role"])
    return group_roles(dirig_chunk, DIRIGEANT_PM_KEYS)
