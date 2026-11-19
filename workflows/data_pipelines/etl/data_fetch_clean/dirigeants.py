import pandas as pd

from data_pipelines_annuaire.helpers.utils import normalize_string

# fmt: off
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.data_enrichment import (
    map_roles,
)

# fmt: on


def concatenate_roles(roles):
    return ", ".join(str(role) for role in roles if pd.notna(role))


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
        )
        .agg(
            role=("role", concatenate_roles),
            role_description=("role_description", concatenate_roles),
        )
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
        )
        .agg(
            role=("role", concatenate_roles),
            role_description=("role_description", concatenate_roles),
        )
        .reset_index()
    )
    return dirig_clean


def _normalize_dirigeant_name(value: str | None) -> str:
    return " ".join((normalize_string(value) or "").split())


def match_protected_dirigeants(
    dirigeants: pd.DataFrame, protected_list: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Find the dirigeants of the RNE present in the protected list.

    A dirigeant matches when all these values are the same, ignoring case, accents and extra spaces:
    * Siren
    * birth date YYYY-MM, or no birth date on both sides
    * full nom, which is mandatory
    * first prénom, or no prénom on both sides

    Args:
        dirigeants: rowid, siren, nom, prenoms, date_de_naissance
            from the dirigeant_pp table.
        protected_list: siren, nom, prenoms, date_de_naissance
            from the protected list.

    Returns:
        The matched dirigeants with rowid and siren.
        And the rows of the protected list with a nom matching no dirigeant.
        The rows without a nom are skipped since some legacy rows only have a Siren.
    """
    protected = pd.DataFrame(
        {
            "siren": protected_list["siren"],
            "date_de_naissance": protected_list["date_de_naissance"],
            "prenom": protected_list["prenoms"]
            .map(_normalize_dirigeant_name)
            .str.split(" ")
            .str[0],
            "nom": protected_list["nom"].map(_normalize_dirigeant_name),
            "protected_row": protected_list.index,
        }
    )
    protected = protected[protected["nom"] != ""]
    candidates = pd.DataFrame(
        {
            "rowid": dirigeants["rowid"],
            "siren": dirigeants["siren"],
            # date_de_naissance is always an empty string in protected_list
            "date_de_naissance": dirigeants["date_de_naissance"].fillna(""),
            "prenom": dirigeants["prenoms"]
            .map(_normalize_dirigeant_name)
            .str.split(" ")
            .str[0],
            "nom": dirigeants["nom"].map(_normalize_dirigeant_name),
        }
    )
    matched = candidates.merge(
        protected, on=["siren", "date_de_naissance", "prenom", "nom"]
    )
    unmatched = protected_list.loc[protected.index.difference(matched["protected_row"])]
    return matched[["rowid", "siren"]].drop_duplicates(), unmatched
