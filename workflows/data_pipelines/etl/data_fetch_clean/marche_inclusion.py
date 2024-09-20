import pandas as pd
from dag_datalake_sirene.helpers.utils import get_last_modified
from dag_datalake_sirene.config import URL_MINIO_MARCHE_INCLUSION


def preprocess_marche_inclusion_data(data_dir, **kwargs):
    df_siae = pd.read_csv(
        URL_MINIO_MARCHE_INCLUSION,
        dtype=str,
    )
    df_siae["siren"] = df_siae["siret"].str[0:9]

    df_siae_grouped = (
        df_siae.groupby("siren")["kind"].agg(lambda x: list(set(x))).reset_index()
    )
    df_siae_grouped.rename(columns={"kind": "type_siae"}, inplace=True)
    df_siae_grouped["type_siae"] = df_siae_grouped["type_siae"].astype(str)

    df_siae_grouped["est_siae"] = True

    del df_siae

    # Get the last modified date of the CSV file
    last_modified = get_last_modified(URL_MINIO_MARCHE_INCLUSION)
    kwargs["ti"].xcom_push(key="marche_inclusion_last_modified", value=last_modified)

    return df_siae_grouped
