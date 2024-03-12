import pandas as pd
from dag_datalake_sirene.helpers.minio_helpers import minio_client


def preprocess_marche_inclusion_data(data_dir):
    minio_client.get_files(
        list_files=[
            {
                "source_path": "marche_inclusion/",
                "source_name": "stock_marche_inclusion.csv",
                "dest_path": f"{data_dir}",
                "dest_name": "marche_inclusion.csv",
            }
        ],
    )
    df_siae = pd.read_csv(
        f"{data_dir}marche_inclusion.csv",
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

    return df_siae_grouped
