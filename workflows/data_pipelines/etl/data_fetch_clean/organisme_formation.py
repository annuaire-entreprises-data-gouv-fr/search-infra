import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_ORGANISME_FORMATION


def preprocess_organisme_formation_data(data_dir):
    df_organisme_formation = pd.read_csv(URL_MINIO_ORGANISME_FORMATION, dtype=str)
    # Convert 'est_qualiopi' column to boolean dtype
    df_organisme_formation["est_qualiopi"] = df_organisme_formation[
        "est_qualiopi"
    ].astype(bool)

    return df_organisme_formation
