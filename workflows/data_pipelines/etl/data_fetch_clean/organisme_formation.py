import pandas as pd
from config import URL_MINIO_ORGANISME_FORMATION


def preprocess_organisme_formation_data(data_dir):
    df_organisme_formation = pd.read_csv(
        URL_MINIO_ORGANISME_FORMATION,
        dtype={
            "siren": "object",
            "liste_id_organisme_formation": "object",
            "est_qualiopi": "bool",
        },
    )
    return df_organisme_formation
