import pandas as pd
from helpers.settings import Settings


def preprocess_spectacle_data(data_dir):
    df_spectacle = pd.read_csv(
        Settings.URL_MINIO_ENTREPRENEUR_SPECTACLE,
        dtype={
            "siren": "object",
            "statut_entrepreneur_spectacle": "object",
            "est_entrepreneur_spectacle": "bool",
        },
    )

    return df_spectacle
