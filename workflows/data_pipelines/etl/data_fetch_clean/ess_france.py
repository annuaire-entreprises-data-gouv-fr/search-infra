import pandas as pd
from helpers.settings import Settings


def preprocess_ess_france_data(data_dir):
    df_ess = pd.read_csv(
        Settings.URL_MINIO_ESS_FRANCE,
        dtype={
            "siren": "object",
            "est_ess_france": "bool",
        },
    )
    return df_ess
