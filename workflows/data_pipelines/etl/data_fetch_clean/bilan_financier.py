import pandas as pd
from dag_datalake_sirene.config import URL_MINIO_BILANS_FINANCIERS


def preprocess_bilan_financier_data(data_dir):
    df_bilan = pd.read_csv(URL_MINIO_BILANS_FINANCIERS, dtype=str)
    df_bilan["ca"] = df_bilan["ca"].astype(float)
    df_bilan["resultat_net"] = df_bilan["resultat_net"].astype(float)

    return df_bilan
