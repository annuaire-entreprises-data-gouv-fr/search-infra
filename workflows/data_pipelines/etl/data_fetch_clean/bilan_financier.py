import pandas as pd
from dag_datalake_sirene.workflows.data_pipelines.bilans_financiers.bilans_financiers_config import (
    BILANS_FINANCIERS_CONFIG,
)


def preprocess_bilan_financier_data(data_dir):
    df_bilan = pd.read_csv(BILANS_FINANCIERS_CONFIG.url_minio, dtype=str)
    df_bilan["ca"] = df_bilan["ca"].astype(float)
    df_bilan["resultat_net"] = df_bilan["resultat_net"].astype(float)

    return df_bilan
