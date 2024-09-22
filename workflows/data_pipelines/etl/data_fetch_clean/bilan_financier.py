import pandas as pd
from helpers.settings import Settings

def preprocess_bilan_financier_data(data_dir):
    df_bilan = pd.read_csv(Settings.URL_BILANS_FINANCIERS, dtype=str)
    df_bilan["ca"] = df_bilan["ca"].astype(float)
    df_bilan["resultat_net"] = df_bilan["resultat_net"].astype(float)

    return df_bilan
