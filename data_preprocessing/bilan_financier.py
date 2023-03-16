import pandas as pd


def preprocess_bilan_financier_data(data_dir):
    df_bilan = pd.read_csv(
        "https://object.files.data.gouv.fr/data-pipeline-open/"
        "prod/signaux_faibles/latest/synthese_bilans.csv",
        dtype=str,
    )
    df_bilan = df_bilan.rename(columns={"chiffre_d_affaires": "ca"})
    df_bilan["ca"] = df_bilan["ca"].astype(float)
    df_bilan["resultat_net"] = df_bilan["resultat_net"].astype(float)

    return df_bilan
