import pandas as pd


def preprocess_uai_data(data_dir):
    df_uai = pd.read_csv(
        "https://object.files.data.gouv.fr/data-pipeline-open/"
        "prod/uai/latest/annuaire_uai.csv",
    )
    df_list_uai = (
        df_uai.groupby(["siret"])["uai"].apply(list).reset_index(name="liste_uai")
    )
    df_list_uai = df_list_uai[["siret", "liste_uai"]]
    df_list_uai["liste_uai"] = df_list_uai["liste_uai"].astype(str)
    del df_uai

    return df_list_uai
