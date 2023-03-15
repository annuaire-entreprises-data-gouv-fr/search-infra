import pandas as pd


def preprocess_organisme_formation_data(data_dir):
    df_organisme_formation = pd.read_csv(
        "https://object.files.data.gouv.fr/data-pipeline-open/"
        "prod/formation/latest/organismes_formation_clean.csv",
        dtype=str,
    )
    df_organisme_formation = df_organisme_formation[
        [
            "id_nda",
            "siret",
        ]
    ]
    df_list_of = (
        df_organisme_formation.groupby(["siret"])[["id_nda"]].agg(list).reset_index()
    )
    df_list_of["id_nda"] = df_list_of["id_nda"].astype(str)
    df_list_of = df_list_of.rename(
        columns={
            "id_nda": "liste_id_organisme_formation",
        }
    )
    del df_organisme_formation

    return df_list_of
