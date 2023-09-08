import pandas as pd
import requests


def preprocess_organisme_formation_data(data_dir):
    # get dataset directly from dge website
    r = requests.get(
        "https://dgefp.opendatasoft.com/api/explore/v2.1/catalog/datasets/liste"
        "-publique-des-of-v2/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels"
        "=true&delimiter=%3B"
    )
    with open(data_dir + "qualiopi-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_organisme_formation = pd.read_csv(
        data_dir + "qualiopi-download.csv", dtype=str, sep=";"
    )
    df_organisme_formation = df_organisme_formation.rename(
        columns={
            "Numéro Déclaration Activité": "id_nda",
            "Code SIREN": "siren",
            "Siret Etablissement Déclarant": "siret",
            "Actions de formations": "cert_adf",
            "Bilans de compétences": "cert_bdc",
            "VAE": "cert_vae",
            "Actions de formations par apprentissage": "cert_app",
            "Certifications": "certifications",
        }
    )
    df_organisme_formation = df_organisme_formation[
        [
            "id_nda",
            "siren",
            "siret",
            "cert_adf",
            "cert_bdc",
            "cert_vae",
            "cert_app",
            "certifications",
        ]
    ]
    df_organisme_formation = df_organisme_formation.where(
        pd.notnull(df_organisme_formation), None
    )
    df_organisme_formation["est_qualiopi"] = df_organisme_formation.apply(
        lambda x: True if x["certifications"] else False, axis=1
    )
    df_organisme_formation = df_organisme_formation[["siren", "est_qualiopi", "id_nda"]]
    df_liste_organisme_formation = (
        df_organisme_formation.groupby(["siren"])[["id_nda"]].agg(list).reset_index()
    )
    df_liste_organisme_formation["id_nda"] = df_liste_organisme_formation[
        "id_nda"
    ].astype(str)
    df_liste_organisme_formation = pd.merge(
        df_liste_organisme_formation,
        df_organisme_formation[["siren", "est_qualiopi"]],
        on="siren",
        how="left",
    )
    df_liste_organisme_formation = df_liste_organisme_formation.rename(
        columns={
            "id_nda": "liste_id_organisme_formation",
        }
    )
    # Drop est_qualiopi=False when True value exists
    df_liste_organisme_formation.sort_values(
        "est_qualiopi", ascending=False, inplace=True
    )
    df_liste_organisme_formation.drop_duplicates(
        subset=["siren", "liste_id_organisme_formation"], keep="first", inplace=True
    )

    del df_organisme_formation

    return df_liste_organisme_formation
