import pandas as pd
import requests
import zipfile
from dag_datalake_sirene.config import (
    URL_COLTER_REGIONS,
    URL_COLTER_DEP,
    URL_COLTER_EPCI,
    URL_COLTER_COMMUNES,
    URL_ELUS_EPCI,
    URL_CONSEILLERS_REGIONAUX,
    URL_CONSEILLERS_DEPARTEMENTAUX,
    URL_CONSEILLERS_MUNICIPAUX,
    URL_ASSEMBLEE_COL_STATUT_PARTICULIER,
)


def preprocess_colter_data(data_dir, **kwargs):
    # Process Régions
    df_regions = pd.read_csv(URL_COLTER_REGIONS, dtype=str, sep=";")
    df_regions = df_regions[df_regions["exer"] == df_regions.exer.max()][
        ["reg_code", "siren"]
    ]
    df_regions = df_regions.drop_duplicates(keep="first")
    df_regions = df_regions.rename(columns={"reg_code": "colter_code_insee"})
    df_regions["colter_code"] = df_regions["colter_code_insee"]
    df_regions["colter_niveau"] = "region"

    # Cas particulier Corse
    df_regions.loc[df_regions["colter_code_insee"] == "94", "colter_niveau"] = (
        "particulier"
    )
    df_colter = df_regions

    # Process Départements
    df_deps = pd.read_csv(URL_COLTER_DEP, dtype=str, sep=";")
    df_deps = df_deps[df_deps["exer"] == df_deps["exer"].max()]
    df_deps = df_deps[["dep_code", "siren"]]
    df_deps = df_deps.drop_duplicates(keep="first")
    df_deps = df_deps.rename(columns={"dep_code": "colter_code_insee"})
    df_deps["colter_code"] = df_deps["colter_code_insee"] + "D"
    df_deps["colter_niveau"] = "departement"

    # Cas Métropole de Lyon
    df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_code"] = "69M"
    df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_niveau"] = "particulier"
    df_deps.loc[df_deps["colter_code_insee"] == "691", "colter_code_insee"] = None

    # Cas Conseil départemental du Rhone
    df_deps.loc[df_deps["colter_code_insee"] == "69", "colter_niveau"] = "particulier"
    df_deps.loc[df_deps["colter_code_insee"] == "69", "colter_code_insee"] = None

    # Cas Collectivité Européenne d"Alsace
    df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_code"] = "6AE"
    df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_niveau"] = "particulier"
    df_deps.loc[df_deps["colter_code_insee"] == "67A", "colter_code_insee"] = None

    # Remove Paris
    df_deps = df_deps[df_deps["colter_code_insee"] != "75"]

    df_colter = pd.concat([df_colter, df_deps])

    # Process EPCI
    df_epci = pd.read_excel(URL_COLTER_EPCI, dtype=str, engine="openpyxl")
    df_epci["colter_code_insee"] = None
    df_epci["siren"] = df_epci["siren_epci"]
    df_epci["colter_code"] = df_epci["siren"]
    df_epci["colter_niveau"] = "epci"
    df_epci = df_epci[["colter_code_insee", "siren", "colter_code", "colter_niveau"]]
    df_colter = pd.concat([df_colter, df_epci])

    # Process Communes
    response = requests.get(URL_COLTER_COMMUNES)
    open(data_dir + "siren-communes.zip", "wb").write(response.content)

    with zipfile.ZipFile(data_dir + "siren-communes.zip", "r") as zip_ref:
        zip_ref.extractall(data_dir + "siren-communes")

    df_communes = pd.read_excel(
        data_dir + "siren-communes/Banatic_SirenInsee2022.xlsx",
        dtype=str,
        engine="openpyxl",
    )
    df_communes["colter_code_insee"] = df_communes["insee"]
    df_communes["colter_code"] = df_communes["insee"]
    df_communes["colter_niveau"] = "commune"
    df_communes = df_communes[
        ["colter_code_insee", "siren", "colter_code", "colter_niveau"]
    ]
    df_communes.loc[df_communes["colter_code_insee"] == "75056", "colter_code"] = "75C"
    df_communes.loc[df_communes["colter_code_insee"] == "75056", "colter_niveau"] = (
        "particulier"
    )

    df_colter = pd.concat([df_colter, df_communes])
    df_colter.to_csv(data_dir + "colter-new.csv", index=False)
    del df_communes

    return df_colter


def preprocess_elus_data(data_dir):
    df_colter = pd.read_csv(data_dir + "colter-new.csv", dtype=str)
    # Conseillers régionaux
    elus = process_elus_files(
        URL_CONSEILLERS_REGIONAUX,
        "Code de la région",
    )

    # Conseillers départementaux
    df_elus_deps = process_elus_files(
        URL_CONSEILLERS_DEPARTEMENTAUX,
        "Code du département",
    )
    df_elus_deps["colter_code"] = df_elus_deps["colter_code"] + "D"
    df_elus_deps.loc[df_elus_deps["colter_code"] == "6AED", "colter_code"] = "6AE"
    elus = pd.concat([elus, df_elus_deps])

    # membres des assemblées des collectivités à statut particulier
    df_elus_part = process_elus_files(
        URL_ASSEMBLEE_COL_STATUT_PARTICULIER,
        "Code de la collectivité à statut particulier",
    )
    df_elus_part.loc[df_elus_part["colter_code"] == "972", "colter_code"] = "02"
    df_elus_part.loc[df_elus_part["colter_code"] == "973", "colter_code"] = "03"
    elus = pd.concat([elus, df_elus_part])
    # Conseillers communautaires
    df_elus_epci = process_elus_files(
        URL_ELUS_EPCI,
        "N° SIREN",
    )
    elus = pd.concat([elus, df_elus_epci])
    # Conseillers municipaux
    df_elus_epci = process_elus_files(
        URL_CONSEILLERS_MUNICIPAUX,
        "Code de la commune",
    )
    df_elus_epci.loc[df_elus_epci["colter_code"] == "75056", "colter_code"] = "75C"
    elus = pd.concat([elus, df_elus_epci])
    df_colter_elus = elus.merge(df_colter, on="colter_code", how="left")
    df_colter_elus = df_colter_elus[df_colter_elus["siren"].notna()]
    df_colter_elus = df_colter_elus[
        [
            "siren",
            "nom_elu",
            "prenom_elu",
            "date_naissance_elu",
            "sexe_elu",
            "fonction_elu",
        ]
    ]
    for col in df_colter_elus.columns:
        df_colter_elus = df_colter_elus.rename(columns={col: col.replace("_elu", "")})

    del elus
    del df_elus_part
    del df_colter
    del df_elus_epci

    return df_colter_elus


def process_elus_files(url, colname):
    df_elus = pd.read_csv(url, dtype=str)

    column_mapping = {
        "Nom de l'élu": "nom_elu",
        "Prénom de l'élu": "prenom_elu",
        "Code sexe": "sexe_elu",
        "Date de naissance": "date_naissance_elu",
        "Libellé de la fonction": "libelle_fonction",
    }

    common_columns = [
        colname,
        "nom_elu",
        "prenom_elu",
        "sexe_elu",
        "date_naissance_elu",
        "libelle_fonction",
    ]

    df_elus = df_elus.rename(columns=column_mapping)[common_columns]
    df_elus = df_elus.rename(
        columns={colname: "colter_code", "libelle_fonction": "fonction_elu"}
    )

    return df_elus
