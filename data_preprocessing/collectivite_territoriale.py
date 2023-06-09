import zipfile

import pandas as pd
import requests


def preprocess_colter_data(data_dir, **kwargs):
    # Process Régions
    df_regions = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/619ee62e-8f9e-4c62-b166-abc6f2b86201",
        dtype=str,
        sep=";",
    )
    df_regions = df_regions[df_regions["exer"] == df_regions.exer.max()][
        ["reg_code", "siren"]
    ]
    df_regions = df_regions.drop_duplicates(keep="first")
    df_regions = df_regions.rename(columns={"reg_code": "colter_code_insee"})
    df_regions["colter_code"] = df_regions["colter_code_insee"]
    df_regions["colter_niveau"] = "region"

    # Cas particulier Corse
    df_regions.loc[
        df_regions["colter_code_insee"] == "94", "colter_niveau"
    ] = "particulier"
    df_colter = df_regions

    # Process Départements
    df_deps = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/2f4f901d-e3ce-4760-b122-56a311340fc4",
        dtype=str,
        sep=";",
    )
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
    df_epci = pd.read_excel(
        "https://www.collectivites-locales.gouv.fr/files/2022/epcisanscom2022.xlsx",
        dtype=str,
        engine="openpyxl",
    )
    df_epci["colter_code_insee"] = None
    df_epci["siren"] = df_epci["siren_epci"]
    df_epci["colter_code"] = df_epci["siren"]
    df_epci["colter_niveau"] = "epci"
    df_epci = df_epci[["colter_code_insee", "siren", "colter_code", "colter_niveau"]]
    df_colter = pd.concat([df_colter, df_epci])

    # Process Communes
    URL = "https://www.data.gouv.fr/fr/datasets/r/42b16d68-958e-4518-8551-93e095fe8fda"
    response = requests.get(URL)
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
    df_communes.loc[
        df_communes["colter_code_insee"] == "75056", "colter_niveau"
    ] = "particulier"

    df_colter = pd.concat([df_colter, df_communes])
    df_colter.to_csv(data_dir + "colter-new.csv", index=False)
    del df_communes

    return df_colter


def preprocess_elus_data(data_dir):
    df_colter = pd.read_csv(data_dir + "colter-new.csv", dtype=str)
    # Conseillers régionaux
    elus = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/430e13f9-834b-4411-a1a8-da0b4b6e715c",
        "code_region",
    )

    # Conseillers départementaux
    df_elus_deps = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/601ef073-d986-4582-8e1a-ed14dc857fba",
        "code_departement",
    )
    df_elus_deps["colter_code"] = df_elus_deps["colter_code"] + "D"
    df_elus_deps.loc[df_elus_deps["colter_code"] == "6AED", "colter_code"] = "6AE"
    elus = pd.concat([elus, df_elus_deps])

    # membres des assemblées des collectivités à statut particulier
    df_elus_part = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/a595be27-cfab-4810-b9d4-22e193bffe35",
        "code_collectivite_statut_particulier",
    )
    df_elus_part.loc[df_elus_part["colter_code"] == "972", "colter_code"] = "02"
    df_elus_part.loc[df_elus_part["colter_code"] == "973", "colter_code"] = "03"
    elus = pd.concat([elus, df_elus_part])
    # Conseillers communautaires
    df_elus_epci = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/41d95d7d-b172-4636-ac44-32656367cdc7",
        "numero_siren",
    )
    elus = pd.concat([elus, df_elus_epci])
    # Conseillers municipaux
    df_elus_epci = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/d5f400de-ae3f-4966-8cb6-a85c70c6c24a",
        "code_commune",
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
    df_elus = pd.read_csv(url, dtype=str, sep="\t")
    if colname == "numero_siren":
        df_elus = df_elus[
            [
                colname,
                "nom_elu",
                "prenom_elu",
                "sexe_elu",
                "date_naissance_elu",
                "code_fonction",
            ]
        ]
        df_elus = df_elus.rename(
            columns={
                colname: "colter_code",
                "code_fonction": "fonction_elu",
            }
        )
        return df_elus
    df_elus = df_elus[
        [
            colname,
            "nom_elu",
            "prenom_elu",
            "sexe_elu",
            "date_naissance_elu",
            "libelle_fonction",
        ]
    ]
    df_elus = df_elus.rename(
        columns={
            colname: "colter_code",
            "libelle_fonction": "fonction_elu",
        }
    )
    return df_elus
