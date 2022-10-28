import filecmp
import os
import zipfile
from ast import literal_eval

import pandas as pd
import requests
from airflow.models import Variable
from elasticsearch import helpers
from elasticsearch_dsl import connections

ELASTIC_PASSWORD = Variable.get("ELASTIC_PASSWORD")
ELASTIC_URL = Variable.get("ELASTIC_URL")
ELASTIC_USER = Variable.get("ELASTIC_USER")
ENV = Variable.get("ENV")


def preprocess_colter_data(
    data_dir,
    **kwargs,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    # Process Régions
    df_regions = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/619ee62e-8f9e-4c62-b166-abc6f2b86201",
        dtype=str,
        sep=";",
    )
    df_regions = df_regions[df_regions["exer"] == df_regions.exer.max()][["reg_code", "siren"]]
    df_regions = df_regions.drop_duplicates(keep="first")
    df_regions = df_regions.rename(columns={"reg_code": "colter_code_insee"})
    df_regions["colter_code"] = df_regions["colter_code_insee"]
    df_regions["colter_niveau"] = "region"

    # Cas particulier Corse
    df_regions.loc[df_regions["colter_code_insee"] == "94", "colter_niveau"] = "particulier"
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
    df_communes = df_communes[["colter_code_insee", "siren", "colter_code", "colter_niveau"]]
    df_communes.loc[df_communes["colter_code_insee"] == "75056", "colter_code"] = "75C"
    df_communes.loc[df_communes["colter_code_insee"] == "75056", "colter_niveau"] = "particulier"

    df_colter = pd.concat([df_colter, df_communes])

    if os.path.exists(data_dir + "colter-new.csv"):
        os.remove(data_dir + "colter-new.csv")

    df_colter.to_csv(data_dir + "colter-new.csv", index=False)


def preprocess_convcollective_data(
    data_dir,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    cc_url = (
        "https://www.data.gouv.fr/fr/datasets/r/bfc3a658-c054-4ecc-ba4b-22f3f5789dc7"
    )
    r = requests.get(cc_url)
    with open(data_dir + "convcollective-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_conv_coll = pd.read_csv(
        data_dir + "convcollective-download.csv",
        dtype=str,
        names=["mois", "siret", "idcc", "date_maj"],
        header=0,
    )
    df_conv_coll["siren"] = df_conv_coll["siret"].str[0:9]
    df_conv_coll["idcc"] = df_conv_coll["idcc"].apply(lambda x: str(x).replace(" ", ""))
    liste_cc = (
        df_conv_coll.groupby(by=["siren"])["idcc"].apply(list).reset_index(name="liste_idcc")
    )
    liste_cc.to_csv(data_dir + "convcollective-new.csv", index=False)


def preprocess_finess_data(
    data_dir,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
    )
    with open(data_dir + "finess-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_finess = pd.read_csv(
        data_dir + "finess-download.csv",
        dtype=str,
        sep=";",
        encoding="Latin-1",
        skiprows=1,
        header=None,
    )
    df_finess = df_finess[[1, 18, 22]]
    df_finess = df_finess.rename(columns={1: "finess", 18: "cat_etablissement", 22: "siren"})
    df_finess["siren"] = df_finess["siren"].str[:9]
    df_finess = df_finess[df_finess["siren"].notna()]
    agg_finess = df_finess.groupby(["siren"])["finess"].apply(list).reset_index(name="liste_finess")
    agg_finess = agg_finess[["siren", "liste_finess"]]
    agg_finess.to_csv(data_dir + "finess-new.csv", index=False)


def process_elus_files(url, colname):
    df_elus = pd.read_csv(url, dtype=str, sep="\t")
    df_elus = df_elus[
        [
            colname,
            "Nom de l'élu",
            "Prénom de l'élu",
            "Code sexe",
            "Date de naissance",
            "Libellé de la fonction",
        ]
    ]
    df_elus = df_elus.rename(
        columns={
            colname: "colter_code",
            "Nom de l'élu": "nom_elu",
            "Prénom de l'élu": "prenom_elu",
            "Code sexe": "sexe_elu",
            "Date de naissance": "date_naissance_elu",
            "Libellé de la fonction": "fonction_elu",
        }
    )
    return df_elus


def preprocess_elu_data(
    data_dir,
    **kwargs,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)

    df_colter = pd.read_csv(data_dir + "colter-new.csv", dtype=str)
    # Conseillers régionaux
    elus = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/430e13f9-834b-4411-a1a8-da0b4b6e715c",
        "Code de la région",
    )
    # Conseillers départementaux
    df_elus_deps = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/601ef073-d986-4582-8e1a-ed14dc857fba",
        "Code du département",
    )
    df_elus_deps["colter_code"] = df_elus_deps["colter_code"] + "D"
    df_elus_deps.loc[df_elus_deps["colter_code"] == "6AED", "colter_code"] = "6AE"
    elus = pd.concat([elus, df_elus_deps])
    # membres des assemblées des collectivités à statut particulier
    df_elus_part = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/a595be27-cfab-4810-b9d4-22e193bffe35",
        "Code de la collectivité à statut particulier",
    )
    df_elus_part.loc[df_elus_part["colter_code"] == "972", "colter_code"] = "02"
    df_elus_part.loc[df_elus_part["colter_code"] == "973", "colter_code"] = "03"
    elus = pd.concat([elus, df_elus_part])
    # Conseillers communautaires
    df_elus_epci = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/41d95d7d-b172-4636-ac44-32656367cdc7",
        "N° SIREN",
    )
    elus = pd.concat([elus, df_elus_epci])
    # Conseillers municipaux
    df_elus_epci = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/d5f400de-ae3f-4966-8cb6-a85c70c6c24a",
        "Code de la commune",
    )
    df_elus_epci.loc[df_elus_epci["colter_code"] == "75056", "colter_code"] = "75C"
    elus = pd.concat([elus, df_elus_epci])
    df_colter_elus = elus.merge(df_colter, on="colter_code", how="left")
    df_colter_elus = df_colter_elus[df_colter_elus["siren"].notna()]
    df_colter_elus["date_naissance_elu"] = df_colter_elus["date_naissance_elu"].apply(
        lambda x: x.split("/")[2] + "-" + x.split("/")[1] + "-" + x.split("/")[0]
    )
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
    df_colter_elus.to_csv(data_dir + "elu-new.csv", index=False)


def preprocess_rge_data(
    data_dir,
) -> None:
    from typing import List

    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    list_rge: List[str] = []
    r = requests.get(
        "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/"
        "lines?size=10000&select=siret%2Ccode_qualification"
    )
    data = r.json()
    list_rge = list_rge + data["results"]
    cpt = 0
    print(data)
    while "next" in data:
        cpt = cpt + 1
        r = requests.get(data["next"])
        data = r.json()
        list_rge = list_rge + data["results"]
    df_rge = pd.DataFrame(list_rge)
    df_rge["siren"] = df_rge["siret"].str[:9]
    agg_rge = (
        df_rge.groupby(["siren"])["code_qualification"]
        .apply(list)
        .reset_index(name="liste_rge")
    )
    agg_rge = agg_rge[["siren", "liste_rge"]]

    agg_rge.to_csv(data_dir + "rge-new.csv", index=False)


def preprocess_spectacle_data(
    data_dir,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)

    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/fb6c3b2e-da8c-4e69-a719-6a96329e4cb2"
    )
    with open(data_dir + "spectacle-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_spectacle = pd.read_csv(data_dir + "spectacle-download.csv", dtype=str, sep=";")
    df_spectacle = df_spectacle[df_spectacle["statut_du_recepisse"] == "Valide"]
    df_spectacle["is_entrepreneur_spectacle"] = True
    df_spectacle["siren"] = df_spectacle["siren_personne_physique_siret_personne_morale"].str[:9]
    df_spectacle = df_spectacle[["siren", "is_entrepreneur_spectacle"]]
    df_spectacle = df_spectacle[df_spectacle["siren"].notna()]
    df_spectacle.to_csv(data_dir + "spectacle-new.csv", index=False)


def preprocess_uai_data(
    data_dir,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)

    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/b22f04bf-64a8-495d-b8bb-d84dbc4c7983"
    )
    with open(data_dir + "uai-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_uai = pd.read_csv(data_dir + "uai-download.csv", dtype=str, sep=";")
    df_uai = df_uai[["identifiant_de_l_etablissement", "siren_siret", "code_nature"]]
    df_uai = df_uai.rename(
        columns={"identifiant_de_l_etablissement": "uai", "siren_siret": "siren"}
    )
    df_uai["siren"] = df_uai["siren"].str[:9]
    agg_uai = df_uai.groupby(["siren"])["uai"].apply(list).reset_index(name="liste_uai")
    agg_uai = agg_uai[["siren", "liste_uai"]]
    agg_uai.to_csv(data_dir + "uai-new.csv", index=False)


def compare_versions_file(
    original_file: str,
    new_file: str,
):
    should_continue = not filecmp.cmp(original_file, new_file)
    return should_continue


def generate_updates_colter(df_colter, current_color):
    for index, row in df_colter.iterrows():
        colter_code_insee = row["colter_code_insee"]
        if row["colter_code_insee"] != row["colter_code_insee"]:
            colter_code_insee = None
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "colter_code_insee": colter_code_insee,
                "colter_code": row["colter_code"],
                "colter_niveau": row["colter_niveau"],
            },
        }


def generate_updates_convcollective(df_conv_coll, current_color):
    df_conv_coll["liste_idcc"] = df_conv_coll["liste_idcc"].apply(literal_eval)
    for index, row in df_conv_coll.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_idcc": list(set(row["liste_idcc"])),
            },
        }


def generate_updates_elu(df_elus, current_color):
    for col in df_elus.columns:
        df_elus = df_elus.rename(columns={col: col.replace("_elu", "")})

    for siren in df_elus["siren"].unique():
        df_elus_siren = df_elus[df_elus["siren"] == siren]
        list_elus = []
        del df_elus_siren["siren"]
        for index, row in df_elus_siren.iterrows():
            for col in row.to_dict():
                if row[col] != row[col]:
                    row[col] = None

            list_elus.append(row.to_dict())
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": siren,
            "doc": {
                "colter_elus": list_elus,
            },
        }


def generate_updates_finess(df_finess, current_color):
    df_finess["liste_finess"] = df_finess["liste_finess"].apply(literal_eval)
    for index, row in df_finess.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_finess": list(set(row["liste_finess"])),
            },
        }


def generate_updates_nondiff(df_nondiff, current_color):
    for index, row in df_nondiff.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "is_nondiffusible": True,
            },
        }


def generate_updates_rge(df_rge, current_color):
    df_rge["liste_rge"] = df_rge["liste_rge"].apply(literal_eval)
    for index, row in df_rge.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_rge": list(set(row["liste_rge"])),
            },
        }


def generate_updates_spectacle(df_spectacle, current_color):
    for index, row in df_spectacle.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "is_entrepreneur_spectacle": True,
            },
        }


def generate_updates_uai(df_uai, current_color):
    df_uai["liste_uai"] = df_uai["liste_uai"].apply(literal_eval)
    for index, row in df_uai.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_uai": list(set(row["liste_uai"])),
            },
        }


def update_es(
    type_file,
    new_file,
    error_file,
    select_color,
    **kwargs,
) -> None:
    if select_color == "current":
        color = kwargs["ti"].xcom_pull(key="current_color", task_ids="get_colors")
    if select_color == "next":
        color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")

    df = pd.read_csv(new_file, dtype=str)
    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    elastic_connection = connections.get_connection()
    list_errors = []
    list_success = []

    if type_file == "rge":
        generations = generate_updates_rge(df, color)
    if type_file == "spectacle":
        generations = generate_updates_spectacle(df, color)
    if type_file == "convcollective":
        generations = generate_updates_convcollective(df, color)
    if type_file == "uai":
        generations = generate_updates_uai(df, color)
    if type_file == "finess":
        generations = generate_updates_finess(df, color)
    if type_file == "colter":
        generations = generate_updates_colter(df, color)
    if type_file == "elu":
        generations = generate_updates_elu(df, color)

    for success, details in helpers.parallel_bulk(
        elastic_connection, generations, chunk_size=1500, raise_on_error=False
    ):
        if not success:
            list_errors.append(details["update"]["_id"])
        else:
            list_success.append(details["update"]["_id"])

    print(str(len(list_errors)) + " siren non trouvé.")
    print(str(len(list_success)) + " documents indexés")
    print("Extrait", list_success[:10])

    with open("/".join(new_file.split("/")[:-1]) + "/" + error_file, "w") as fp:
        fp.write("\n".join(list_errors))


def publish_mattermost(
    text,
) -> None:
    data = {"text": text}
    if ENV == "dev-geoff":
        r = requests.post(
            "https://mattermost.incubateur.net/hooks/geww4je6minn9p9m6qq6xiwu3a",
            json=data,
        )
        print(r.status_code)
