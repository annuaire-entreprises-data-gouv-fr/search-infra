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
    df = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/619ee62e-8f9e-4c62-b166-abc6f2b86201",
        dtype=str,
        sep=";",
    )
    df = df[df["exer"] == df.exer.max()][["reg_code", "siren"]]
    df = df.drop_duplicates(keep="first")
    df = df.rename(columns={"reg_code": "colter_code_insee"})
    df["colter_code"] = df["colter_code_insee"]
    df["colter_niveau"] = "region"

    # Cas particulier Corse
    df.loc[df["colter_code_insee"] == "94", "colter_niveau"] = "particulier"
    dfcolter = df

    # Process Départements
    df = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/2f4f901d-e3ce-4760-b122-56a311340fc4",
        dtype=str,
        sep=";",
    )
    df = df[df["exer"] == df["exer"].max()]
    df = df[["dep_code", "siren"]]
    df = df.drop_duplicates(keep="first")
    df = df.rename(columns={"dep_code": "colter_code_insee"})
    df["colter_code"] = df["colter_code_insee"] + "D"
    df["colter_niveau"] = "departement"

    # Cas Métropole de Lyon
    df.loc[df["colter_code_insee"] == "691", "colter_code"] = "69M"
    df.loc[df["colter_code_insee"] == "691", "colter_niveau"] = "particulier"
    df.loc[df["colter_code_insee"] == "691", "colter_code_insee"] = None

    # Cas Conseil départemental du Rhone
    df.loc[df["colter_code_insee"] == "69", "colter_niveau"] = "particulier"
    df.loc[df["colter_code_insee"] == "69", "colter_code_insee"] = None

    # Cas Collectivité Européenne d"Alsace
    df.loc[df["colter_code_insee"] == "67A", "colter_code"] = "6AE"
    df.loc[df["colter_code_insee"] == "67A", "colter_niveau"] = "particulier"
    df.loc[df["colter_code_insee"] == "67A", "colter_code_insee"] = None

    # Remove Paris
    df = df[df["colter_code_insee"] != "75"]

    dfcolter = pd.concat([dfcolter, df])

    # Process EPCI
    df = pd.read_excel(
        "https://www.collectivites-locales.gouv.fr/files/2022/epcisanscom2022.xlsx",
        dtype=str,
        engine="openpyxl",
    )
    df["colter_code_insee"] = None
    df["siren"] = df["siren_epci"]
    df["colter_code"] = df["siren"]
    df["colter_niveau"] = "epci"
    df = df[["colter_code_insee", "siren", "colter_code", "colter_niveau"]]
    dfcolter = pd.concat([dfcolter, df])

    # Process Communes
    URL = "https://www.data.gouv.fr/fr/datasets/r/42b16d68-958e-4518-8551-93e095fe8fda"
    response = requests.get(URL)
    open(data_dir + "siren-communes.zip", "wb").write(response.content)

    with zipfile.ZipFile(data_dir + "siren-communes.zip", "r") as zip_ref:
        zip_ref.extractall(data_dir + "siren-communes")

    df = pd.read_excel(
        data_dir + "siren-communes/Banatic_SirenInsee2022.xlsx",
        dtype=str,
        engine="openpyxl",
    )
    df["colter_code_insee"] = df["insee"]
    df["colter_code"] = df["insee"]
    df["colter_niveau"] = "commune"
    df = df[["colter_code_insee", "siren", "colter_code", "colter_niveau"]]
    df.loc[df["colter_code_insee"] == "75056", "colter_code"] = "75C"
    df.loc[df["colter_code_insee"] == "75056", "colter_niveau"] = "particulier"

    dfcolter = pd.concat([dfcolter, df])

    if os.path.exists(data_dir + "colter-new.csv"):
        os.remove(data_dir + "colter-new.csv")

    dfcolter.to_csv(data_dir + "colter-new.csv", index=False)


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

    cc_df = pd.read_csv(
        data_dir + "convcollective-download.csv",
        dtype=str,
        names=["mois", "siret", "idcc", "date_maj"],
        header=0,
    )
    cc_df["siren"] = cc_df["siret"].str[0:9]
    cc_df["idcc"] = cc_df["idcc"].apply(lambda x: str(x).replace(" ", ""))
    liste_cc = (
        cc_df.groupby(by=["siren"])["idcc"].apply(list).reset_index(name="liste_idcc")
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

    df = pd.read_csv(
        data_dir + "finess-download.csv",
        dtype=str,
        sep=";",
        encoding="Latin-1",
        skiprows=1,
        header=None,
    )
    df = df[[1, 18, 22]]
    df = df.rename(columns={1: "finess", 18: "cat_etablissement", 22: "siren"})
    df["siren"] = df["siren"].str[:9]
    df = df[df["siren"].notna()]
    res = df.groupby(["siren"])["finess"].apply(list).reset_index(name="liste_finess")
    res2 = (
        df.groupby(["siren"])["cat_etablissement"]
        .apply(list)
        .reset_index(name="liste_cat_etablissement")
    )
    res = res.merge(res2, on="siren", how="left")
    res = res[["siren", "liste_finess"]]
    res.to_csv(data_dir + "finess-new.csv", index=False)


def process_elus_files(url, colname):
    df = pd.read_csv(url, dtype=str, sep="\t")
    df = df[
        [
            colname,
            "Nom de l'élu",
            "Prénom de l'élu",
            "Code sexe",
            "Date de naissance",
            "Libellé de la fonction",
        ]
    ]
    df = df.rename(
        columns={
            colname: "colter_code",
            "Nom de l'élu": "nom_elu",
            "Prénom de l'élu": "prenom_elu",
            "Code sexe": "sexe_elu",
            "Date de naissance": "date_naissance_elu",
            "Libellé de la fonction": "fonction_elu",
        }
    )
    return df


def preprocess_elu_data(
    data_dir,
    **kwargs,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)

    colter = pd.read_csv(data_dir + "colter-new.csv", dtype=str)
    # Conseillers régionaux
    elus = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/430e13f9-834b-4411-a1a8-da0b4b6e715c",
        "Code de la région",
    )
    # Conseillers départementaux
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/601ef073-d986-4582-8e1a-ed14dc857fba",
        "Code du département",
    )
    df["colter_code"] = df["colter_code"] + "D"
    df.loc[df["colter_code"] == "6AED", "colter_code"] = "6AE"
    elus = pd.concat([elus, df])
    # membres des assemblées des collectivités à statut particulier
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/a595be27-cfab-4810-b9d4-22e193bffe35",
        "Code de la collectivité à statut particulier",
    )
    df.loc[df["colter_code"] == "972", "colter_code"] = "02"
    df.loc[df["colter_code"] == "973", "colter_code"] = "03"
    elus = pd.concat([elus, df])
    # Conseillers communautaires
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/41d95d7d-b172-4636-ac44-32656367cdc7",
        "N° SIREN",
    )
    elus = pd.concat([elus, df])
    # Conseillers municipaux
    df = process_elus_files(
        "https://www.data.gouv.fr/fr/datasets/r/d5f400de-ae3f-4966-8cb6-a85c70c6c24a",
        "Code de la commune",
    )
    df.loc[df["colter_code"] == "75056", "colter_code"] = "75C"
    elus = pd.concat([elus, df])
    colter_elus = elus.merge(colter, on="colter_code", how="left")
    colter_elus = colter_elus[colter_elus["siren"].notna()]
    colter_elus["date_naissance_elu"] = colter_elus["date_naissance_elu"].apply(
        lambda x: x.split("/")[2] + "-" + x.split("/")[1] + "-" + x.split("/")[0]
    )
    colter_elus = colter_elus[
        [
            "siren",
            "nom_elu",
            "prenom_elu",
            "date_naissance_elu",
            "sexe_elu",
            "fonction_elu",
        ]
    ]
    colter_elus.to_csv(data_dir + "elu-new.csv", index=False)


def preprocess_rge_data(
    data_dir,
) -> None:
    from typing import List

    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    arr: List[str] = []
    r = requests.get(
        "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/"
        "lines?size=10000&select=siret%2Ccode_qualification"
    )
    data = r.json()
    arr = arr + data["results"]
    cpt = 0
    print(data)
    while "next" in data:
        cpt = cpt + 1
        r = requests.get(data["next"])
        data = r.json()
        arr = arr + data["results"]
    df = pd.DataFrame(arr)
    df["siren"] = df["siret"].str[:9]
    res = (
        df.groupby(["siren"])["code_qualification"]
        .apply(list)
        .reset_index(name="liste_rge")
    )
    res = res[["siren", "liste_rge"]]

    res.to_csv(data_dir + "rge-new.csv", index=False)


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

    df = pd.read_csv(data_dir + "spectacle-download.csv", dtype=str, sep=";")
    df = df[df["statut_du_recepisse"] == "Valide"]
    df["is_entrepreneur_spectacle"] = True
    df["siren"] = df["siren_personne_physique_siret_personne_morale"].str[:9]
    df = df[["siren", "is_entrepreneur_spectacle"]]
    df = df[df["siren"].notna()]
    df.to_csv(data_dir + "spectacle-new.csv", index=False)


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

    df = pd.read_csv(data_dir + "uai-download.csv", dtype=str, sep=";")
    df = df[["identifiant_de_l_etablissement", "siren_siret", "code_nature"]]
    df = df.rename(
        columns={"identifiant_de_l_etablissement": "uai", "siren_siret": "siren"}
    )
    df["siren"] = df["siren"].str[:9]
    res = df.groupby(["siren"])["uai"].apply(list).reset_index(name="liste_uai")
    res2 = (
        df.groupby(["siren"])["code_nature"]
        .apply(list)
        .reset_index(name="liste_code_nature")
    )
    res = res.merge(res2, on="siren", how="left")
    res = res[["siren", "liste_uai"]]
    res.to_csv(data_dir + "uai-new.csv", index=False)


def compare_versions_file(
    original_file: str,
    new_file: str,
):
    should_continue = not filecmp.cmp(original_file, new_file)
    return should_continue


def generate_updates_colter(df, current_color):
    for index, row in df.iterrows():
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


def generate_updates_convcollective(df, current_color):
    df["liste_idcc"] = df["liste_idcc"].apply(literal_eval)
    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_idcc": list(set(row["liste_idcc"])),
            },
        }


def generate_updates_elu(df, current_color):
    df = df
    for col in df.columns:
        df = df.rename(columns={col: col.replace("_elu", "")})

    for siren in df["siren"].unique():
        inter = df[df["siren"] == siren]
        arr = []
        del inter["siren"]
        for index, row in inter.iterrows():
            for col in row.to_dict():
                if row[col] != row[col]:
                    row[col] = None

            arr.append(row.to_dict())
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": siren,
            "doc": {
                "colter_elus": arr,
            },
        }


def generate_updates_finess(df, current_color):
    df["liste_finess"] = df["liste_finess"].apply(literal_eval)
    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_finess": list(set(row["liste_finess"])),
            },
        }


def generate_updates_nondiff(df, current_color):
    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "is_nondiffusible": True,
            },
        }


def generate_updates_rge(df, current_color):
    df["liste_rge"] = df["liste_rge"].apply(literal_eval)
    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_rge": list(set(row["liste_rge"])),
            },
        }


def generate_updates_spectacle(df, current_color):
    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "is_entrepreneur_spectacle": True,
            },
        }


def generate_updates_uai(df, current_color):
    df["liste_uai"] = df["liste_uai"].apply(literal_eval)
    for index, row in df.iterrows():
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
