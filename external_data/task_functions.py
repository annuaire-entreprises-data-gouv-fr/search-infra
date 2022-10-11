import filecmp
import os

import pandas as pd
import requests
from airflow.models import Variable
from elasticsearch import helpers
from elasticsearch_dsl import connections

ELASTIC_PASSWORD = Variable.get("ELASTIC_PASSWORD")
ELASTIC_URL = Variable.get("ELASTIC_URL")
ELASTIC_USER = Variable.get("ELASTIC_USER")
ENV = Variable.get("ENV")


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


def preprocess_rge_data(
    data_dir,
) -> None:
    arr = []
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
        .reset_index(name="liste_code_qualification_rge")
    )
    res["is_rge"] = True
    res = res[["siren", "is_rge"]]

    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
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
) -> None:
    same = False
    same = filecmp.cmp(original_file, new_file)
    return not same


def generate_updates_convcollective(df, current_color):
    from ast import literal_eval

    df["liste_idcc"] = df["liste_idcc"].apply(literal_eval)
    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_idcc": row["liste_idcc"],
            },
        }


def generate_updates_finess(df, current_color):
    from ast import literal_eval
    df["liste_finess"] = df["liste_finess"].apply(literal_eval)

    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_finess": row["liste_finess"],
            },
        }


def generate_updates_rge(df, current_color):
    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "is_rge": True,
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
    from ast import literal_eval
    df["liste_uai"] = df["liste_uai"].apply(literal_eval)

    for index, row in df.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "liste_uai": row["liste_uai"],
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
        print(r.json())
