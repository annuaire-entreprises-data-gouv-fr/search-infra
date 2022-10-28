import os
from ast import literal_eval

import pandas as pd
import requests


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
