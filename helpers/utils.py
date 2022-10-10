import logging
from datetime import datetime
import pandas as pd
from unicodedata import normalize


def unique_list(lst):
    ulist = []
    [ulist.append(x) for x in lst if x not in ulist]
    return ulist


def unique_string(a):
    return " ".join(unique_list(a.strip().split(","))).strip()


def get_empty_string_if_none(string):
    if string is None:
        return ""
    return string


def dict_from_row(row):
    return dict(zip(row.keys(), row))


def normalize_string(string):
    if string is None:
        return None
    norm_string = (
        normalize("NFD", string.lower().strip())
        .encode("ascii", errors="ignore")
        .decode()
    )
    return norm_string


def normalize_date(date_string):
    if date_string is None or date_string == "":
        return

    date_patterns = ["%d-%m-%Y", "%Y-%m-%d", "%Y%m%d", "%d/%m/%Y"]
    for pattern in date_patterns:
        try:
            return datetime.strptime(date_string, pattern).strftime("%Y-%m-%d")
        except ValueError:
            pass

    logging.info(f"Date is not in expected format: {date_string}")


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
            colname: "code_colter",
            "Nom de l'élu": "nom_elu",
            "Prénom de l'élu": "prenom_elu",
            "Code sexe": "sexe_elu",
            "Date de naissance": "date_naissance_elu",
            "Libellé de la fonction" : "fonction_elu",
        }
    )
    return df


def drop_exact_duplicates(list_dict):
    # frozenset is used to assign a value to key in dictionary as a set. The repeated
    # entries of dictionary are hence ignored
    return list(
        {frozenset(dictionary.items()): dictionary for dictionary in list_dict}.values()
    )
