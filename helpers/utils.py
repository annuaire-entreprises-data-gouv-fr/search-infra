from unicodedata import normalize
import pandas as pd

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
    norm_string = (
        normalize("NFD", string.lower().strip())
        .encode("ascii", errors="ignore")
        .decode()
    )
    return norm_string

def process_elus_files(url, colname):
    df = pd.read_csv(url, dtype=str, sep="\t")
    df = df[[colname, 'Nom de l\'élu', 'Prénom de l\'élu', 'Code sexe', 'Date de naissance']]
    df = df.rename(columns={
        colname: 'code_colter',
        'Nom de l\'élu': 'nom_elu',
        'Prénom de l\'élu': 'prenom_elu',
        'Code sexe': 'sexe_elu',
        'Date de naissance': 'date_naissance_elu'
    })
    return df