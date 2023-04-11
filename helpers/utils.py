import filecmp
import logging
from ast import literal_eval
from datetime import datetime
from unicodedata import normalize

import requests
from airflow.models import Variable

ENV = Variable.get("ENV")


def str_to_list(string):
    if string is None:
        return None
    li = literal_eval(string)
    return li


def str_to_bool(string):
    if string is None:
        return None
    if string == "true":
        return True
    elif string == "false":
        return False


def sqlite_str_to_bool(string):
    if string is None:
        return None
    if string == 1:
        return True


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

    logging.debug(f"Date is not in expected format: {date_string}")


def drop_exact_duplicates(list_dict):
    # frozenset is used to assign a value to key in dictionary as a set. The repeated
    # entries of dictionary are hence ignored
    return list(
        {frozenset(dictionary.items()): dictionary for dictionary in list_dict}.values()
    )


def publish_mattermost(
    text,
) -> None:
    data = {"text": f"{text} ({ENV})"}
    if ENV == "prod" or ENV == "staging":
        r = requests.post(
            "https://mattermost.incubateur.net/hooks/z4k8a159yjnx584idit1ubf74r",
            json=data,
        )
        logging.info(f"Status code: {r.status_code}")


def compare_versions_file(
    original_file: str,
    new_file: str,
):
    should_continue = not filecmp.cmp(original_file, new_file)
    return should_continue
