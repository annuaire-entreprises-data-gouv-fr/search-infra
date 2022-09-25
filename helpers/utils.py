from datetime import datetime
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
    date_patterns = ["%d-%m-%Y", "%Y-%m-%d", "%Y%m%d", "%d/%m/%Y"]
    for pattern in date_patterns:
        try:
            return datetime.strptime(date_string, pattern).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # logging.info(f"Date is not in expected format: {date_string}")


def drop_exact_duplicates(list_dict):
    # frozenset is used to assign a value to key in dictionary as a set. The repeated
    # entries of dictionary are hence ignored
    return list(
        {frozenset(dictionary.items()): dictionary for dictionary in list_dict}.values()
    )
