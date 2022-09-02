from unicodedata import normalize
import logging
from datetime import datetime
import dateutil.parser as parser

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

"""
def normalize_date(date):
    if date is None:
        return None
    try:
        norm_date = datetime.strptime(date, "%d/%m/%Y").strftime('%Y-%m-%d')
    except ValueError:
        logging.info("The string is not a date with format %d/%m/%Y: " + date)
        norm_date = date
    return norm_date
"""
def normalize_date(date):
    if date is None:
        return None
    try:
        date_parsed = parser.parse(date)
        norm_date = datetime.strptime(date, "%d/%m/%Y").strftime('%Y-%m-%d')
    except ValueError:
        logging.info("The string is not a date with format %d/%m/%Y: " + date)
        norm_date = date
    return norm_date

