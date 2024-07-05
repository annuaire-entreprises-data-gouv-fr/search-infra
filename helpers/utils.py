import filecmp
from ast import literal_eval
import logging
import requests
import os
from datetime import datetime, date
from unicodedata import normalize
from dag_datalake_sirene.config import AIRFLOW_ENV


def check_if_prod():
    return AIRFLOW_ENV == "dev"


def is_valid_number(value):
    if value is None:
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


def str_to_list(string):
    if string is None:
        return None
    try:
        li = literal_eval(string)
        return li
    except ValueError:
        logging.info(f"////////////////Could not evaluate: {string}")


def str_to_bool(string):
    if string is None:
        return None
    if string == "true":
        return True
    elif string == "false":
        return False


def sqlite_str_to_bool(string):
    """Return True only if sqlite value is one, else (None or False) return None
    SQlite tables (egapro, entrepreneur spectacle, qualiopi) only include siren
    numbers which have those labels otherwise it's empty hence this function to make
    sure it gets labeled False"""
    if string == 1:
        return True
    return False


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
    data = {"text": f"{text} ({AIRFLOW_ENV})"}
    if AIRFLOW_ENV == "prod" or AIRFLOW_ENV == "staging":
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


def check_if_monday():
    return date.today().weekday() == 0


def get_last_line(file_path):
    """
    Retrieve the last line from a given file.

    Parameters:
    - file_path (str): The path to the file.

    Returns:
    - str or None: The last line if found, or None if the file is empty.
    """
    try:
        with open(file_path, "rb") as f:
            try:  # catch OSError in case of a one line file
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b"\n":
                    f.seek(-2, os.SEEK_CUR)
            except OSError as error:
                logging.error(f"{error}")
                f.seek(0)
            last_line = f.readline().decode()
            logging.info(f"Last line: {last_line}")

        return last_line if last_line else None
    except Exception as e:
        logging.error(f"Error while reading last line: {e}")
        return None


def convert_date_format(original_date_string):
    try:
        if original_date_string is None:
            return None
        # Convert to datetime object
        original_datetime = datetime.strptime(
            original_date_string, "%Y-%m-%d %H:%M:%S%z"
        )

        # Format
        converted_date_string = original_datetime.strftime("%Y-%m-%dT%H:%M:%S")

        return converted_date_string
    except Exception as e:
        # Handle invalid date string
        logging.error(f"Error: {e}")
        return None


def get_current_year():
    return datetime.now().year


def get_fiscal_year(date):
    # Get the fiscal year based on the month of the date
    return date.year if date.month >= 7 else date.year - 1


def remove_spaces(string):
    if string is None:
        return None
    return string.replace(" ", "")
