import filecmp
import gzip
import json
import logging
import os
import re
from ast import literal_eval
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal
from unicodedata import normalize
from urllib.parse import urlparse

import pandas as pd
import requests

from dag_datalake_sirene.config import AIRFLOW_ENV
from dag_datalake_sirene.helpers.datagouv import fetch_last_modified_date


def check_if_prod():
    return AIRFLOW_ENV == "prod"


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


def get_previous_months(
    lookback: int = 12,
    starting_date: date = date.today(),
    string_format: bool = True,
    period_type: Literal["year", "month", "day"] = "month",
    step: int = 1,
) -> list[str] | list[date]:
    """
    Generate a list of continuous periods (months, years, weeks, or days)
    starting from a given date.

    Args:
        lookback (int): The number of previous periods to include in the list.
        starting_date (date): The starting date from which to calculate the previous periods.
        string_format (bool): If True outputs a list of string, otherwise dates. Default to True.
        period_type (Literal["year", "month", "day"]): The type of period to calculate.
            Defaults to "month".
        step (int): Step between each period. Default is 1.

    Returns:
        list[str | date]: A descending ordered list of strings or date of the previous periods.
    """

    _formats = {
        "year": "%Y",
        "month": "%Y-%m",
        "day": "%Y-%m-%d",
    }
    _time_delta = {
        "year": ({"day": 1, "month": 1}, {"days": 1}),
        "month": ({"day": 1}, {"days": 1}),
        "day": ({"hour": 0}, {"hours": 1}),
    }

    periods = [starting_date]

    for _ in range(lookback * step):
        period = periods[-1].replace(**_time_delta[period_type][0]) - timedelta(
            **_time_delta[period_type][1]
        )
        periods.append(period)

    if string_format:
        return [
            period.strftime(_formats[period_type])
            for i, period in enumerate(periods)
            if i % step == 0
        ]

    return periods


def remove_spaces(string):
    if string is None:
        return None
    return string.replace(" ", "")


def flatten_dict(dd, separator="_", prefix=""):
    return (
        {
            prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
        }
        if isinstance(dd, dict)
        else {prefix: dd}
    )


def zip_file(file_path: str, chunk_size: int = 100000):
    zip_path = f"{file_path}.gz"
    with open(file_path, "rb") as orig_file:
        with gzip.open(zip_path, "wb") as zipped_file:
            while True:
                chunk = orig_file.read(chunk_size)
                if not chunk:
                    break
                zipped_file.write(chunk)
    return zip_path


def save_data_to_zipped_csv(df: pd.DataFrame, folder: str, filename: str):
    """Save DataFrame to CSV and log the action."""
    file_path: str = os.path.join(folder, filename)
    df.to_csv(file_path, index=False)
    zip_file(file_path)
    logging.info(f"Saved {filename} with {df.shape[0]} records.")


def flatten_object(obj, prop):
    res = ""
    for item in obj:
        res += f",{item[prop]}"
    return res[1:]


def get_date_last_modified(response=None, url=None) -> str | None:
    """
    Fetches the Last-Modified date from either a response object or a URL.
    Returns it in ISO 8601 format using `parse_date_string`.

    Args:
        response (requests.Response, optional): The HTTP response object if
        already available.
        url (str, optional): The URL of the file if no response is available.

    Returns:
        str: The Last-Modified date in '%Y-%m-%dT%H:%M:%S' format,
             or 'No date available' if not found.
    """
    try:
        if response:
            # If we already have a response object, use its headers
            last_modified_raw = response.headers.get("Last-Modified", None)
        elif url:
            # Make a HEAD request to get the headers
            head_response = requests.head(url)
            head_response.raise_for_status()
            last_modified_raw = head_response.headers.get("Last-Modified", None)

            # If not found in HEAD response, fallback to GET
            if last_modified_raw is None:
                get_response = requests.get(url)
                get_response.raise_for_status()
                last_modified_raw = get_response.headers.get("Last-Modified", None)
                logging.info(get_response.headers)
        else:
            raise ValueError("Either a response or URL must be provided")

        # Use parse_date_string to convert the Last-Modified date to ISO 8601 format
        return parse_date_string(last_modified_raw) if last_modified_raw else None
    except requests.RequestException as e:
        logging.error(f"Error fetching last modified date: {e}")
        return "Error fetching date"
    except Exception as e:
        logging.error(f"Error parsing date: {e}")
        return "Error parsing date"


def parse_date_string(
    date_string: str | None,
    input_formats: str | list[str] = [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ],
    output_format: str = "%Y-%m-%d",
    default_value: str = "No date available",
) -> str:
    """
    Parse a date string from various formats and return it in a specified format.

    Args:
        date_string (Optional[str]): The date string to parse.
        input_formats (Union[str, list[str]]): Format(s) to parse the input string.
                                               Can be a single format string or
                                               a list of format strings.
        output_format (str): The desired output format.
        default_value (str): The value to return if parsing fails.

    Returns:
        str: The parsed and formatted date string, or
        the default value if parsing fails.

    Examples:
        >>> parse_date_string("Mon, 01 Jan 2024 12:00:00 GMT")
        '2024-01-01'
        >>> parse_date_string("2024-01-01")
        '2024-01-01'
        >>> parse_date_string(None)
        'No date available'
        >>> parse_date_string("2024-01-01 15:30:00", output_format="%Y-%m-%d")
        '2024-01-01'
    """
    if not date_string:
        return default_value

    if isinstance(input_formats, str):
        input_formats = [input_formats]

    for format in input_formats:
        try:
            parsed_date = datetime.strptime(date_string, format)
            return parsed_date.strftime(output_format)
        except ValueError:
            continue

    return default_value


def extract_date_from_filename(filename):
    # Use regex to find a date pattern in the filename
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)

    if match:
        date_string = match.group(1)
        # Parse the date string into a datetime object
        return datetime.strptime(date_string, "%Y-%m-%d").date().isoformat()
    else:
        return None


def fetch_latest_file_from_folder(
    folder_path: str, extension: str | None = None
) -> Path | None:
    """
    Fetches the latest file from the specified folder.

    Parameters:
        folder_path (str): The path to the folder to search.
        extension (str, optional): File extension to filter by (e.g., '.csv').
                                   If None, all files are considered.

    Returns:
        Optional[Path]: The path of the latest file, or None if no files are found.
    """
    path = Path(folder_path)

    if not path.is_dir():
        raise ValueError(f"The path '{folder_path}' is not a valid directory.")

    # Get files matching the extension or all files if no extension is specified
    files = list(path.glob(f"*{extension}")) if extension else list(path.glob("*"))

    if not files:
        return None  # No files found

    # Return the latest file based on modification time
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    logging.info(f"********Latest file found in folder: {latest_file}")
    return latest_file


def save_to_metadata(metadata_file: str, key: str, value: str) -> None:
    """
    Saves a key-value pair in a metadata JSON file.

    Args:
        metadata_file (str): The path to the metadata JSON file.
        key (str): The key to store in the metadata.
        value (str): The value to associate with the key.

    Returns:
        None
    """
    # Ensure the metadata file exists
    if not os.path.exists(metadata_file):
        with open(metadata_file, "w") as f:
            json.dump({}, f)

    # Load existing metadata
    with open(metadata_file, "r") as f:
        metadata = json.load(f)

    # Update the metadata with the new key-value pair
    metadata[key] = value

    # Save the updated metadata back to the file
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=4)


def fetch_and_store_last_modified_metadata(resource_id: str, file_path: str) -> None:
    """
    Fetch 'last_modified' from resource and save the date to a metadata file.
    """
    try:
        # Define the metadata file path
        metadata_path = os.path.join(file_path, "metadata.json")

        # Save the 'last_modified' date to the metadata file
        save_to_metadata(
            metadata_path, "last_modified", fetch_last_modified_date(resource_id)
        )

        logging.info(f"Last modified date saved successfully to {metadata_path}")

    except Exception as e:
        raise RuntimeError(f"Failed to save last modified metadata: {e}")


def fetch_last_modified_date_from_json(url: str) -> datetime | None:
    """
    Fetch and parse the 'last_modified' date from a JSON response at the given URL.

    Args:
        url (str): The URL to fetch the JSON data from.

    Returns:
        Optional[datetime]: The parsed 'last_modified' date if present and valid,
                            or None if not found or in case of any error.

    Raises:
        ValueError: If the URL is empty or not a string.
    """
    if not isinstance(url, str) or not url.strip():
        raise ValueError("URL must be a non-empty string")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        last_modified = data.get("last_modified")
        if last_modified:
            return datetime.fromisoformat(last_modified)
        return None

    except requests.exceptions.RequestException as e:
        logging.warning(f"Error fetching data: {e}")
        return None
    except ValueError as e:
        logging.warning(f"Error parsing JSON or date: {e}")
        return None


def simplify_date(datetime_str: str) -> str:
    if datetime_str is None:
        return None
    return datetime_str.split("T")[0]


def download_file(download_url: str, destination_path: str) -> None:
    logging.info(f"Downloading file from {download_url}..")

    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    with open(destination_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024):
            file.write(chunk)

    logging.info(f"..download successful! File located in {destination_path}.")


def get_dates_since_start_of_month(
    include_today: bool = True,
    ascending: bool = True,
) -> list[str]:
    """
    Get the dates since the beginning of the month until today.

    Returns:
        list[str]: List of dates with YYYY-MM-DD string format.
    """
    last_day_of_month = date.today()
    if not include_today:
        if last_day_of_month.day == 1:
            return []
        last_day_of_month -= timedelta(days=1)

    # Get the first day of the current month
    first_day_of_month = last_day_of_month.replace(day=1)

    # Generate the list of dates
    dates: list[str] = []
    day = first_day_of_month
    while day <= last_day_of_month:
        dates.append(day.strftime("%Y-%m-%d"))
        day += timedelta(days=1)

    dates.sort(reverse=(not ascending))

    return dates


def load_file(file_name: str):
    labels_file_path = "dags/dag_datalake_sirene/helpers/labels/"

    with open(f"{labels_file_path}{file_name}") as json_file:
        file_decoded = json.load(json_file)
    return file_decoded


def fetch_hyperlink_from_page(url: str, search_text: str) -> str:
    """
    Fetches a URL from a web page that matches the given search text.

    Args:
        url (str): The URL of the web page to search.
        search_text (str): The text to search for within the web page's HTML content.
    Returns:
        str: The full URL found in the web page that matches the search text.
    Raises:
        ValueError: If no URL matching the search text is found in the HTML content.
        requests.exceptions.RequestException: If there is an issue with the provided URL.
    """

    parsed_url = urlparse(url)
    base_url = base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    response = requests.get(url)
    response.raise_for_status()
    html_content = response.text

    logging.info(f"Looking for the URL behind: {search_text}")
    match = re.search(
        r'<a\s+[^>]*href="([^"]+)"[^>]*>' + re.escape(search_text) + r"</a>",
        html_content,
    )

    if not match:
        raise ValueError(f"No URL found in the html source of: {url}")
    hyperlink = match.group(1)

    if parsed_url.netloc not in hyperlink:
        hyperlink = base_url + match.group(1)

    logging.info(f"Likely found the hyperlink: {hyperlink}")

    return hyperlink


def clean_siren_column(siren: pd.Series) -> pd.Series:
    """
    Clean the SIREN column by removing unwanted characters and adding leading zeros.
    """
    # Remove NaN
    siren = siren.loc[~siren.isna()]
    # Check for non-digit characters
    non_digit_rows = siren.loc[~siren.str.isdigit()]
    if not non_digit_rows.empty:
        logging.warning(
            f"SIREN column contains non-digit characters. Sample:\n{non_digit_rows.head().tolist()}\nRemoving non numeric characters.."
        )

    clean_siren = siren.astype(str).str.replace(r"[^0-9]", "", regex=True)
    # Add leading zeros only to digits with at least 6 characters
    # since no Siren have more than 3 leading zeros
    clean_siren = clean_siren.apply(
        lambda x: x.zfill(9) if pd.notna(x) and len(x) >= 6 else x
    ).astype("string")

    # Check for rows that are not 9 characters long
    incorrect_length_rows = clean_siren.loc[clean_siren.str.len() != 9]
    if not incorrect_length_rows.empty:
        logging.warning(
            f"SIREN column should be 9 values long. Sample:\n{incorrect_length_rows.head().tolist()}"
        )

    return clean_siren


def is_url_valid(url: str) -> bool:
    """
    Check if a URL is valid and working.

    Args:
        url (str): The URL to check.

    Returns:
        bool: True if the URL is ok, False otherwise.
    """
    try:
        response = requests.head(url)
        return response.ok
    except requests.RequestException as e:
        logging.warning(f"Error checking URL status: {e}")
        return False
