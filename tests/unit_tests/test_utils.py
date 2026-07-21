import logging
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from data_pipelines_annuaire.helpers.utils import fetch_hyperlink_from_page


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


@pytest.fixture
def temp_file_path():
    # Create a temporary file for testing
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file_path = temp_file.name
    temp_file.close()
    yield temp_file_path
    # Remove the temporary file after the test
    os.remove(temp_file_path)


def write_lines_to_file(file_path, lines):
    # Helper function to write lines to a file
    with open(file_path, "w") as file:
        file.writelines(lines)


def test_empty_file(temp_file_path):
    # Test case for an empty file
    result = get_last_line(temp_file_path)
    assert result is None


def test_one_line_file(temp_file_path):
    # Test case for a file with one line
    write_lines_to_file(temp_file_path, ["This is the only line"])
    result = get_last_line(temp_file_path)
    assert result == "This is the only line"


def test_multi_line_file(temp_file_path):
    # Test case for a file with multiple lines
    lines = ["First line\n", "Second line\n", "Third line\n"]
    write_lines_to_file(temp_file_path, lines)
    result = get_last_line(temp_file_path)
    assert result == "Third line\n"


def test_one_character_line_file(temp_file_path):
    # Test case for a file with one-character lines
    lines = ["A\n", "B\n", "C\n"]
    write_lines_to_file(temp_file_path, lines)
    result = get_last_line(temp_file_path)
    assert result == "C\n"


def test_error_reading_last_line(temp_file_path):
    # Test case for an error while reading the last line
    write_lines_to_file(temp_file_path, ["Single line"])
    # Changing file permissions to simulate a read error
    os.chmod(temp_file_path, 0o000)
    result = get_last_line(temp_file_path)
    assert result is None


PAGE_HTML = """
<a href="/documents/report.pdf" class="link">Annual report</a>
<a href="/files/2026-06/Monthly_Data_Export_June2026.xlsx"
   type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
   class="link link--download" download>     Monthly data
   export <span class="link__detail">(XLSX - 188.18 Ko)</span>
</a>
"""


@pytest.fixture
def mock_page_response():
    with patch("data_pipelines_annuaire.helpers.utils.requests.get") as mock_get:
        mock_get.return_value = MagicMock(text=PAGE_HTML)
        yield mock_get


def test_fetch_hyperlink_matching_text(mock_page_response):
    result = fetch_hyperlink_from_page("https://example.com/some-page", "Annual report")
    assert result == "https://example.com/documents/report.pdf"


def test_fetch_hyperlink_matching_href(mock_page_response):
    # The anchor content is spread over several lines and contains a nested
    # span, so only an href match can find it
    result = fetch_hyperlink_from_page(
        "https://example.com/some-page",
        "Monthly_Data_Export_",
        match_on="href",
    )
    assert (
        result == "https://example.com/files/2026-06/Monthly_Data_Export_June2026.xlsx"
    )


def test_fetch_hyperlink_not_found(mock_page_response):
    with pytest.raises(ValueError):
        fetch_hyperlink_from_page(
            "https://example.com/some-page",
            "not_in_the_page",
            match_on="href",
        )
