import logging
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from data_pipelines_annuaire.helpers.utils import (
    fetch_hyperlink_from_page,
    sanitize_free_text_for_tabular_format,
)

logger = logging.getLogger(__name__)


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
                logger.error(f"{error}")
                f.seek(0)
            last_line = f.readline().decode()
            logger.info(f"Last line: {last_line}")

        return last_line if last_line else None
    except Exception as e:
        logger.error(f"Error while reading last line: {e}")
        return None


@pytest.fixture
def temp_file_path():
    # Create a temporary file for testing
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

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


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, ""),
        (
            "  Harcèlement\r\n\tsuite à\u00a0un  article ",
            "  Harcèlement\r\n\tsuite à\u00a0un  article ",
        ),
        ("abc\u202edef\u200b\x00\x1bghi", "abcdefghi"),
        ('Il a dit "stop"', "Il a dit stop"),
        ("=1+1", "'=1+1"),
        ("+33 6", "'+33 6"),
        ("-2", "'-2"),
        ("@SUM(A1)", "'@SUM(A1)"),
        ("\t=1+1", "'\t=1+1"),
        (" \n=1+1", "' \n=1+1"),
        ("'=1+1", "'=1+1"),
        ("<script>alert(1)</script>", "<script>alert(1)</script>"),
        ("e\u0301", "é"),
    ],
)
def test_sanitize_free_text(value, expected):
    assert sanitize_free_text_for_tabular_format(value) == (expected, False)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("a" * 5, ("aaaaa", False)),
        ("a" * 6, ("aaaaa", True)),
        ("=" * 5, ("'====", True)),
        ("=abc ", ("'=abc", True)),
    ],
)
def test_sanitize_free_text_length(value, expected):
    assert sanitize_free_text_for_tabular_format(value, max_length=5) == expected


@pytest.mark.parametrize(
    "value",
    ['  =a "b"\r\n c ', "=" * 10, "=abc  def", "\t-\u202e1"],
)
def test_sanitize_free_text_idempotent(value):
    once, _ = sanitize_free_text_for_tabular_format(value, max_length=5)

    assert sanitize_free_text_for_tabular_format(once, max_length=5) == (once, False)
