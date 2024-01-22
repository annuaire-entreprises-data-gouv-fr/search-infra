import os
import tempfile
import pytest
from dag_datalake_sirene.helpers.utils import get_last_line


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
