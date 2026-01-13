import os
import tempfile
import zipfile

import numpy as np
import pandas as pd

from data_pipelines_annuaire.helpers.data_quality import (
    _clean_sirent_series,
    clean_sirent_column,
    validate_file,
)


def test_validate_file_csv():
    """Test the validate_file() function with CSV files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test valid CSV with header and data
        valid_csv_path = os.path.join(tmpdir, "valid.csv")
        with open(valid_csv_path, "w", encoding="utf-8") as f:
            f.write("header1,header2\n")
            f.write("data1,data2\n")
            f.write("data3,data4\n")

        # Should not raise an exception
        validate_file(valid_csv_path)

        # Test CSV with only header (should fail)
        header_only_csv_path = os.path.join(tmpdir, "header_only.csv")
        with open(header_only_csv_path, "w", encoding="utf-8") as f:
            f.write("header1,header2\n")

        try:
            validate_file(header_only_csv_path)
            assert False, "Expected ValueError for CSV with only header"
        except ValueError as e:
            assert "has only 1 line(s)" in str(e)
            assert "Expected at least 2 lines" in str(e)

        # Test empty CSV (should fail)
        empty_csv_path = os.path.join(tmpdir, "empty.csv")
        with open(empty_csv_path, "w", encoding="utf-8") as f:
            pass  # Create empty file

        try:
            validate_file(empty_csv_path)
            assert False, "Expected ValueError for empty CSV"
        except ValueError as e:
            assert "has only 0 line(s)" in str(e)
            assert "Expected at least 2 lines" in str(e)

        # Test non-existent file (should fail)
        non_existent_path = os.path.join(tmpdir, "non_existent.csv")
        try:
            validate_file(non_existent_path)
            assert False, "Expected ValueError for non-existent file"
        except ValueError as e:
            assert "does not exist" in str(e)


def test_validate_file_xlsx():
    """Test the validate_file() function with XLSX files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test valid XLSX with data
        valid_xlsx_path = os.path.join(tmpdir, "valid.xlsx")
        df = pd.DataFrame({"col1": ["data1", "data2"], "col2": ["data3", "data4"]})
        df.to_excel(valid_xlsx_path, index=False)

        # Should not raise an exception
        validate_file(valid_xlsx_path)

        # Test XLSX with only header (should fail)
        header_only_xlsx_path = os.path.join(tmpdir, "header_only.xlsx")
        df_empty = pd.DataFrame({"col1": [], "col2": []})
        df_empty.to_excel(header_only_xlsx_path, index=False)

        try:
            validate_file(header_only_xlsx_path)
            assert False, "Expected ValueError for XLSX with only header"
        except ValueError as e:
            assert "has only 0 row(s)" in str(e)
            assert "Expected at least 2 rows" in str(e)


def test_validate_file_zip():
    """Test the validate_file() function with ZIP files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test valid ZIP with files
        valid_zip_path = os.path.join(tmpdir, "valid.zip")
        with zipfile.ZipFile(valid_zip_path, "w") as zipf:
            zipf.writestr("file1.txt", "content1")
            zipf.writestr("file2.txt", "content2")

        # Should not raise an exception
        validate_file(valid_zip_path)

        # Test empty ZIP (should fail)
        empty_zip_path = os.path.join(tmpdir, "empty.zip")
        with zipfile.ZipFile(empty_zip_path, "w") as zipf:
            pass  # Create empty zip

        try:
            validate_file(empty_zip_path)
            assert False, "Expected ValueError for empty ZIP"
        except ValueError as e:
            assert "ZIP file" in str(e)
            assert "is empty" in str(e)

        # Test ZIP with valid CSV file
        valid_csv_zip_path = os.path.join(tmpdir, "valid_csv.zip")
        with zipfile.ZipFile(valid_csv_zip_path, "w") as zipf:
            csv_content = "header1,header2\ndata1,data2\ndata3,data4\n"
            zipf.writestr("data.csv", csv_content)

        # Should not raise an exception
        validate_file(valid_csv_zip_path)

        # Test ZIP with empty CSV file (should fail)
        empty_csv_zip_path = os.path.join(tmpdir, "empty_csv.zip")
        with zipfile.ZipFile(empty_csv_zip_path, "w") as zipf:
            csv_content = "header1,header2\n"  # Only header
            zipf.writestr("data.csv", csv_content)

        try:
            validate_file(empty_csv_zip_path)
            assert False, "Expected ValueError for ZIP with empty CSV"
        except ValueError as e:
            assert "ZIP file" in str(e)
            assert "contains invalid file" in str(e)
            assert "data.csv" in str(e)

        # Test ZIP with valid XLSX file
        valid_xlsx_zip_path = os.path.join(tmpdir, "valid_xlsx.zip")
        with zipfile.ZipFile(valid_xlsx_zip_path, "w") as zipf:
            # Create a simple XLSX file in memory
            df = pd.DataFrame({"col1": ["data1", "data2"], "col2": ["data3", "data4"]})
            xlsx_path = os.path.join(tmpdir, "temp.xlsx")
            df.to_excel(xlsx_path, index=False)
            with open(xlsx_path, "rb") as f:
                zipf.writestr("data.xlsx", f.read())
            os.remove(xlsx_path)

        # Should not raise an exception
        validate_file(valid_xlsx_zip_path)


def test_validate_file_unsupported_type():
    """Test the validate_file() function with unsupported file types."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test unsupported file type (should not raise exception, just return)
        unsupported_path = os.path.join(tmpdir, "file.json")
        with open(unsupported_path, "w", encoding="utf-8") as f:
            f.write('{"key": "value"}')

        # Should not raise an exception for unsupported types
        validate_file(unsupported_path)


def test_validate_file_csv_encoding():
    """Test the validate_file() function with different CSV encodings."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Test valid CSV with UTF-8 encoding (default)
        utf8_csv_path = os.path.join(tmpdir, "utf8.csv")
        with open(utf8_csv_path, "w", encoding="utf-8") as f:
            f.write("header1,header2\n")
            f.write("data1,data2\n")
            f.write("data3,data4\n")

        # Should not raise an exception with default encoding
        validate_file(utf8_csv_path)
        # Should not raise an exception with explicit UTF-8 encoding
        validate_file(utf8_csv_path, csv_encoding="utf-8")

        # Test CSV with ISO-8859-1 encoding containing non-ASCII characters
        iso_csv_path = os.path.join(tmpdir, "iso.csv")
        with open(iso_csv_path, "w", encoding="iso-8859-1") as f:
            f.write("header1,header2\n")
            f.write("café,naïve\n")  # French characters that work in ISO-8859-1
            f.write("résumé,cliché\n")

        # Should work with correct encoding
        validate_file(iso_csv_path, csv_encoding="iso-8859-1")

        # Test CSV with wrong encoding (should fail with non-ASCII content)
        try:
            validate_file(iso_csv_path, csv_encoding="utf-8")
            assert False, "Expected UnicodeDecodeError for wrong encoding"
        except UnicodeDecodeError:
            # This is expected when using wrong encoding
            pass
        except ValueError as e:
            # The function might catch the UnicodeDecodeError and re-raise as ValueError
            assert "CSV file" in str(e)


def test_clean_sirent_series():
    """Test the _clean_sirent_series() function with the "siren" type."""
    test_data = {
        "valid_siren": ["123456789", "987654321", "123890456", None, ""],
        "invalid_siren": [
            "12345",
            "1234567890",
            "abc123456",
            "  123 456 789  ",
            "siren",
        ],
        "invalid_siren_with_zeros": [
            "12345678",
            "1234567",
            "123456789",
            "1234567890",
            None,
        ],
        "nan_siren": [None, "123456789", "987654321", "", np.nan],
    }
    siren_length = 9

    df = pd.DataFrame(test_data)

    # Test valid Siren
    result = _clean_sirent_series(df.valid_siren, length=siren_length)
    expected = pd.Series(
        ["123456789", "987654321", "123890456"], dtype="object", name="valid_siren"
    )
    pd.testing.assert_series_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )

    # Test invalid Siren
    result_invalid = _clean_sirent_series(df.invalid_siren, length=siren_length)
    expected_invalid = pd.Series(["123456789"], dtype="object", name="invalid_siren")
    pd.testing.assert_series_equal(
        result_invalid.reset_index(drop=True), expected_invalid.reset_index(drop=True)
    )

    # Test invalid Siren with leading zeros
    result_zeros = _clean_sirent_series(
        df.invalid_siren_with_zeros, length=siren_length, add_leading_zeros=True
    )
    expected_zeros = pd.Series(
        ["012345678", "001234567", "123456789"],
        dtype="string",
        name="invalid_siren_with_zeros",
    )
    pd.testing.assert_series_equal(
        result_zeros.reset_index(drop=True), expected_zeros.reset_index(drop=True)
    )

    # Test Siren with NaN values
    result_nan = _clean_sirent_series(df.nan_siren, length=siren_length)
    expected_nan = pd.Series(
        ["123456789", "987654321"], dtype="object", name="nan_siren"
    )
    pd.testing.assert_series_equal(
        result_nan.reset_index(drop=True), expected_nan.reset_index(drop=True)
    )


def test_clean_sirent_column():
    """Test the clean_sirent_column() function with various scenarios."""
    # Test data with mixed valid and invalid values
    test_data = {
        "siren": [
            "123456789",  # valid
            "987654321",  # valid
            "12345",  # too short
            "1234567890",  # too long
            "abc123456",  # contains letters
            "  123 456 789  ",  # contains spaces
            "siren",  # text
            None,  # None value
            "",  # empty string
            "123456789",  # valid duplicate
        ],
        "other_data": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    }

    df = pd.DataFrame(test_data)

    # Test with siren type
    result = clean_sirent_column(
        df=df,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=False,
        max_removal_percentage=70,  # Need higher threshold since we're removing 60% of data
    )

    # Should keep only valid siren rows
    expected_siren_values = ["123456789", "987654321", "123456789", "123456789"]
    expected_other_data = ["a", "b", "f", "j"]

    assert len(result) == 4, f"Expected 4 rows, got {len(result)}"
    assert result["siren"].tolist() == expected_siren_values, (
        f"Expected {expected_siren_values}, got {result['siren'].tolist()}"
    )
    assert result["other_data"].tolist() == expected_other_data, (
        f"Expected {expected_other_data}, got {result['other_data'].tolist()}"
    )

    # Test with siret type
    siret_test_data = {
        "siret": [
            "12345678901234",  # valid
            "98765432109876",  # valid
            "123456789",  # too short
            "123456789012345",  # too long
            "abc12345678901",  # contains letters
            None,  # None value
        ],
        "company_name": [
            "Company A",
            "Company B",
            "Company C",
            "Company D",
            "Company E",
            "Company F",
        ],
    }

    siret_df = pd.DataFrame(siret_test_data)

    result_siret = clean_sirent_column(
        df=siret_df,
        column_type="siret",
        column_name="siret",
        add_leading_zeros=False,
        max_removal_percentage=70,  # Need higher threshold since we're removing 66.67% of data
    )

    # Should keep only valid siret rows
    expected_siret_values = ["12345678901234", "98765432109876"]
    expected_company_names = ["Company A", "Company B"]

    assert len(result_siret) == 2, f"Expected 2 rows, got {len(result_siret)}"
    assert result_siret["siret"].tolist() == expected_siret_values, (
        f"Expected {expected_siret_values}, got {result_siret['siret'].tolist()}"
    )
    assert result_siret["company_name"].tolist() == expected_company_names, (
        f"Expected {expected_company_names}, got {result_siret['company_name'].tolist()}"
    )


def test_clean_sirent_column_with_leading_zeros():
    """Test the clean_sirent_column() function with add_leading_zeros=True."""
    test_data = {
        "siren": [
            "12345678",  # needs leading zero
            "1234567",  # needs leading zeros
            "123456789",  # already valid
            "1234567890",  # too long
        ],
        "data": ["a", "b", "c", "d"],
    }

    df = pd.DataFrame(test_data)

    result = clean_sirent_column(
        df=df,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=True,
        max_removal_percentage=30,  # Need higher threshold since we're removing 25% of data
    )

    # Should add leading zeros to valid but short siren numbers
    expected_siren_values = ["012345678", "001234567", "123456789"]
    expected_data = ["a", "b", "c"]

    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert result["siren"].tolist() == expected_siren_values, (
        f"Expected {expected_siren_values}, got {result['siren'].tolist()}"
    )
    assert result["data"].tolist() == expected_data, (
        f"Expected {expected_data}, got {result['data'].tolist()}"
    )

    # Check that the siren column has string dtype when add_leading_zeros=True
    assert result["siren"].dtype == "string", (
        f"Expected string dtype, got {result['siren'].dtype}"
    )


def test_clean_sirent_column_max_removal_percentage():
    """Test the clean_sirent_column() function with max_removal_percentage parameter."""
    test_data = {
        "siren": [
            "123456789",  # valid
            "invalid1",  # invalid
            "invalid2",  # invalid
            "invalid3",  # invalid
            "invalid4",  # invalid
        ],
        "data": ["a", "b", "c", "d", "e"],
    }

    df = pd.DataFrame(test_data)

    # Test with a threshold that should pass (50% removal allowed)
    result = clean_sirent_column(
        df=df,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=False,
        max_removal_percentage=80.0,
    )

    # Should succeed and return 1 valid row
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result["siren"].tolist() == ["123456789"], (
        f"Expected ['123456789'], got {result['siren'].tolist()}"
    )

    # Test with a threshold that should fail (only 20% removal allowed)
    try:
        clean_sirent_column(
            df=df,
            column_type="siren",
            column_name="siren",
            add_leading_zeros=False,
            max_removal_percentage=20.0,
        )
        assert False, "Expected ValueError to be raised"
    except Exception as e:
        assert "exceeds the maximum allowed threshold" in str(e), (
            f"Expected threshold error message, got: {e}"
        )

    # Test with default max_removal_percentage=0 (should fail if any data is removed)
    try:
        clean_sirent_column(
            df=df,
            column_type="siren",
            column_name="siren",
            add_leading_zeros=False,
            # max_removal_percentage defaults to 0
        )
        assert False, (
            "Expected ValueError to be raised with default max_removal_percentage=0"
        )
    except Exception as e:
        assert "exceeds the maximum allowed threshold" in str(e), (
            f"Expected threshold error message, got: {e}"
        )


def test_clean_sirent_column_empty_dataframe():
    """Test the clean_sirent_column() function with an empty DataFrame."""
    df = pd.DataFrame({"siren": [], "data": []})

    result = clean_sirent_column(
        df=df,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=False,
        max_removal_percentage=100,  # Need high threshold since we're removing 100% of data
    )

    # Should return empty DataFrame
    assert len(result) == 0, f"Expected 0 rows, got {len(result)}"
    assert list(result.columns) == ["siren", "data"], (
        f"Expected ['siren', 'data'] columns, got {list(result.columns)}"
    )


def test_clean_sirent_column_all_invalid():
    """Test the clean_sirent_column() function when all values are invalid."""
    test_data = {
        "siren": [
            "12345",  # too short
            "abc123",  # contains letters
            None,  # None value
            "",  # empty string
        ],
        "data": ["a", "b", "c", "d"],
    }

    df = pd.DataFrame(test_data)

    result = clean_sirent_column(
        df=df,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=False,
        max_removal_percentage=100,
    )

    # Should return empty DataFrame
    assert len(result) == 0, f"Expected 0 rows, got {len(result)}"
    assert list(result.columns) == ["siren", "data"], (
        f"Expected ['siren', 'data'] columns, got {list(result.columns)}"
    )


def test_clean_sirent_column_missing_column():
    """Test the clean_sirent_column() function when the specified column doesn't exist."""
    df = pd.DataFrame({"wrong_column": ["123456789"], "data": ["a"]})

    try:
        clean_sirent_column(
            df=df,
            column_type="siren",
            column_name="siren",
            add_leading_zeros=False,
            max_removal_percentage=0,
        )
        assert False, "Expected ValueError to be raised"
    except ValueError as e:
        assert "Column siren does not exist in the DataFrame." in str(e), (
            f"Expected column not found error, got: {e}"
        )


def test_clean_sirent_column_default_max_removal_percentage():
    """Test that clean_sirent_column() works with default max_removal_percentage=0 when no data is removed."""
    # Test data with all valid values - should work with default max_removal_percentage=0
    test_data = {
        "siren": [
            "123456789",  # valid
            "987654321",  # valid
            "111222333",  # valid
        ],
        "data": ["a", "b", "c"],
    }

    df = pd.DataFrame(test_data)

    # This should work fine because no data is removed
    result = clean_sirent_column(
        df=df,
        column_type="siren",
        column_name="siren",
        add_leading_zeros=False,
        # max_removal_percentage defaults to 0, but since no data is removed, it should pass
    )

    # Should return all rows since all are valid
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert result["siren"].tolist() == ["123456789", "987654321", "111222333"], (
        f"Expected all valid siren values, got {result['siren'].tolist()}"
    )


def test_clean_siret_series():
    """Test the _clean_sirent_series() function with the "siret" type."""
    test_data = {
        "valid_siret": ["12345678901234", "98765432109876", "12345678901234", None, ""],
        "invalid_siret": [
            "123456789",
            "123456789012345",
            "abc12345678901",
            "  123 456 789 012 34  ",
            "1.12345876432e12",
        ],
        "invalid_siret_with_zeros": [
            "1234567890123",
            "123456789012",
            "12345678901234",
            None,
            "siret",
        ],
        "nan_siret": [
            None,
            "12345678901234",
            None,
            "98765432109876",
            np.nan,
        ],
        "scientific_notation": [
            "1,60572634981e+12",
            "1.60572634981E-12",
            "1,36980972634E-22",
            "13698097263422",
            "1.3698097263443E",
        ],
    }
    siret_length = 14

    df = pd.DataFrame(test_data)

    # Test valid Siret
    result = _clean_sirent_series(df.valid_siret, length=siret_length)
    expected = pd.Series(
        ["12345678901234", "98765432109876", "12345678901234"],
        dtype="object",
        name="valid_siret",
    )
    pd.testing.assert_series_equal(
        result.reset_index(drop=True), expected.reset_index(drop=True)
    )

    # Test invalid Siret
    result_invalid = _clean_sirent_series(df.invalid_siret, length=siret_length)
    expected_invalid = pd.Series(
        ["12345678901234"], dtype="object", name="invalid_siret"
    )  # Only the valid one after cleaning
    pd.testing.assert_series_equal(
        result_invalid.reset_index(drop=True), expected_invalid.reset_index(drop=True)
    )

    # Test leading zeros
    result_zeros = _clean_sirent_series(
        df.invalid_siret_with_zeros, length=siret_length, add_leading_zeros=True
    )
    expected_zeros = pd.Series(
        ["01234567890123", "00123456789012", "12345678901234"],
        dtype="string",
        name="invalid_siret_with_zeros",
    )
    pd.testing.assert_series_equal(
        result_zeros.reset_index(drop=True), expected_zeros.reset_index(drop=True)
    )

    # Test NaN values
    result_nan = _clean_sirent_series(df.nan_siret, length=siret_length)
    expected_nan = pd.Series(
        ["12345678901234", "98765432109876"], dtype="object", name="nan_siret"
    )
    pd.testing.assert_series_equal(
        result_nan.reset_index(drop=True), expected_nan.reset_index(drop=True)
    )

    # Test scientific notation
    result_nan = _clean_sirent_series(df.scientific_notation, length=siret_length)
    print(result_nan)
    expected_nan = pd.Series(
        ["13698097263422"], dtype="object", name="scientific_notation"
    )
    pd.testing.assert_series_equal(
        result_nan.reset_index(drop=True), expected_nan.reset_index(drop=True)
    )
