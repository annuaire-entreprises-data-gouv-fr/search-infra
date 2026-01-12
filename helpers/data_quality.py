import logging
import os
import tempfile
import zipfile
from typing import Literal

import pandas as pd


def validate_file(file_path: str, min_rows: int = 2) -> None:
    """
    Validate that a file is not empty based on its type.

    Args:
        file_path (str): Path to the file to validate
        min_rows (int): Minimum number of rows expected (for CSV/XLSX). Defaults to 2.

    Raises:
        ValueError: If the file is empty or invalid

    Supported file types:
        - CSV: Must have at least min_rows lines
        - XLSX: Must have at least min_rows rows in the first sheet
        - ZIP: Must contain at least one file, and any CSV/XLSX files within must be valid
    """
    if not os.path.exists(file_path):
        raise ValueError(f"File {file_path} does not exist")

    if file_path.endswith(".csv"):
        with open(file_path, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f)
        if line_count < min_rows:
            raise ValueError(
                f"CSV file {file_path} has only {line_count} line(s). "
                f"Expected at least {min_rows} lines"
            )

    elif file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
        if len(df) < min_rows:
            raise ValueError(
                f"XLSX file {file_path} has only {len(df)} row(s). "
                f"Expected at least {min_rows} rows"
            )

    elif file_path.endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            file_list = zip_ref.namelist()
            if len(file_list) == 0:
                raise ValueError(f"ZIP file {file_path} is empty")

            with tempfile.TemporaryDirectory() as temp_dir:
                for file_name in file_list:
                    if file_name.endswith((".csv", ".xlsx")):
                        extract_path = os.path.join(
                            temp_dir, os.path.basename(file_name)
                        )
                        with open(extract_path, "wb") as f:
                            f.write(zip_ref.read(file_name))

                        try:
                            validate_file(extract_path, min_rows)
                        except ValueError as e:
                            raise ValueError(
                                f"ZIP file {file_path} contains invalid file {file_name}: {str(e)}"
                            ) from e

    else:
        logging.warning(f"Unsupported file type for {file_path}")


def _clean_sirent_series(
    column: pd.Series,
    length: int,
    add_leading_zeros: bool = False,
) -> pd.Series:
    """
    Clean a Siret or Siret column by removing non-numeric characters and adding leading zeros.

    Args:
        column (pd.Series): The column to clean.
        length (int): expected number length.
        add_leading_zeros (bool, optional): Whether to add leading zeros to valid Siren numbers. Default to False.

    Returns:
        pd.Series: only the valid rows
    """

    # Remove NaN
    raw_column = column.loc[~column.isna()]

    # Remove any row looking like a scientific notation
    scientific_notation_pattern = r"^[+-]?\d{1,3}(?:[.,]\d+)?[Ee][+-]?\d*$"
    clean_column = raw_column.loc[
        ~raw_column.str.match(scientific_notation_pattern, na=False)
    ]

    # Remove non numeric characters
    clean_column = clean_column.astype(str).str.replace(r"[^0-9]", "", regex=True)

    # Add leading zeros if required
    if add_leading_zeros:
        clean_column = clean_column.apply(
            lambda x: x.zfill(length)
            # No Siren has more than 3 leading zeros
            if pd.notna(x) and len(x) >= length - 3
            else x
        ).astype("string")

    # Keep only rows that are within the required length
    clean_column = clean_column.loc[clean_column.str.len() == length]

    return clean_column


def clean_sirent_column(
    df: pd.DataFrame,
    column_type: Literal["siret", "siren"],
    column_name: str | None = None,
    add_leading_zeros: bool = False,
    max_removal_percentage: float = 0.0,
) -> pd.DataFrame:
    """
    Clean the Siren and Siret column in a DataFrame and remove invalid rows.

    Args:
        df (pd.DataFrame): The DataFrame to process
        column_type (str): "siret" or "siren" value.
        column_name (str, optional): The "siret" or "siren" column name. Defaults to column_type value.
        add_leading_zeros (bool, optional): Whether to add leading zeros to valid Siren numbers. Default to False.
        max_removal_percentage (float | None, optional): Maximum percentage of data that can be removed during cleaning.
                                                        If exceeded, raises ValueError. Set to None to disable check.

    Returns:
        pd.DataFrame: DataFrame with only rows containing valid Siren/Siret values

    Raises:
        ValueError: If more than max_removal_percentage of data is removed from any column
    """

    if not column_name:
        column_name = column_type

    if column_name not in df.columns:
        raise ValueError(f"Column {column_name} does not exist in the DataFrame.")

    # Handle empty DataFrame case
    if len(df) == 0:
        return df

    original_row_count = len(df)
    original_siren_values = df[column_name].copy()

    if column_type == "siren":
        cleaned = _clean_sirent_series(
            df[column_name],
            length=9,
            add_leading_zeros=add_leading_zeros,
        )
    elif column_type == "siret":
        cleaned = _clean_sirent_series(
            df[column_name],
            length=14,
            add_leading_zeros=add_leading_zeros,
        )

    # Use the cleaned's index to filter the DataFrame directly
    # This ensures proper index alignment and handles duplicate indices correctly
    cleaned_df = df.loc[cleaned.index].copy()
    # Replace the raw values in the DataFrame with the cleaned values
    cleaned_df[column_name] = cleaned.values

    # Find and print distinct removed values
    removed_indices = original_siren_values.index.difference(cleaned.index)
    if not removed_indices.empty:
        dirty_values = df.loc[removed_indices]
        if len(dirty_values) > 0:
            logging.warning(
                f"Removed {len(removed_indices)} rows on column {column_name} with invalid {column_type} values. "
                f"Removed values:\n{dirty_values.to_string()}"
            )

    # Calculate overall removal percentage
    removed_count = original_row_count - len(cleaned_df)
    removal_percentage = (
        (removed_count / original_row_count) * 100 if original_row_count > 0 else 0
    )

    if removal_percentage > max_removal_percentage:
        raise Exception(
            f"Data cleaning removed {removal_percentage:.2f}% of data "
            f"(removed {removed_count} out of {original_row_count} rows), "
            f"which exceeds the maximum allowed threshold of {max_removal_percentage}%"
        )

    logging.info(
        f"Overall data cleaning: {removal_percentage:.2f}% of data removed "
        f"({removed_count} out of {original_row_count} rows)"
    )

    return cleaned_df
