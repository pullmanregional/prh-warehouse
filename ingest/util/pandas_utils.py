"""
Additional utility functions for working with data in pandas
"""

import pandas as pd
import re
from openpyxl.utils import cell


# ----------------------------------
# Pandas functions
# ----------------------------------
def df_get_val_or_range(df: pd.DataFrame, cell_range: str) -> pd.DataFrame:
    """
    Returns a subset of a dataframe using excel-like A1 notation.
    If given a range, returns a dataframe.
    If given a single location, returns the value.
    For example, df_get_range(df, "B2") returns the value in column 2, row 2,
    and df_get_range(df, "B2:D5") returns a dataframe with data from columns 2-4, rows 2-5.
    """
    # Check if provided range is a single coordinate or range
    if ":" in cell_range:
        cell_refs = re.split("[:]", cell_range)
        start_row, start_col = cell.coordinate_to_tuple(cell_refs[0])
        end_row, end_col = cell.coordinate_to_tuple(cell_refs[1])

        return df.iloc[start_row - 1 : end_row, start_col - 1 : end_col]
    else:
        row, col = cell.coordinate_to_tuple(cell_range)
        return df.iloc[row - 1, col - 1]


def df_find_by_column(
    df: pd.DataFrame, text: str, start_cell: str = "A1"
) -> tuple[int, int]:
    """
    Search the dataframe, df, by column for a cell equal to the given value, text.
    Return the (row, col) of the cell or None if not found
    start_cell is the cell to start searching from in A1 notation
    """
    # Convert A1 to row, col. These will be 1-based
    start_row, start_col = cell.coordinate_to_tuple(start_cell)

    for col_idx in range(start_col - 1, df.shape[1]):
        column = df.iloc[start_row - 1 :, col_idx]
        for row_idx, cell_value in enumerate(column):
            if str(cell_value).lower() == text.lower():
                # Return (row, column). start_row is 1-based
                return row_idx + start_row - 1, col_idx

    return None


def df_convert_first_row_to_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a dataframe, get the columns names from the first row, then drops the row
    """
    columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    df.columns = columns
    return df


def df_get_tables_by_columns(
    df: pd.DataFrame, rows: str, limit: int = 0
) -> list[pd.DataFrame]:
    """
    Returns a list of dataframes representing tables in the original dataframe based on specified rows.
    rows is specified in Excel A1-notation, eg. 5:10
    Specify limit > 0 to return a maximum number of tables
    """
    ret = []
    start_col = 0
    row_indices = _rows_A1_to_idx_list(rows)

    while (limit == 0) or (len(ret) < limit):
        # Find the next nonempty column after the current start_col
        nonempty_col = df_next_col(df, rows, start_col_idx=start_col)

        # Exit if no more data in columns
        if nonempty_col == -1:
            break

        # Find the next empty column after the nonempty_col
        empty_col = df_next_empty_col(df, rows, start_col_idx=nonempty_col)

        # If no more empty columns are found, use the entire remaining columns
        if empty_col == -1:
            empty_col = df.shape[1]

        # Extract the table as a dataframe and yield it
        table = df.iloc[row_indices, nonempty_col:empty_col]
        ret.append(table)

        # Start next iteration from the first empty column after the table
        start_col = empty_col

    return ret


def df_get_tables_by_rows(
    df: pd.DataFrame, cols: str, start_row_idx: int = 0, limit: int = 0
) -> list[pd.DataFrame]:
    """
    Yields dataframes representing tables in the original dataframe with data in specified columns.
    cols is specified in Excel A1-notation, eg. A:F
    Specify limit > 0 to return a maximum number of tables
    """
    ret = []
    start_row = start_row_idx
    col_indices = _cols_A1_to_idx_list(cols)

    while (limit == 0) or (len(ret) < limit):
        # Find the next nonempty row after the current start_row
        nonempty_row = df_next_row(df, cols, start_row_idx=start_row)

        # Exit if no more data in rows
        if nonempty_row == -1:
            break

        # Find the next empty row after the nonempty_col
        empty_row = df_next_empty_row(df, cols, start_row_idx=nonempty_row)

        # If no more empty rows are found, use the entire remaining rows
        if empty_row == -1:
            empty_row = df.shape[0]

        # Extract the table as a dataframe and yield it
        table = df.iloc[nonempty_row:empty_row, col_indices]
        ret.append(table)

        # Start next iteration from the first empty row after the table
        start_row = empty_row

    return ret


def df_get_table(
    df: pd.DataFrame, start_cell: str, has_header_row: bool = True
) -> pd.DataFrame:
    """
    Returns a dataframe with the first table in the original dataframe starting at the given cell in A1 notation.
    cols is specified in Excel A1-notation, eg. A:F
    """
    # Convert starting cell address from A1 notation to int. cell.coordinate_to_tuple() is 1 based
    row_start_idx, col_start_idx = cell.coordinate_to_tuple(start_cell)
    row_start_idx = row_start_idx - 1
    col_start_idx = col_start_idx - 1

    # First row with data is the starting row, unless there is a header row
    first_data_row_idx = row_start_idx + 1 if has_header_row else row_start_idx

    # Determine columns range of table by finding the first empty cell by column.
    # Find the column with both empty header (if exists) and first data row
    col_end = df.shape[1]
    for col in range(col_start_idx, df.shape[1]):
        header_val = df.iloc[row_start_idx, col]
        first_row_val = df.iloc[first_data_row_idx, col]
        if pd.isna(header_val) and pd.isna(first_row_val):
            col_end = col
            break

    # Determine row range of table by finding the empty row across all columns
    row_end = df.shape[0]
    for row in range(row_start_idx, df.shape[0]):
        row_data = df.iloc[row, col_start_idx:col_end]
        if row_data.isnull().all():
            row_end = row
            break

    # Extract table. Note, iloc() is exclusive of the end index.
    table = df.iloc[row_start_idx:row_end, col_start_idx:col_end]

    # Use first row as column names if indicated
    if has_header_row:
        table = df_convert_first_row_to_column_names(table)

    return table


def df_next_row(
    df: pd.DataFrame, columns: str, start_row_idx: int = 0, find_empty: bool = False
) -> int:
    """
    Given a dataframe, starting row offset, and set of columns, returns the next row index where there is data in one of the columns.
    columns is specified in Excel A1-notation, eg. A:F,AB,ZZ
    If find_empty is True, then returns next row where all the columns are empty
    """
    # Convert the columns from Excel A1-notation to column indices
    column_indices = _cols_A1_to_idx_list(columns)

    # Iterate over the rows starting from the specified row
    for row in range(start_row_idx, df.shape[0]):
        row_data = df.iloc[row, column_indices]
        if (not find_empty and not row_data.isnull().all()) or (
            find_empty and row_data.isnull().all()
        ):
            # Return index of either first non-empty or empty row, depending on find_empty parameter
            return row

    # Return -1 if no empty row is found
    return -1


def df_next_empty_row(df: pd.DataFrame, columns: str, start_row_idx: int = 0) -> int:
    """
    Given a dataframe, starting row offset, and set of columns, returns the next row index where all the columns are empty.
    columns is specified in Excel A1-notation, eg. A:F,AB,ZZ
    """
    return df_next_row(df, columns, start_row_idx, find_empty=True)


def df_next_col(
    df: pd.DataFrame, rows: str, start_col_idx: int = 0, find_empty: bool = False
) -> int:
    """
    Given a dataframe, starting column offset, and set of rows, returns the next column index where there is data in one of the rows.
    rows is specified in Excel A1-notation or row numbers (first row is 1), eg. 1:5,10,15
    If find_empty is True, then returns next column where all the rows are empty
    """
    # Convert the rows from Excel A1-notation to row indices
    row_indices = _rows_A1_to_idx_list(rows)

    # Iterate over the columns starting from the specified column
    for col in range(start_col_idx, df.shape[1]):
        col_data = df.iloc[row_indices, col]
        if (not find_empty and not col_data.isnull().all()) or (
            find_empty and col_data.isnull().all()
        ):
            return col

    # Return -1 if no non-empty column is found
    return -1


def df_next_empty_col(df: pd.DataFrame, rows: str, start_col_idx: int = 0) -> int:
    """
    Given a dataframe, starting column offset, and set of rows, returns the next column index where all the rows are empty.
    rows is specified in Excel A1-notation or row numbers (first row is 1), eg. 1:5,10,15
    """
    return df_next_col(df, rows, start_col_idx, find_empty=True)


def _cols_A1_to_idx_list(columns: str) -> list[int]:
    """
    Given a set of columns in Excel A1-notation or single row numbers, eg A:F,AB,ZZ
    return a list of 0-based row indexes in the range.
    """
    column_indices = []
    for column_range in columns.split(","):
        if ":" in column_range:
            start_col, end_col = column_range.split(":")
            column_indices.extend(
                range(
                    cell.column_index_from_string(start_col) - 1,
                    cell.column_index_from_string(end_col),
                )
            )
        else:
            column_indices.append(cell.column_index_from_string(column_range) - 1)
    return column_indices


def _rows_A1_to_idx_list(rows: str) -> list[int]:
    """
    Given a set of rows in Excel A1-notation or single row numbers, eg 1:5,10,15 (note, A1 row numbers are 1-based)
    return a list of 0-based row indexes in the range.
    """
    row_indices = []
    for row_range in rows.split(","):
        if ":" in row_range:
            start_row, end_row = row_range.split(":")
            row_indices.extend(range(int(start_row) - 1, int(end_row)))
        else:
            row_indices.append(int(row_range) - 1)
    return row_indices
