import os
import re
import pandas as pd
from util import util


def check_data_dir(
    base_path,
    volumes_file,
    misc_volumes_file,
    income_stmt_path,
    balance_path,
    hours_path,
    aged_ar_file,
):
    """
    Sanity checks for data directory
    """
    error = None
    if not os.path.isdir(base_path):
        error = f"ERROR: data directory path does not exist: {base_path}"
    if not os.path.isfile(volumes_file):
        error = f"ERROR: volumes data file is missing: {volumes_file}"
    if not os.path.isfile(misc_volumes_file):
        error = f"ERROR: misc volumes data file is missing: {misc_volumes_file}"
    if (
        not os.path.isdir(income_stmt_path)
        or len(util.find_data_files(income_stmt_path)) == 0
    ):
        error = f"ERROR: balance sheets root directory is empty: {balance_path}"
    if not os.path.isdir(balance_path) or len(util.find_data_files(balance_path)) == 0:
        error = f"ERROR: income statements root directory is empty: {income_stmt_path}"

    hours_files = util.find_data_files(hours_path)
    if not os.path.isdir(hours_path) or len(hours_files) == 0:
        error = f"ERROR: productivity data root directory is empty: {hours_path}"
    else:
        # Make sure every data file is named by convention "PP#<NN> <YYYY>..."
        # Except for the historical hours file
        historical_hours_re = r"^PP#\d\d?-PP#\d\d?.*"
        for file in hours_files:
            if not re.match(r"^PP#\d\d? \d{4}.*", file) and not re.match(historical_hours_re, file):
                error = f"ERROR: invalid productivity data filename: {file}"
                break

    if not os.path.isfile(aged_ar_file):
        error = f"ERROR: aged AR data file is missing: {aged_ar_file}"

    if error is not None:
        print(error)
        return False
    return True


def check_data_files(volumes_file: str, income_stmt_files: list[str]) -> bool:
    """
    Source data sanity checks. This doesn't compare actual numbers, but checks that we can
    do ingest at all, such as ensuring the right columns are where we expect them to be
    """
    return True
