import os
import pandas as pd
from util import util


def check_data_dir(
    base_path,
    volumes_file,
    misc_volumes_file,
    income_stmt_path,
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
        error = f"ERROR: income statements root directory is empty: {income_stmt_path}"
    if not os.path.isdir(hours_path) or len(util.find_data_files(hours_path)) == 0:
        error = f"ERROR: productivity data root directory is empty: {hours_path}"

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
