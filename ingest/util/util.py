import re
import os
import logging


# -------------------------------------------------------
# Utilities
# -------------------------------------------------------
def mask_pw(odbc_str):
    """
    Mask uid and pwd in ODBC connection string for logging
    """
    # Use regex to mask uid= and pwd= values
    masked_str = re.sub(r"(uid=|pwd=)[^;]*", r"\1****", odbc_str, flags=re.IGNORECASE)
    return masked_str


def get_excel_files(dir_path):
    """
    Returns a list of Excel files in a given directory. Does not recurse.
    """
    return [
        os.path.join(dir_path, f)
        for f in os.listdir(dir_path)
        if f.endswith(".xlsx") and not f.startswith(".") and not f.startswith("~")
    ]


def find_data_files(path, exclude=None):
    """
    Return list of full path for all files in a directory recursively.
    Filter out any files starting with . or ~.
    """
    ret = []
    for dirpath, _dirnames, files in os.walk(path):
        for file in files:
            # Filter out temporary files: anything that starts with . or ~
            if not file.startswith(".") and not file.startswith("~"):
                # Filter out explicitly excluded files
                filepath = os.path.join(dirpath, file)
                if exclude is None or filepath not in exclude:
                    ret.append(filepath)

    return sorted(ret)


def error_exit(msg):
    logging.error(msg)
    exit(1)
