"""
Tools to translate patient IDs to prw_ids and ensure uniqueness
"""

import logging
import pandas as pd
from util import fnv


def prw_id_base(patient_id: int | str) -> int:
    """
    Given a unique ID, returns a unique hash to use as prw_id.
    This hash has low collision probability, but still needs to be verified to be unique
    before assignment to a patient
    """
    # Unqiue value used to salt prw_id hash
    PRW_ID_SALT = "560f80d9-b01f-4bda-84fd-1b39c56c5be5"
    id = str(f"{PRW_ID_SALT}#{patient_id}").encode()
    return str(fnv.hash(id, bits=32))


def prw_id_ensure_unique(df: pd.DataFrame, id_col="prw_id") -> pd.DataFrame:
    """
    Given a DataFrame with the id_col column, ensures that the values in id_col are unique,
    rehashing if needed. Updates values in place.
    """
    while df[id_col].duplicated().any():
        duplicates = df[df[id_col].duplicated()][id_col]
        for idx in duplicates.index[1:]:
            logging.warning(f"Updating prw_id collision: {df.at[idx, id_col]} ({idx})")
            id = str(f"{df.at[idx, id_col]}-{idx}").encode()
            df.at[idx, id_col] = str(fnv.hash(id, bits=32))
    return df


def calc_prw_id(df: pd.DataFrame, src_id_col="mrn", id_col="prw_id") -> pd.DataFrame:
    """
    Given a DataFrame with a source ID column, deterministically calculates a unique prw_id column by
    hashing the source ID
    """
    df[id_col] = df[src_id_col].apply(prw_id_base)
    prw_id_ensure_unique(df, id_col=id_col)
    return df
