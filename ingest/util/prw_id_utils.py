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


def calc_prw_id(
    df: pd.DataFrame, src_id_col="mrn", id_col="prw_id", src_id_to_id_df=None
) -> pd.DataFrame:
    """
    Given a DataFrame with a source ID column, deterministically calculates a unique prw_id column by
    hashing the source ID
    """
    if src_id_to_id_df is not None:
        src_id_to_id_df = src_id_to_id_df.loc[:, [src_id_col, id_col]]
        df = df.merge(src_id_to_id_df, on=src_id_col, how="left")

        # For any rows where id_col didn't exist in the given mapping, calculate it, making sure
        # the new value is unique
        rows_missing_id = df[id_col].isna()
        new_ids_df = pd.DataFrame(
            {
                src_id_col: df.loc[rows_missing_id, src_id_col],
                id_col: df.loc[rows_missing_id, src_id_col].apply(prw_id_base),
            }
        )
        prw_id_ensure_unique(new_ids_df, id_col=id_col)

        # Detect collisions where id in new_ids_df was already used in existing src_id_to_id_df
        collisions = new_ids_df[new_ids_df[id_col].isin(src_id_to_id_df[id_col])]
        while len(collisions) > 0:
            # Rehash collisions until all are unique
            for idx, row in collisions.iterrows():
                new_ids_df.at[idx, id_col] = str(
                    fnv.hash(row[id_col].encode(), bits=32)
                )
            prw_id_ensure_unique(new_ids_df, id_col=id_col)
            collisions = new_ids_df[new_ids_df[id_col].isin(src_id_to_id_df[id_col])]

        # Assign new prw_ids to df by matching on src_id_col
        df = df.merge(new_ids_df, on=src_id_col, how="left", suffixes=("", "_new"))
        df[id_col] = df[id_col + "_new"].fillna(df[id_col])
        df = df.drop(columns=[id_col + "_new"])

    else:
        df[id_col] = df[src_id_col].apply(prw_id_base)
        prw_id_ensure_unique(df, id_col=id_col)

    return df


def mrn_to_prw_id_col(
    df: pd.DataFrame, mrn_to_prw_id_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Given a DataFrame with a MRN column, calculates a PRW ID column and updates the mrn_to_prw_id_df
    with new mappings. Returns the updated DataFrame with the PRW ID column and the new mappings.
    """
    # Calculate PRW ID from MRN for patients that don't have one
    patients_df = df.drop_duplicates(subset=["mrn"], keep="first")
    patients_df = patients_df.dropna(subset=["mrn"])
    patients_df = calc_prw_id(
        patients_df,
        src_id_to_id_df=mrn_to_prw_id_df,
    )

    # Get new PRW IDs that were added and combine with existing map
    new_ids_df = patients_df[~patients_df["prw_id"].isin(mrn_to_prw_id_df["prw_id"])]
    mrn_to_prw_id_df = pd.concat([mrn_to_prw_id_df, new_ids_df[["prw_id", "mrn"]]])

    # Merge into data
    df = df.merge(mrn_to_prw_id_df, on="mrn", how="left")

    # Return data with PRW IDs and new IDs that were created
    return df, new_ids_df
