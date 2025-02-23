import re
import urllib.parse
import logging
import time
import numpy as np
import pandas as pd
import sqlalchemy
from typing import List
from dataclasses import dataclass
from sqlmodel import SQLModel, create_engine, delete, Session


# Association of table with its data to update in a DB
@dataclass
class TableData:
    table: SQLModel
    df: pd.DataFrame


# -------------------------------------------------------
# DB Utilities
# -------------------------------------------------------
def get_db_connection(odbc_str: str, echo: bool = False) -> sqlalchemy.Engine:
    """
    Given an ODBC connection string, return a connection to the DB via SQLModel
    """
    # Split connection string into odbc prefix and parameters (ie everything after odbc_connect=)
    match = re.search(r"^(.*odbc_connect=)(.*)$", odbc_str)
    prefix = match.group(1) if match else ""
    params = match.group(2) if match else ""
    if prefix and params:
        # URL escape ODBC connection string
        conn_str = prefix + urllib.parse.quote_plus(params)
    else:
        # No odbc_connect= found, just original string
        conn_str = odbc_str

    # Use SQLModel to establish connection to DB
    try:
        if conn_str.startswith('mssql'):
            # Optimize with fast_executemany, which is supported by MSSQL / Azure SQL
            engine = create_engine(conn_str, echo=echo, fast_executemany=True)
        else:
            engine = create_engine(conn_str, echo=echo)
        return engine
    except Exception as e:
        logging.error(f"ERROR: failed to connect to DB")
        logging.error(e)
        return None


def clear_tables(session: Session, tables: List[SQLModel]):
    """
    Delete all rows from specified tables
    """
    for table in tables:
        logging.info(f"Clearing table: {table.__tablename__}")
        session.exec(delete(table))


def _convert_df_dtypes_to_db(table_data: TableData, table_columns: List[str]):
    """
    Convert dataframe columns to match database column types by
    mapping SQLAlchemy types to pandas-compatible dtypes
    """
    df = table_data.df
    for col in table_columns:
        if col in df.columns:
            sa_column = table_data.table.__table__.columns[col]
            current_dtype = str(df[col].dtype)
            
            if isinstance(sa_column.type, sqlalchemy.String):
                target_dtype = "object"
            elif isinstance(sa_column.type, sqlalchemy.Integer):
                # Use pandas nullable integer type if there are NaNs
                target_dtype = "Int64" if df[col].isna().any() else "int64"
            elif isinstance(sa_column.type, sqlalchemy.Float):
                target_dtype = "float64"
            elif isinstance(sa_column.type, sqlalchemy.DateTime) or isinstance(sa_column.type, sqlalchemy.Date):
                target_dtype = "datetime64[ns]"
            elif isinstance(sa_column.type, sqlalchemy.Boolean):
                target_dtype = "bool"
            else:
                target_dtype = "object"

            # Only convert if needed
            if current_dtype != target_dtype:
                logging.info(f"Converting column {col} from {current_dtype} to {target_dtype}")
                # Can use .copy() to avoid SettingWithCopyWarning
                df[col] = df[col].astype(target_dtype, copy=False)


def clear_tables_and_insert_data(
    session: Session, tables_data: List[TableData], chunk_size: int = 100000
):
    """
    Write data from dataframes to DB tables, clearing and overwriting existing tables
    """
    for table_data in tables_data:
        logging.info(
            f"Writing data to table: {table_data.table.__tablename__}, rows: {len(table_data.df)}"
        )

        # Clear data in DB
        logging.info(f"Clearing table: {table_data.table.__tablename__}")
        session.exec(delete(table_data.table))

        # Select columns from dataframe that match table columns
        table_columns = table_data.table.__table__.columns.keys()

        # Remove columns that aren't in the dataframe
        table_columns = [col for col in table_columns if col in table_data.df.columns]

        # Remove the PK column from the dataframe, which will be computed by the DB
        if table_data.table.__table__.primary_key.columns.keys()[0] in table_columns:
            table_columns.remove(
                table_data.table.__table__.primary_key.columns.keys()[0]
            )

        # Convert dataframe datatypes to match DB types
        _convert_df_dtypes_to_db(table_data, table_columns)

        # Write data from dataframe
        start_time = time.time()
        logging.info(f"Writing table: {table_data.table.__tablename__}")
        df = table_data.df[table_columns]
        df.to_sql(
            name=table_data.table.__tablename__,
            con=session.connection(),
            if_exists="append", 
            index=False,
            chunksize=chunk_size,
        )
        elapsed_time = time.time() - start_time
        logging.info(f"Wrote {len(df)} rows to {table_data.table.__tablename__} in {elapsed_time:.2f}s")
