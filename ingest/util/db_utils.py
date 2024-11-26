import re
import urllib.parse
import logging
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


def clear_tables_and_insert_data(session: Session, tables_data: List[TableData]):
    """
    Write data from dataframes to DB tables, clearing and overwriting existing tables
    """
    for table_data in tables_data:
        logging.info(f"Writing data to table: {table_data.table.__tablename__}")

        # Clear data in DB
        session.exec(delete(table_data.table))

        # Select columns from dataframe that match table columns, except "id" column
        table_columns = table_data.table.__table__.columns.keys()
        table_columns.remove("id")

        # Remove columns that aren't in the dataframe
        table_columns = [col for col in table_columns if col in table_data.df.columns]

        # Write data from dataframe
        df = table_data.df[table_columns]
        df.to_sql(
            name=table_data.table.__tablename__,
            con=session.connection(),
            if_exists="append",
            index=False,
        )
