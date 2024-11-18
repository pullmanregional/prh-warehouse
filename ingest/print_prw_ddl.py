"""
Print the Data Definition Language (DDL) statements for the PRW model.
"""

from sqlalchemy import create_mock_engine
from prw_model import *


def dump(sql, *multiparams, **params):
    print(sql.compile(dialect=engine.dialect))


if __name__ == "__main__":
    engine = create_mock_engine("mssql+pyodbc://", dump)
    PrwIdModel.metadata.create_all(engine, checkfirst=False)
    PrwMetaModel.metadata.create_all(engine, checkfirst=False)
    PrwModel.metadata.create_all(engine, checkfirst=False)
    PrwFinanceModel.metadata.create_all(engine, checkfirst=False)
