"""
Print the Data Definition Language (DDL) statements for the PRW model.
"""

from sqlalchemy import create_mock_engine
from prw_model import prw_model
from prw_model import prw_id_model


def dump(sql, *multiparams, **params):
    print(sql.compile(dialect=engine.dialect))


if __name__ == "__main__":
    engine = create_mock_engine("mssql+pyodbc://", dump)
    prw_model.PrwModel.metadata.create_all(engine, checkfirst=False)
    prw_id_model.PrwIdModel.metadata.create_all(engine, checkfirst=False)
