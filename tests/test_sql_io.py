#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import pandas as pd
from pandas_mate import sql_io
from pandas_mate import transform
from pandas_mate.tests import create_test_df

import random
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import String, Integer, Float
from sqlalchemy import select


def test_smart_insert():
    def count_n_rows(engine, table):
        """Count how many row in a table.
        """
        return engine.execute(table.count()).fetchone()[0]

    # define database
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    t_test = Table(
        "test", metadata,
        Column("_id", String, primary_key=True),
        Column("int", Integer),
        Column("float", Float),
    )
    metadata.create_all(engine)

    # create test data
    df = create_test_df(n_rows=1000)

    # insert 5 data
    rows = list(transform.to_index_row_dict(df, "_id").values())
    data = random.sample(rows, 5)
    engine.execute(t_test.insert(), data)
    assert count_n_rows(engine, t_test) == 5

    # regular method will fail
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        df.to_sql("test", engine, index=False, if_exists="append")
    assert count_n_rows(engine, t_test) == 5

    # smart_insert method success
    sql_io.smart_insert(df, t_test, engine)
    assert count_n_rows(engine, t_test) == 1000


def test_excel_to_sql():
    path = os.path.join(os.path.dirname(__file__), "company.xlsx")
    engine = create_engine("sqlite:///:memory:")
    sql_io.excel_to_sql(path, engine)

    metadata = MetaData()
    metadata.reflect(engine)

    for table in metadata.tables.values():
        sql = select([table]).count()
        count = engine.execute(sql).fetchone()[0]

        df = pd.read_excel(path, table.name)
        assert count == df.shape[0]


def test_database_to_excel():
    path = os.path.join(os.path.dirname(__file__), "company.xlsx")
    engine = create_engine("sqlite:///:memory:")
    sql_io.excel_to_sql(path, engine)

    excel_file_path = os.path.join(os.path.dirname(__file__), "export.xlsx")
    sql_io.database_to_excel(engine, excel_file_path)


if __name__ == "__main__":
    import os
    pytest.main([os.path.basename(__file__), "--tb=native", "-s", ])
