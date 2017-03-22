#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import warnings
from datetime import datetime
import pytest
import numpy as np
import pandas as pd
from sfm.timer import Timer
from pandas_mate import transform
from pandas_mate.tests import create_test_df


data_file_path = os.path.join(os.path.dirname(__file__), "data.csv")


def setup_module(module):
    if not os.path.exists(data_file_path):
        df = create_test_df(1000000)
        df.to_csv(data_file_path, index=False)


def test_itertuple():
    display = False

    df = pd.read_csv(data_file_path)

    with Timer(display=display, title="DataFrame.itertuples()") as timer1:
        for _id, a, b in df.itertuples(index=False):
            pass

    with Timer(display=display, title="itertuples()") as timer2:
        for _id, a, b in transform.itertuple(df):
            pass

    if not timer2.elapsed < timer1.elapsed:
        warnings.warn(
            "DataFrame.itertuples() should not faster than itertuples(df)!")


def test_to_index_row_dict():
    display = False

    df = create_test_df(1000)
    df.index = df["_id"]

    with Timer(display=display, title="DataFrame.iterrows(）") as timer1:
        table = dict()
        for ind, row in df.iterrows():
            table[ind] = dict(row)

    with Timer(display=display, title="to_index_row_dict()") as timer2:
        table = transform.to_index_row_dict(df)
        for ind in df.index:
            row = table[ind]

    if not timer2.elapsed < timer1.elapsed:
        warnings.warn("to_index_row_dict() is slower than iterrows()!")


def test_grouper_df():
    df = pd.DataFrame(np.random.random((10, 4)))
    df1, df2, df3, df4 = list(transform.grouper_df(df, 3))
    for df_ in [df1, df2, df3]:
        assert df_.shape == (3, 4)
    assert df4.shape == (1, 4)


def test_to_dict_list():
    data = [
        ["Jack", 1, 0.1, None],
        ["Tom", 2, None, datetime(2000, 1, 2)],
        ["Bob", None, 0.3, datetime(2000, 1, 3)],
        [None, 4, 0.4, datetime(2000, 1, 4)],
    ]
    df = pd.DataFrame(data, columns=list("ABCD"))
    dict_list = transform.to_dict_list(df)
    for l, d in zip(data, dict_list):
        for i, j in zip(l, d.values()):
            if i is None:
                pass
            else:
                assert i == j


def test_to_dict_list_generic_type():
    data = [
        ["Jack", 1, 0.1, None],
        ["Tom", 2, None, datetime(2000, 1, 2)],
        ["Bob", None, 0.3, datetime(2000, 1, 3)],
        [None, 4, 0.4, datetime(2000, 1, 4)],
    ]
    df = pd.DataFrame(data, columns=list("ABCD"))
    data = transform.to_dict_list_generic_type(df, int_col="B")
    doc1, doc2, doc3, doc4 = data

    assert doc1["D"] is None
    assert doc2["C"] is None
    assert doc3["B"] is None
    assert doc4["A"] is None

    assert doc1["B"] == 1
    assert doc2["B"] == 2
    assert doc4["B"] == 4

    assert doc2["D"].day == 2
    assert doc3["D"].day == 3
    assert doc4["D"].day == 4


if __name__ == "__main__":
    import os
    pytest.main([os.path.basename(__file__), "--tb=native", "-s", ])
