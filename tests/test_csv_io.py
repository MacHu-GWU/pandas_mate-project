#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import pandas as pd
import warnings
from sfm.timer import Timer, EasyTimer
from pandas_mate import csv_io
from pandas_mate.tests import create_test_df


data_file_path = os.path.join(os.path.dirname(__file__), "data.csv")


def setup_module(module):
    if not os.path.exists(data_file_path):
        df = create_test_df(1000000)
        df.to_csv(data_file_path, index=False)


def test_iter_tuple_from_csv():
    display = False

    with Timer(display=display, title="pandas.read_csv()") as timer1:
        for _id, a, b  in pd.read_csv(data_file_path).itertuples(index=False):
            pass

    with Timer(display=display, title="iter_tuple_from_csv(iterator=True)") as timer2:
        for _id, a, b in csv_io.iter_tuple_from_csv(
                data_file_path, iterator=True, chunksize=1000):
            pass

    with Timer(display=display, title="iter_tuple_from_csv(iterator=False)") as timer3:
        for _id, a, b in csv_io.iter_tuple_from_csv(
                data_file_path):
            pass

    if not timer3.elapsed < timer1.elapsed:
        warnings.warn(
            "csv_io.iter_tuple_from_csv() is slower than itertuples()!")


def test_index_row_dict_from_csv():
    display = True

    with Timer(display=display) as timer:
        table1 = csv_io.index_row_dict_from_csv(
            data_file_path, index_col="_id", iterator=True, chunksize=1000)

    with Timer(display=display) as timer:
        table2 = csv_io.index_row_dict_from_csv(
            data_file_path, index_col="_id")


if __name__ == "__main__":
    import os
    pytest.main([os.path.basename(__file__), "--tb=native", "-s", ])
