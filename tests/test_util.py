#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
from pandas_mate.tests import create_test_df
from pandas_mate import util

data_file_path = os.path.join(os.path.dirname(__file__), "data.csv")


def setup_module(module):
    if not os.path.exists(data_file_path):
        df = create_test_df(1000000)
        df.to_csv(data_file_path, index=False)


def test_read_csv_arg_preprocess():
    iterator, chunksize = util.read_csv_arg_preprocess(
        data_file_path, 5*1000*1000)
    assert iterator is True
    assert chunksize <= 100000  # 83334


def test_ascii_table():
    df = create_test_df(3)
    s = util.ascii_table(df)


if __name__ == "__main__":
    import os
    pytest.main([os.path.basename(__file__), "--tb=native", "-s", ])
