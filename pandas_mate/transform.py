#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module convert pandas's data type into varieties of form.
"""

import sys
if sys.version_info[0] == 2:
    from itertools import (
        izip as zip,
    )
import json
import base64
from collections import OrderedDict
import pandas as pd
try:
    from .pkg.rolex import rolex
except:
    from pandas_mate.pkg.rolex import rolex


def itertuple(df):
    """High performance tuple style iterator.

    :param df: ``pandas.DataFrame`` instance.

    **中文文档**

    对 ``pandas.DataFrame`` 进行tuple风格的高性能行遍历。
    """
    return zip(*(l for col, l in df.iteritems()))


def grouper_df(df, chunksize):
    """Evenly divide pd.DataFrame into n rows piece, no filled value 
    if sub dataframe's size smaller than n.

    :param df: ``pandas.DataFrame`` instance.
    :param chunksize: number of rows of each small DataFrame.

    **中文文档**

    将 ``pandas.DataFrame`` 分拆成等大小的小DataFrame。
    """
    data = list()
    counter = 0
    for tp in zip(*(l for col, l in df.iteritems())):
        counter += 1
        data.append(tp)
        if counter == chunksize:
            new_df = pd.DataFrame(data, columns=df.columns)
            yield new_df
            data = list()
            counter = 0

    if len(data) > 0:
        new_df = pd.DataFrame(data, columns=df.columns)
        yield new_df


def to_index_row_dict(df, index_col=None, use_ordered_dict=True):
    """Transform data frame to list of dict.

    :param index_col: None or str, the column that used as index.
    :param use_ordered_dict: if True, row dict is has same order as df.columns. 

    **中文文档**

    将dataframe以指定列为key, 转化成以行为视角的dict结构, 提升按行index访问
    的速度。若无指定列, 则使用index。
    """
    if index_col:
        index_list = df[index_col]
    else:
        index_list = df.index

    columns = df.columns

    if use_ordered_dict:
        table = OrderedDict()
    else:
        table = dict()

    for ind, tp in zip(index_list, itertuple(df)):
        table[ind] = dict(zip(columns, tp))

    return table


def to_dict_list(df, use_ordered_dict=True):
    """Transform each row to dict, and put them into a list.

    **中文文档**

    将 ``pandas.DataFrame`` 转换成一个字典的列表。列表的长度与行数相同, 其中
    每一个字典相当于表中的一行, 相当于一个 ``pandas.Series`` 对象。
    """
    if use_ordered_dict:
        dict = OrderedDict

    columns = df.columns
    data = list()
    for tp in itertuple(df):
        data.append(dict(zip(columns, tp)))
    return data


def to_dict_list_generic_type(df, int_col=None, binary_col=None):
    """Transform each row to dict, and put them into a list. And automatically
    convert ``np.int64`` to ``int``, ``pandas.tslib.Timestamp`` to 
    ``datetime.datetime``, ``np.nan`` to ``None``.

    :param df: ``pandas.DataFrame`` instance.
    :param int_col: integer type columns.
    :param binary_col: binary type type columns.

    **中文文档**

    由于 ``pandas.Series`` 中的值的整数数据类型是 ``numpy.int64``, 
    时间数据类型是 ``pandas.tslib.Timestamp``, None的数据类型是 ``np.nan``。
    虽然从访问和计算的角度来说没有什么问题, 但会和很多数据库的操作不兼容。 

    此函数能将 ``pandas.DataFrame`` 转化成字典的列表。数据类型能正确的获得int, 
    bytes和datetime.datetime。
    """
    # Pre-process int_col, binary_col and datetime_col
    if (int_col is not None) and (not isinstance(int_col, (list, tuple))):
        int_col = [int_col, ]

    if (binary_col is not None) and (not isinstance(binary_col, (list, tuple))):
        binary_col = [binary_col, ]

    datetime_col = list()
    for col, dtype in dict(df.dtypes).items():
        if "datetime64" in str(dtype):
            datetime_col.append(col)
    if len(datetime_col) == 0:
        datetime_col = None

    # Pre-process binary column dataframe
    def b64_encode(b):
        try:
            return base64.b64encode(b)
        except:
            return b

    if binary_col is not None:
        for col in binary_col:
            df[col] = df[col].apply(b64_encode)

    data = json.loads(df.to_json(orient="records", date_format="iso"))

    if int_col is not None:
        for row in data:
            for col in int_col:
                try:
                    row[col] = int(row[col])
                except:
                    pass

    if binary_col is not None:
        for row in data:
            for col in binary_col:
                try:
                    row[col] = base64.b64decode(row[col].encode("ascii"))
                except:
                    pass

    if datetime_col is not None:
        for row in data:
            for col in datetime_col:
                try:
                    row[col] = rolex.str2datetime(row[col])
                except:
                    pass

    return data
