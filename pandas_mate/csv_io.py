#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module provide advance csv I/O interface.
"""

from collections import OrderedDict
import pandas as pd
try:
    from .transform import itertuple
except:
    from pandas_mate.transform import itertuple


def iter_tuple_from_csv(path,
                        iterator=False,
                        chunksize=None,
                        skiprows=None,
                        nrows=None,
                        **kwargs):
    """A high performance, low memory usage csv file row iterator function.

    :param path: csv file path.
    :param iterator:
    :param chunksize:
    :param skiprows:
    :param nrows:

    :yield tuple: 

    **中文文档**

    对dataframe进行tuple风格的高性能行遍历。

    对用pandas从csv文件读取的dataframe进行逐行遍历时, iterrows和itertuple
    都不是性能最高的方法。这是因为iterrows要生成Series对象, 而itertuple
    也要对index进行访问。所以本方法是使用内建zip方法对所有的column进行打包
    解压, 所以性能上是最佳的。
    """
    kwargs["iterator"] = iterator
    kwargs["chunksize"] = chunksize
    kwargs["skiprows"] = skiprows
    kwargs["nrows"] = nrows

    if iterator is True:
        for df in pd.read_csv(path, **kwargs):
            for tp in itertuple(df):
                yield tp
    else:
        df = pd.read_csv(path, **kwargs)
        for tp in itertuple(df):
            yield tp


def index_row_dict_from_csv(path,
                            index_col=None,
                            iterator=False,
                            chunksize=None,
                            skiprows=None,
                            nrows=None,
                            use_ordered_dict=True,
                            **kwargs):
    """Read the csv into a dictionary. The key is it's index, the value
    is the dictionary form of the row.

    :param path: csv file path.
    :param index_col: None or str, the column that used as index.
    :param iterator:
    :param chunksize:
    :param skiprows:
    :param nrows:
    :param use_ordered_dict:

    :returns: {index_1: row1, index2: row2, ...} 

    **中文文档**

    读取csv, 选择一值完全不重复, 可作为index的列作为index, 生成一个字典
    数据结构, 使得可以通过index直接访问row。
    """
    _kwargs = dict(list(kwargs.items()))
    _kwargs["iterator"] = None
    _kwargs["chunksize"] = None
    _kwargs["skiprows"] = 0
    _kwargs["nrows"] = 1

    df = pd.read_csv(path, index_col=index_col, **_kwargs)
    columns = df.columns

    if index_col is None:
        raise Exception("please give index_col!")

    if use_ordered_dict:
        table = OrderedDict()
    else:
        table = dict()

    kwargs["iterator"] = iterator
    kwargs["chunksize"] = chunksize
    kwargs["skiprows"] = skiprows
    kwargs["nrows"] = nrows

    if iterator is True:
        for df in pd.read_csv(path, index_col=index_col, **kwargs):
            for ind, tp in zip(df.index, itertuple(df)):
                table[ind] = dict(zip(columns, tp))
    else:
        df = pd.read_csv(path, index_col=index_col, **kwargs)
        for ind, tp in zip(df.index, itertuple(df)):
            table[ind] = dict(zip(columns, tp))

    return table
