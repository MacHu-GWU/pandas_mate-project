#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module provide database I/O interface.
"""

import math
import pandas as pd
try:
    from .transform import grouper_df, to_dict_list_generic_type
except:
    from pandas_mate.transform import grouper_df, to_dict_list_generic_type


def smart_insert(df, table, engine, minimal_size=5):
    """An optimized Insert strategy.

    **中文文档**

    一种优化的将大型DataFrame中的数据, 在有IntegrityError的情况下将所有
    好数据存入数据库的方法。
    """
    from sqlalchemy.exc import IntegrityError
    
    try:
        table_name = table.name
    except:
        table_name = table
    
    # 首先进行尝试bulk insert
    try:
        df.to_sql(table_name, engine, index=False, if_exists="append")
    # 失败了
    except IntegrityError:
        # 分析数据量
        n = df.shape[0]
        # 如果数据条数多于一定数量
        if n >= minimal_size ** 2:
            # 则进行分包
            n_chunk = math.floor(math.sqrt(n))
            for sub_df in grouper_df(df, n_chunk):
                smart_insert(
                    sub_df, table_name, engine, minimal_size)
        # 否则则一条条地逐条插入
        else:
            for sub_df in grouper_df(df, 1):
                try:
                    sub_df.to_sql(
                        table_name, engine, index=False, if_exists="append")
                except IntegrityError:
                    pass


def excel_to_sql(excel_file_path, engine,
                 read_excel_kwargs=None,
                 to_generic_type_kwargs=None,
                 to_sql_kwargs=None):
    """Create a database from excel.

    :param read_excel_kwargs: dict, arguments for ``pandas.read_excel`` method.
      example: ``{"employee": {"skiprows": 10}, "department": {}}``
    :param to_sql_kwargs: dict, arguments for ``pandas.DataFrame.to_sql`` 
      method.

    limitation:

    1. If a integer column has None value, data type in database will be float.
      Because pandas thinks that it is ``np.nan``.
    2. If a string column looks like integer, ``pandas.read_excel()`` method
      doesn't have options to convert it to string.
    """
    if read_excel_kwargs is None:
        read_excel_kwargs = dict()

    if to_sql_kwargs is None:
        to_sql_kwargs = dict()

    if to_generic_type_kwargs is None:
        to_generic_type_kwargs = dict()

    xl = pd.ExcelFile(excel_file_path)
    for sheet_name in xl.sheet_names:
        df = pd.read_excel(
            excel_file_path, sheet_name,
            **read_excel_kwargs.get(sheet_name, dict())
        )
        
        kwargs = to_generic_type_kwargs.get(sheet_name)
        if kwargs:
            data = to_dict_list_generic_type(df, **kwargs)
            smart_insert(data, sheet_name, engine)
        else:
            df.to_sql(
                sheet_name, engine, index=False,
                **to_sql_kwargs.get(sheet_name, dict(if_exists="replace"))
            )


def database_to_excel(engine, excel_file_path):
    """Export database to excel.
    
    :param engine: 
    :param excel_file_path:
    """
    from sqlalchemy import MetaData, select
    
    metadata = MetaData()
    metadata.reflect(engine)
    
    writer = pd.ExcelWriter(excel_file_path)
    for table in metadata.tables.values():
        sql = select([table])
        df = pd.read_sql(sql, engine)
        df.to_excel(writer, table.name, index=False)
    
    writer.save()