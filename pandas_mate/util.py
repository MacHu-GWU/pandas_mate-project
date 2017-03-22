#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import math


def count_lines(abspath):
    """Count how many lines in a pure text file.
    """
    with open(abspath, "rb") as f:
        i = 0
        for line in f:
            i += 1
            pass
        return i


def read_csv_arg_preprocess(abspath, memory_usage=100*1000*1000):
    """

    :param abspath: csv file absolute path.
    :param memory_usage: max memory will be used for pandas.read_csv().
    """
    if memory_usage < 1000*1000:
        raise ValueError("Please specify a valid memory usage for read_csv, "
                         "the value should larger than 1MB and less than "
                         "your available memory.")

    size = os.path.getsize(abspath)  # total size in bytes
    n_time = math.ceil(size * 1.0 / memory_usage)  # at lease read n times

    lines = count_lines(abspath)  # total lines
    chunksize = math.ceil(lines / n_time)  # lines to read each time

    if chunksize >= lines:
        iterator = False
        chunksize = None
    else:
        iterator = True
    return iterator, chunksize
