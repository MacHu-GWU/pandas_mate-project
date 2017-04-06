#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from sfm import rnd

def create_test_df(n_rows):
    """
    
    **中文文档**
    
    创建一个 ``n_rows`` 行的测试用DataFrame。
    """
    df = pd.DataFrame()
    df["_id"] = [rnd.rand_hexstr(32) for i in range(n_rows)]
    df["int"] = np.random.randint(1, 1000, n_rows)
    df["float"] = np.random.random(n_rows)
    return df