#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: mithril
# Created Date: 2022-07-15 17:16:45
# Last Modified: 2022-07-15 17:25:05


import re
def safepath(path, type='win'):
    '''path 转换
    
    >>> s = 's3://sss/ddS12\\sddd/ddd//ssS1\\\333/////333'
    >>> re.findall(r'(?<![(\\\\)/])[(\\\\)/](?![(\\\\)/])', s)
    ['/', '\\', '/', '\\']
    
    >>> re.sub(r'(?<![(\\\\)/])[(\\\\)/](?![(\\\\)/])', os.sep, s)
    error: bad escape (end of pattern) at position 0
    
    # windows 的 os.sep 是 \\， 转义了不行
    >>> re.sub(r'(?<![(\\\\)/])[(\\\\)/](?![(\\\\)/])', re.escape(os.sep), s)
    's3://sss\\ddS12\\sddd\\ddd//ssS1\\333'


    目标是转换为 's3://sss/ddS12/sddd/ddd/ssS1/333'
    '''

    match_duoble_slashs =  r'(?<!:)[\\|/]{2}' # 匹配 // \\  不匹配 ://
    match_slashs = r'(?<![\\/])[\\/](?![\\/])' # 前后都不能存在\ / , 只匹配 / \ 

    if type == 'unix':
        sep = '/'
    else:
        sep = '\\\\'

    return re.sub(match_slashs, sep, re.sub(match_duoble_slashs, sep, path) )



from itertools import chain
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype


def apply_parallel(dfGrouped, func):
    from joblib import Parallel, delayed
    from multiprocessing import cpu_count
    
    retLst = Parallel(n_jobs=cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)


def exclude_outliers(df, col):
    return df[~(np.abs(df[col]-df[col].mean()) > (3*df[col].std()))]


def concat_with_cat(objs, catcols, keep_index=False, **kwargs):
    for c in catcols:
        newcats = set(chain(*[o[c].cat.categories for o in objs]))
        for o in objs:
            # if o[c].cat.categories.shape[0] < len(newcats):
            o[c] = o[c].astype(CategoricalDtype(newcats))

    df = pd.concat(objs, **kwargs)
    
    if keep_index:
        return df
    else:
        return df.reset_index(drop=True)


try:
    from toybox.path_utils import safepath
    from mlbox.pandas_utils import concat_with_cat
except:
    pass