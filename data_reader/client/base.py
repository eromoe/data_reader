# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import

import sys
import os
from posixpath import join
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__)) ) )
from mlbox.pandas_utils import concat_with_cat
from toybox.path_utils import safepath
from .log import logger

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class BaseClient(object):
    def __init__(self, categories=None, suffix='.parquet', **kwargs):
        # can't use or, need allow []
        self.categories = categories
        self.suffix = suffix

    def ls(self, *args, **kwargs):
        raise NotImplementedError

    def exists(self, *args, **kwargs):
        raise NotImplementedError

    def glob(self, *args, **kwargs):
        raise NotImplementedError

    def open(self, *args, **kwargs):
        raise NotImplementedError

    def iter_subfolders(self, path):
        raise NotImplementedError

    def validate_path(self, path):
        return path.endswith(self.suffix)

    def concat_dfs(self, dfs, categories=None):
        categories = categories or self.categories
        if categories is None:
            return pd.concat(dfs)
        else:
            try:
                return concat_with_cat(dfs, categories)
            except:
                logger.exception('Concat error with categories: %s', categories)
                safe_cats = [c for c in categories if c in dfs[0].columns]

                if len(safe_cats)>0:
                    logger.warn('Safe catcols:%s doesnt match categories:%s' % (safe_cats, categories))
                    return concat_with_cat(dfs, safe_cats)
                else:
                    logger.warn('No cat match categories:%s, concat without categories' % (categories,))
                    return pd.concat(dfs).reset_index(drop=True)


    def read(self, path, columns=None):
        assert self.validate_path(path)

        if self.suffix == '.parquet':
            x = pd.read_parquet(path, columns=columns)
        elif self.suffix == '.csv':
            x = pd.read_csv(path, header=True, columns=columns)

        if isinstance(x.index, pd.MultiIndex):
            x = x.reset_index()

        if self.categories:
            for c in self.categories:
                if c in x.columns:
                    x[c] = x[c].astype('category')

        return x
    
    def read_dir(self, dirpath, columns=None, categories=None):
        dfs = list()

        for p in self.ls(dirpath):
            logger.debug(dirpath)
            if self.validate_path(p):
                dfs.append(self.read(p, columns=columns))
        
        if len(dfs):
            return self.concat_dfs(dfs)

        raise FileNotFoundError('No file found under dir :%s' % dirpath)

    def read_wildcard_path(self, path, columns=None, categories=None):
        path = safepath(path, 'unix')
        dfs = []
        for i in self.glob(path):
            logger.debug(i)
            x = self.read(i, columns=columns)
            dfs.append(x)

        if dfs:
            df = self.concat_dfs(dfs)
        else:
            raise FileNotFoundError('No file found with pattern :%s' % path)

        return df

    def iter_read_paths(self, paths, columns=None, categories=None):
        for path in paths:
            try:
                if '*' in path:
                    print('Read wildcard path: %s' % path)
                    x = self.read_wildcard_path(path, columns=columns)
                elif path.endswith('/') or path.endswith('\\'):
                    print('Read dir: %s' % path )
                    x = self.read_dir(path, columns=columns)
                    if x is None:
                        print('No files under: %s' % path) 
                        continue
                else:
                    print(path)
                    x = self.read(path, columns=columns)

                yield x
            except FileNotFoundError as e:
                print(e)
                continue

    def read_paths(self, paths, columns=None, categories=None, concat_size=None):
        if not concat_size:
            dfs = list(self.iter_read_paths(paths, columns=columns))
            if not len(dfs):
                raise Exception('Reader did not find files in : %s' % paths )
                return
            df = self.concat_dfs(dfs)
        else:
            gen = self.iter_read_paths(paths, columns=columns)
            lst = []
            for idx, x in enumerate(gen):
                lst.append(x)
                if not idx % concat_size:
                    df = self.concat_dfs(lst)
                    lst = [df]

            df = self.concat_dfs(lst)

        return df

    def get_table_daterange(self, dirpath):
        list_folder_names = lambda path: [int(os.path.split(p)[-1]) for p in self.iter_subfolders(path)]
        
        year_max = max(list_folder_names(dirpath))
        month_max = max(list_folder_names('%s/%d' % (dirpath, year_max) ))
        day_max = max(list_folder_names('%s/%d/%02d' % (dirpath, year_max, month_max) ))
        hour_max = max(list_folder_names('%s/%d/%02d/%02d' % (dirpath, year_max, month_max, day_max)   ))

        year_min = min(list_folder_names(dirpath))
        month_min = min(list_folder_names('%s/%d' % (dirpath, year_min) ))
        day_min = min(list_folder_names('%s/%d/%02d' % (dirpath, year_min, month_min) ))
        hour_min = min(list_folder_names('%s/%d/%02d/%02d' % (dirpath, year_min, month_min, day_min)   ))

        max_date = datetime(year=year_max, month=month_max, day=day_max, hour=hour_max)
        min_date = datetime(year=year_min, month=month_min, day=day_min, hour=hour_min)
        
        return min_date, max_date


    def read_dir_daterange(self, dirpath, columns=None, categories=None, start_date=None, end_date=None):
        days = pd.date_range(start=start_date, end=end_date, freq='D').to_series().apply(lambda x: x.strftime('%Y/%m/%d')).ravel()
        paths = [join(dirpath, "%s/*%s" % (i, self.suffix)) for i in days]

        df = self.read_paths(paths, columns=columns)
        return df

    def read_by_daterange(self, dirpath, columns=None, categories=None, start_date=None, end_date=None):
        datapaths = pd.date_range(start_date, end_date, freq='D').to_series().apply(lambda x:join(dirpath, x.strftime('%Y/%m/%d')) ).ravel()
        
        dfs = list() 
        for d in datapaths:
            logger.info('read dir :%s' % d)
            try:
                dfs.append(self.read_dir(d, columns=columns))
            except Exception as e:
                logger.warn('Error on reading %s, error:%s' % (d, e) )

        if len(dfs):
            logger.info('concating files...')
            r = self.concat_dfs(dfs)
            return r

        logger.info('No file found under daterange dir :%s' % dirpath)



