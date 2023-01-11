# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import

import sys
import os
import re
from posixpath import join
from datetime import datetime
from pathlib import Path
from collections import Counter

from .libs import pd
from .utils import safepath, concat_with_cat
from .log import build_logger

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class BaseClient(object):
    
    
    def __init__(self, categories=None, suffix=None, colnames=None, debug=False, date_regex=None, **kwargs):
        # can't use or, need allow []
        self.categories = categories
        self.suffix = suffix
        self.date_regex = date_regex
        
        # use for csv
        self.colnames = colnames
        
        if debug:
            self.logger = build_logger(level='DEBUG')
        else:
            self.logger = build_logger(level='INFO')

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
                self.logger.exception('Concat error with categories: %s', categories)
                safe_cats = [c for c in categories if c in dfs[0].columns]

                if len(safe_cats)>0:
                    self.logger.warn('Safe catcols:%s doesnt match categories:%s' % (safe_cats, categories))
                    return concat_with_cat(dfs, safe_cats)
                else:
                    self.logger.warn('No cat match categories:%s, concat without categories' % (categories,))
                    return pd.concat(dfs).reset_index(drop=True)

    def get_suffix(self, path):
        assert '.' in path
        return '.' + path.lower().rsplit('.', 1)[-1]
        
    def guess_suffix(self, dirpath):
        suffixes = [self.get_suffix(p) for p in self.ls(dirpath) if Path(p).is_file()]
        c = Counter(suffixes)
        return c.most_common(1)[0][0]
        
    def has_header(self, path):
        '''
        only support on local filesystem
        '''
        raise NotImplementedError

    def read(self, path, columns=None, **kwargs):
        if self.suffix:
            assert self.validate_path(path)

        suffix = self.suffix or self.get_suffix(path)
        if suffix == '.parquet':
            x = pd.read_parquet(path, columns=columns, **kwargs)
        elif suffix == '.csv' or suffix == '.zip':
            x = pd.read_csv(path, names=self.colnames if self.colnames else None, usecols=columns, **kwargs)
        elif suffix == '.xlsx' or suffix == 'xls':
            x = pd.read_excel(path, columns=columns, **kwargs)
        elif suffix == '.pkl':
            x = pd.read_pickle(path)

        # if isinstance(x.index, pd.MultiIndex):
        #     x = x.reset_index()

        if self.categories:
            for c in self.categories:
                if c in x.columns:
                    x[c] = x[c].astype('category')

        return x
        
    def read_dir(self, dirpath, columns=None, categories=None):
        dfs = list()

        for p in self.ls(dirpath):
            self.logger.debug(dirpath)
            if self.validate_path(p):
                dfs.append(self.read(p, columns=columns))
        
        if len(dfs):
            return self.concat_dfs(dfs)

        raise FileNotFoundError('No file found under dir :%s' % dirpath)


    def iter_read_wildcard_path(self, path, columns=None, categories=None):
        path = safepath(path, 'unix')
        for i in self.glob(path):
            self.logger.debug(i)
            yield self.read(i, columns=columns)

    def read_wildcard_path(self, path, columns=None, categories=None):
        path = safepath(path, 'unix')
        dfs = []
        for i in self.glob(path):
            self.logger.debug(i)
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
                    self.logger.debug('Read wildcard path: %s' % path)
                    x = self.read_wildcard_path(path, columns=columns)
                elif path.endswith('/') or path.endswith('\\'):
                    self.logger.debug('Read dir: %s' % path )
                    x = self.read_dir(path, columns=columns)
                    if x is None:
                        self.logger.debug('No files under: %s' % path) 
                        continue
                else:
                    self.logger.debug(path)
                    x = self.read(path, columns=columns)

                yield x
            except FileNotFoundError as e:
                self.logger.debug(e)
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

    def iter_date_path(self, dirpath, start, end, date_regex=None, freq=None):
        '''
        read filename with date under a dirpath
        '''
        
        dirpath = safepath(dirpath, 'unix')
        
        date_regex = date_regex or self.date_regex
        date_pt = re.compile(date_regex)
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
        
        for p in self.ls(dirpath):
            r = date_pt.findall(Path(p).name)
            if len(r):
                curdate = pd.to_datetime(r[0])
                if curdate >=start and curdate < end:
                    yield p

    def iread_daterange(self, dirpath, start, end, date_regex=None, freq='D', columns=None):
        paths = (p for p in self.iter_date_path(dirpath, start, end, date_regex=date_regex))
        dfs = self.iter_read_paths(paths, columns=columns)
        return dfs
      
    def read_daterange(self, dirpath, start, end, date_regex=None, freq='D', columns=None):
        dfs = self.iread_daterange(dirpath, start, end, date_regex=date_regex, columns=columns)
        df = self.concat_dfs(dfs) 
        return df

    # def read_file_daterange(self, filename_tpl, date_fmt=None, freq='D', start_date=None, end_date=None, columns=None,):
    #     days = pd.date_range(start=start_date, end=end_date, freq=freq).to_series().apply(lambda x: x.strftime(date_fmt)).ravel()
        
    #     paths = [join(filename_tpl % (i, self.suffix)) for i in days]

    #     df = self.read_paths(paths, columns=columns)
    #     return df

    # def read_dir_daterange(self, dirpath, columns=None, start_date=None, end_date=None):
    #     days = pd.date_range(start=start_date, end=end_date, freq='D').to_series().apply(lambda x: x.strftime('%Y/%m/%d')).ravel()
    #     paths = [join(dirpath, "%s/*%s" % (i, self.suffix)) for i in days]

    #     df = self.read_paths(paths, columns=columns)
    #     return df

    # def read_by_daterange(self, dirpath, columns=None, start_date=None, end_date=None):
    #     datapaths = pd.date_range(start_date, end_date, freq='D').to_series().apply(lambda x:join(dirpath, x.strftime('%Y/%m/%d')) ).ravel()
        
    #     dfs = list() 
    #     for d in datapaths:
    #         self.logger.info('read dir :%s' % d)
    #         try:
    #             dfs.append(self.read_dir(d, columns=columns))
    #         except Exception as e:
    #             self.logger.warn('Error on reading %s, error:%s' % (d, e) )

    #     if len(dfs):
    #         self.logger.info('concating files...')
    #         r = self.concat_dfs(dfs)
    #         return r

    #     self.logger.info('No file found under daterange dir :%s' % dirpath)



