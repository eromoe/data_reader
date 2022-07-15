# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import

import os

from itertools import chain
import fnmatch

import s3fs
import pandas as pd

from .base import BaseClient


class S3Client(BaseClient):
    
    def __init__(self, *args, **kwargs):
        super(S3Client, self).__init__(*args, **kwargs)
        self.conn = s3fs.S3FileSystem()

    def iter_ls(self, dirpath, **kwargs):
        for i in self.conn.ls(dirpath, **kwargs):
            yield 's3://' + i

    def ls(self, dirpath, **kwargs):
        return list(self.iter_ls(dirpath, **kwargs))

    def iter_subfolders(self, dirpath):
        for i in self.conn.ls(dirpath, detail=True, refresh=True):
            if i['StorageClass'] == 'DIRECTORY':
                yield 's3://' + i['Key']

    def glob(self, wildcard, **kwargs):
        ''' s3fs  glob doesn't support yielding
        '''

        if wildcard.count('*') == 1:
            dirpath = os.path.dirname(wildcard)
            for i in self.iter_ls(dirpath, **kwargs):
                if fnmatch.fnmatch(i, wildcard):
                    yield i
        else:
            dirpath, subpath = wildcard.split('*', 1)
            for i in self.iter_ls(dirpath, **kwargs):
                if fnmatch.fnmatch(i+subpath, wildcard):
                    yield from self.glob(i+subpath)


if __name__ == '__main__':
    
    s3 = S3Client()
    s3.read_path('s3://xxx/test/db77f51b-156b-4814-9fde-066ba1d0e202')
    paths = ['s3://xxx/1.gz.parquet', 's3://xxx/2.gz.parquet']
    columns=["store_id", "product_id", "store_product_quantity_old", "store_product_quantity_new",  "time_create"]
    path = 's3://x/3/*.parquet'
    df = s3.read_wildcard_path(path, columns=columns)
    s3.conn.ls(os.path.dirname(path))
