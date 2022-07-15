# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import, division

from os.path import join
import pandas as pd

from hdfs3 import HDFileSystem

from .base import BaseClient


class HdfsClient(BaseClient, HDFileSystem):
    
    def __init__(self, *args, **kwargs):
        super(HdfsClient, self).__init__(*args, **kwargs)



# from hdfs import InsecureClient

# class HdfsInsecureReader(InsecureClient):

#     def __init__(self, host=None, port=None, timeout=10000):
#         super(HdfsInsecureReader, self).__init__('http://%s:%s' % (host, port), timeout=timeout)

#     def read_df(self, path, columns=None):
#         with self.read(path) as reader:
#             x = reader.read()
#             f = BytesIO(x)
#             x = pd.read_parquet(f, columns=columns)
#         return x

#     def write_df(self, df, path):
#         with self.write(path) as writer:
#             df.to_parquet(writer)

#     def read_paths(self, paths, columns=None):
#         df = pd.DataFrame()
#         for path in paths:
#             x = self.read(path, columns)
#             df = pd.concat((df, x))

#         return df