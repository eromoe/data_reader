# -*- coding: utf-8 -*-
# @Author: mithril


from __future__ import unicode_literals, print_function, absolute_import

import os
from posixpath import join
from os.path import isdir
from glob import glob

from .base import BaseClient


def lsr(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))

def ls(directory):
    for path in os.listdir(directory):
        yield os.path.abspath(os.path.join(directory, path))




import zipfile
import csv

class FSClient(BaseClient):
    
    def has_header(self, path):
        if path.endswith('.zip'):
            with zipfile.ZipFile(path) as zipf:
                inside_path = path.replace('.zip', '.csv').rsplit('/', 1)[-1]
                with zipf.open(inside_path, "r") as f:
                    return csv.Sniffer().has_header(f.read(2048).decode('utf-8'))
        else:
            with open(path, "r") as f:
                return csv.Sniffer().has_header(f.read(2048).decode('utf-8'))

    def ls(self, *args, **kwargs):
        return ls(*args, **kwargs)

    def glob(self, *args, **kwargs):
        return glob(*args, **kwargs)

    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    def iter_subfolders(self, path):
        for p in os.listdir(path):
            if isdir(p):
                yield join(path, p)


if __name__ == "__main__":
    from glob import glob
    paths = list(glob('e:/orderitems/2018/*.parquet'))
    fs = FSClient()
    df = fs.read_paths(paths[:2])


