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


class FSClient(BaseClient):

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


