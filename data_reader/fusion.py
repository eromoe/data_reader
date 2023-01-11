# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import

from datetime import datetime
import time
import numpy as np
from .libs import pd

class FusionClient(object):
    def __init__(self, mysql=None, hdfs=None, s3=None):
        self.mysql = mysql
        self.hdfs = hdfs
        self.s3 = s3

    def read_table(self, table, columns, start_year=None, conditions=None):
        dfs = list()

        if self.s3:
            df1 = self.s3.read_table(table, columns=columns, start_year=start_year)
            dfs.append(df1)
        elif self.hdfs:
            df1 = self.hdfs.read_table(table, columns=columns, start_year=start_year)
            dfs.append(df1)
        else:
            pass

        if self.mysql:
            n = datetime.now()
            datetime(n.year, n.month, n.day)
            time_start = int(time.mktime(n.timetuple()) * 1000)

            df2 = self.mysql.read_table(table, columns=columns, time_start=time_start, conditions=conditions)
            dfs.append(df2)

        df = pd.concat(dfs)

        return df