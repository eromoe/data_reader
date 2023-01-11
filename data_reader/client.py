#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author: mithril
# Created Date: 2022-07-15 17:09:34
# Last Modified: 2022-12-30 14:12:41

import os
from .log import logger
from .filesystem import FSClient

try:
    from .s3 import S3Client
except ImportError as e:
    logger.warn('Import s3 client error:', e)

try:
    from .hdfs import HdfsClient
except ImportError as e:
    logger.warn('Import hdfs client error:', e)
    
    
def get_reader(name=None, suffix=None, colnames=None, categories=None, debug=False, **kwargs):
    if name == 's3':
        # print('Use S3Client')
        return S3Client(suffix=suffix, colnames=colnames, categories=categories, debug=debug, **kwargs)
    if name == 'hdfs':
        # print('Use HdfsClient')
        host = os.environ['HDFS_HOST']
        port = os.environ['HDFS_PORT']
        return HdfsClient(host, port)
    
    # print('Use Default Reader')
    return FSClient(suffix=suffix, colnames=colnames, categories=categories, debug=debug, **kwargs)