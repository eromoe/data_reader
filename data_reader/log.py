# -*- coding: utf-8 -*-
# @Author: mithril

from __future__ import unicode_literals, print_function, absolute_import, division


import logging
from logging.handlers import RotatingFileHandler
from os.path import join, abspath, dirname

try:
    from config import DEBUG
except:
    DEBUG=False


def build_logger(name=None, level=None, filename=None):
    #
    # logging.basicConfig(level=logging.DEBUG)
    # logging.getLogger('asyncio').setLevel(logging.ERROR)
    if not level:
        level = logging.DEBUG if DEBUG else logging.INFO
    else:
        level = getattr(logging, level)
        
    logger = logging.getLogger(name or __file__)
    logger.setLevel(level)

    logger.propagate = False # prevent log twice

    while logger.handlers:
        logger.handlers.pop()

    # formatter = logging.Formatter(
    #     '[%(levelname)s]'
    #     ' [%(asctime)s] : %(message)s '
    # )
    
    detail_formatter = logging.Formatter(
        '[%(levelname)s][%(pathname)s:%(lineno)d]'
        ' [%(asctime)s] %(message)s '
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(detail_formatter)
    logger.addHandler(stream_handler)

    if filename:
        file_handler = RotatingFileHandler(filename, maxBytes=1024*1024, backupCount=2)
        file_handler.setLevel(level)
        file_handler.setFormatter(detail_formatter)
        logger.addHandler(file_handler)

    return logger

logger = build_logger('data_reader')