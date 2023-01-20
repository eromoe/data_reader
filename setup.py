# -*- coding: utf-8 -*-
# @Author: mithril
# @Date:   2016-05-09 13:54:41
# @Last Modified by:   mithril
# @Last Modified time: 2016-05-09 13:56:23


import sys
from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    install_requires = f.read().split('\\n')

setup(
    name='data_reader',
    version='0.1',

    description='data_reader by Mithril ',
    long_description=long_description,

    author='Mithril',

    classifiers=[
        'Development Status :: 1 - Beta',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',

        'Intended Audience :: Developers',
        'Operating System :: OS Independent',

        "License :: GPLv3",

        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Tools',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],

    keywords='tools',

    install_requires=install_requires,

    py_modules=['data_reader'],

)
