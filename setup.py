#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.md').read()
history = open('HISTORY.md').read()

setup(
    name='voron',
    version='0.2.0',
    description='Tool for converting web server logs into statsd/graphite time series.',
    long_description=readme + '\n\n' + history,
    author='Kirill Borisov',
    author_email='lensvol@gmail.com',
    url='https://github.com/lensvol/voron',
    packages=[
        'voron',
    ],
    package_dir={'voron': 'voron'},
    include_package_data=True,
    install_requires=[
        'pyinotify'
    ],
    license="BSD",
    zip_safe=False,
    keywords='voron',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ],
    test_suite='tests',
)
