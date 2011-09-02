#! /usr/bin/env python

from setuptools import setup
from distutils.extension import Extension

VERSION = '0.0.1'

DESCRIPTION = """\
A gevent (http://www.gevent.org) adaption of the asynchronous MySQL driver
from the Concurrence framework (http://opensource.hyves.org/concurrence)
"""


setup(
    name="gevent-MySQL",
    version=VERSION,
    license="New BSD",
    description=DESCRIPTION,
    package_dir={'': 'lib'},
    packages=['geventmysql'],
    install_requires=["gevent"],
    ext_modules=[Extension("geventmysql._mysql",
        ["lib/geventmysql/geventmysql._mysql.c"])]
)
