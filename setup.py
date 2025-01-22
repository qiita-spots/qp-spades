#!/usr/bin/env python

# -----------------------------------------------------------------------------
# Copyright (c) 2021, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------
from setuptools import setup
from glob import glob

__version__ = "2021.05"

classes = """
    Development Status :: 5 - Beta
    License :: OSI Approved :: BSD License
    Topic :: Scientific/Engineering :: Bio-Informatics
    Topic :: Software Development :: Libraries :: Application Frameworks
    Topic :: Software Development :: Libraries :: Python Modules
    Programming Language :: Python
    Programming Language :: Python :: 3.6
    Programming Language :: Python :: Implementation :: CPython
    Operating System :: POSIX :: Linux
    Operating System :: MacOS :: MacOS X
"""

with open('README.rst') as f:
    long_description = f.read()

classifiers = [s.strip() for s in classes.split('\n') if s]

setup(name='qp-spades',
      version=__version__,
      long_description=long_description,
      license="BSD",
      description='Qiita Plugin: qp-spades',
      author="Qiita development team",
      author_email="qiita.help@gmail.com",
      url='https://github.com/qiita-spots/qp-spades',
      packages=['qp_spades'],
      package_data={'qp_spades': [
        'support_files/*',
        'support_files/raw_data/*']},
      scripts=glob('scripts/*'),
      extras_require={'test': ["pytest", "pep8"]},
      install_requires=['click >= 3.3', 'future', 'pandas >= 0.15',
                        'pyreadline3',
                        'qiita_client @ https://github.com/qiita-spots/'
                        'qiita_client/archive/master.zip'],
      classifiers=classifiers
      )
