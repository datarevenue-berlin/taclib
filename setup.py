#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
import versioneer
from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

with open('requirements.txt' 'r') as fp:
    requirements = fp.readlines()

test_requirements = ['pytest']

setup(
    author="Data Revenue GmbH",
    author_email='alan@datarevenue.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Task as containers made easy with kubernetes and luigi",
    entry_points={
        'console_scripts': [
            'taclib=taclib.cli:main',
        ],
    },
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='taclib',
    name='taclib',
    packages=find_packages(include=['taclib']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/datarevenue-berlin/taclib',
    version = versioneer.get_version(),
    cmdclass = versioneer.get_cmdclass(),
    zip_safe=False,
)
