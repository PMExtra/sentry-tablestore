#!/usr/bin/env python
"""
sentry-nodestore-tablestore
===========================

A Sentry extension to support Alicloud Tablestore (OTS) as a node storage backend.
"""
from setuptools import setup, find_packages

with open("README.md", "r") as readme:
    long_description = readme.read()

install_requires = [
    'tablestore>=5.2.1',
    'sentry>=21.9.0',
]

setup(
    name='sentry-nodestore-tablestore',
    version='1.0.0-alpha',
    author='PM Extra <pm@jubeat.net>',
    author_email='pm@jubeat.net',
    url='https://github.com/PMExtra/sentry-nodestore-tablestore',
    description='A Sentry extension to support Alicloud Tablestore (OTS) as a node storage backend.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    license='Apache-2.0',
    zip_safe=False,
    install_requires=install_requires,
    include_package_data=True,
    download_url='https://github.com/PMExtra/sentry-nodestore-tablestore',
    classifiers=[
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development'
    ],
    project_urls={
        'Bug Tracker': 'https://github.com/PMExtra/sentry-nodestore-tablestore/issues',
        'CI': 'https://github.com/PMExtra/sentry-nodestore-tablestore/actions',
        'Source Code': 'https://github.com/PMExtra/sentry-nodestore-tablestore',
    },
)