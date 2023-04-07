#!/usr/bin/python3.8
from setuptools import find_packages
from setuptools import setup

setup (
	name='codechallenge20230218',
	version='1.0',
	install_requires=['apache-beam[gcp]==2.45.0','geopy==2.3.0',],
	packages=find_packages(exclude=['notebooks']),
	py_modules=['config'],
	include_package_data=True,
	description='Coding Challenge'
)
