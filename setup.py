"""Setup file for aqara package."""
from setuptools import setup, find_packages

setup(name='etlpy',
 version='1.0',
 description='Super ETL tool',
 url='https://github.com/ferventdesert/etlpy',
 author='Yiming Zhao',
 author_email= 'buptzym@qq.com',
 license='MIT',
 packages=['etlpy'],
 keywords = ['spider', 'stream', 'dsl'],
 install_requires=['lxml', 'pyquery','requests']
)