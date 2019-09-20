# -*- encoding: utf-8 -*-
"""
@File    : Project.py
@Time    : 19/8/2019 08:55
@Author  : liyang

工程初始化
"""


class Project():
    def __init__(self):
        self.modules = {}
        self.tables = {}
        self.connectors = {}
        self.__defaultdict__ = {}
