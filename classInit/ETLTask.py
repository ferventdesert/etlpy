# -*- encoding: utf-8 -*-
"""
@File    : ETLTask.py
@Time    : 19/8/2019 08:57
@Author  : liyang

数据清洗模块
"""


class ETLTask():
    '''SmartETLTool（数据清洗）的子任务

    '''

    def __init__(self):
        self.AllETLTools = []


class ETLTool():
    def __init__(self):
        self.Enabled = True
        self.Column = ''

    def process(self, data):
        return data

    def init(self):
        pass
