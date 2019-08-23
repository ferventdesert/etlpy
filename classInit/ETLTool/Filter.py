# -*- encoding: utf-8 -*-
"""
@File    : Filter.py
@Time    : 19/8/2019 08:59
@Author  : liyang
"""
import sys

sys.path.append('../')
from classInit.ETLTask import ETLTool
import re


class Filter(ETLTool):
    '''ETLTool的过滤类组（在xml文件中为Group="Filter"）

    '''

    def __init__(self):
        super(Filter, self).__init__()
        self.Revert = False

    def filter(self, data):

        return True

    def process(self, data):
        for r in data:
            item = None
            if self.Column in r:
                item = r[self.Column]
            if item is None and self.__class__ != NullFT:
                continue
            result = self.filter(item)
            if result == True and self.Revert == False:
                yield r
            elif result == False and self.Revert == True:
                yield r


def create(item):
    '''
    类实列化
    :param item: 待实例化的类名
    :return: 实例化后的类（对象）
    '''
    return eval('%s()' % item)


class RegexFT(Filter):

    def init(self):
        self.Regex = re.compile(self.Script)
        self.Count = 1

    def filter(self, data):
        v = self.Regex.findall(data)
        if v is None:
            return False
        else:
            return self.Count <= len(v)


class RangeFT(Filter):

    def filter(self, item):
        f = float(item)
        return self.Min <= f <= self.Max


class RepeatFT(Filter):

    def init(self):
        self.set = set()

    def filter(self, data):
        if data in self.set:
            return False
        else:
            self.set.add(data)
            return True


class NullFT(Filter):
    '''空对象过滤器

    '''
    def filter(self, data):
        if data is None:
            return False
        if isinstance(data, str):
            return data.strip() != ''
        return True


class NumRangeFT(Filter):
    pass
