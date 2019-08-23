# -*- encoding: utf-8 -*-
"""
@File    : Generator.py
@Time    : 19/8/2019 08:55
@Author  : liyang

生成器
"""

import sys

sys.path.append('../')
from classInit.ETLTask import ETLTool


class Generator(ETLTool):
    '''ETLTool的生成类组（在xml文件中为"Group="Generator"）

        生成数据清洗的工具。eg.ETLTool类型为RangeGE
    '''

    def __init__(self):
        # 继承父类的初始化
        super(Generator, self).__init__()
        # 初始化合并类型为append
        self.MergeType = 'Append'
        # 初始化位置为0
        self.Position = 0

    def generate(self, generator):
        pass

    def process(self, generator):
        pass
        # if generator is None:
        #     return self.generate(None)
        # else:
        #     if self.MergeType == 'Append':
        #         return extends.Append(generator, self.process(None))
        #     elif self.MergeType == 'Merge':
        #         return extends.Merge(generator, self.process(None))
        #     else:
        #         return extends.Cross(generator, self.generate)


def create(item):
    '''
    类实列化
    :param item: 待实例化的类名
    :return: 实例化后的类（对象）
    '''
    return eval('%s()' % item)


class RangeGE(Generator):
    '''数据清洗任务中数据清洗工具（ETLTool）类型（Type）为RangeGE的处理。

    继承于生成类，生成区间数
    '''

    def __init__(self):
        super(RangeGE, self).__init__()
        # 初始化设置间隔数
        self.Interval = '1'
        # 初始化设置数的最大值
        self.MaxValue = '1'
        # 初始化最小值
        self.MinValue = '1'

    def generate(self):
        items = []
        # 生成由最小值到最大值，间隔为Interval的int序列
        interval = int(self.Interval)
        maxvalue = int(self.MaxValue)
        minvalue = int(self.MinValue)
        # 包括最大值
        for i in range(minvalue, maxvalue + 1, interval):
            item = {self.Column: round(i, 5)}
            items.append(item)
        return items
        #     yield item


class EtlGE(Generator):
    '''子任务生成

    '''
    # def generate(self, data):
    #     subetl = self.__proj__.modules[self.ETLSelector]
    #     for r in generate(subetl.AllETLTools):
    #         yield r
    pass


class TextGE(Generator):
    '''从文本生成。

        直接导入url,若导入url必须有'https://'或'http://'
    '''

    def __init__(self):
        super(TextGE, self).__init__()
        self.Content = ''
    def generate(self):
        result = []
        self.arglists = [r.strip() for r in self.Content.split('\n')]
        for i in range(self.Position, len(self.arglists)):
           result.append({self.Column: self.arglists[i]})
        return result
            # yield


class BfsGE(Generator):
    pass


class FolderGE(Generator):
    pass


class TableGE(Generator):
    pass
