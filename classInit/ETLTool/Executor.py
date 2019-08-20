# -*- encoding: utf-8 -*-
"""
@File    : Executor.py
@Time    : 19/8/2019 08:59
@Author  : liyang

执行器
"""
import sys

sys.path.append('../')
from classInit.ETLTask import ETLTool


class Executor(ETLTool):
    '''ETLTool的执行类组（在xml文件中为Group="Executor"）

    '''

    def execute(self, data):
        pass

    def process(self, data):
        for r in data:
            self.execute(r)
            yield r


def create(item):
    '''
    类实列化
    :param item: 待实例化的类名
    :return: 实例化后的类（对象）
    '''
    return eval('%s()' % item)


class EtlEX(Executor):
    pass
    # def execute(self, datas):
    #     subetl = self.__proj__.modules[self.ETLSelector]
    #     for data in datas:
    #         if spider.IsNone(self.NewColumn):
    #             doc = data.copy()
    #         else:
    #             doc = {}
    #             extends.MergeQuery(doc, data, self.NewColumn + " " + self.Column)
    #         result = (r for r in generate(subetl.AllETLTools, [doc]))
    #         count = 0
    #         for r in result:
    #             count += 1
    #             print(r)
    #         print(count)
    #         yield data


class TableEX(Executor):
    def __init__(self):
        super(TableEX, self).__init__()
        self.Table = 'Table'

    def execute(self, data):
        tables = self.__proj__.tables
        tname = self.Table
        if tname not in tables:
            tables[tname] = []
        for r in data:
            tables[tname].append(r)
            yield r


class SaveFileEX(Executor):
    def __init__(self):
        super(SaveFileEX, self).__init__()
        self.SavePath = ''

    def execute(self, data):
        pass
        # save_path = extends.Query(data, self.SavePath)
        # (folder, file) = os.path.split(save_path)
        # if not os.path.exists(folder):
        #     os.makedirs(folder)
        # urllib.request.urlretrieve(data[self.Column], save_path)


class DbEX(Executor):
    pass
