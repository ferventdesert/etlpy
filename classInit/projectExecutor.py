# -*- encoding: utf-8 -*-
"""
@File    : projectExecutor.py
@Time    : 19/8/2019 11:19
@Author  : liyang

工程执行器
"""

from classInit.mongodbSetting import mongo
from . import spider


class projExecute():

    def __init__(self, project):
        '''初始化工程

        :param project: 传入工程项目
        '''
        self.project = project
        self.modules = project.modules
        self.tables = project.tables
        self.connectors = project.connectors
        self.__defaultdict__ = project.__defaultdict__
        # print(self.__defaultdict__)

    def saveDataToDB(self, data):
        '''将数据存入MongoDB

        :param data: json格式的数据
        :return:
        '''
        # 初始化mongo class
        conn = mongo()
        # 建立MongoDB连接
        c = conn.connect('139.196.85.202', 27017, 'test1', 'contents')
        # 数据插入
        conn.insert_one(data)

    def projectFunction(self):
        '''
        迭代project的modules，并顺序执行相应功能（item）
        :return:
        '''
        for module_name, module in self.project.modules.items():
            module_type = str(module).split('.')[1]
            # 如果为爬虫模块
            if module_type == 'SmartCrawler':

                # 爬虫提取的xpath规则
                # items = spider.setCrawItems(module.CrawItems)
                # 爬虫headers
                headers = spider.setHttpItem(module.HttpItem)
                # 爬虫返回的数据
                html_data = spider.getURLdata(module.Url, headers)
                # 根据xpath规则处理html,
                data = spider.processHtml(html_data, module.RootXPath, module.CrawItems)
                # 将数据存入MongoDB
                self.saveDataToDB(data)

            # 为数据清洗模块
            elif module_type == 'SmartETLTool':

                pass
                # 迭代模块的各个工具（ETLTool）
                print(module.AllETLTools)
                for tool in module.AllETLTools:
                    type = str(tool).split('.')[2]
                    # result为返回值
                    if type == 'Generator':
                        result = tool.generate()
                    elif type == 'Transformer':
                        result = tool.transform(result, self.project)

                    elif type == 'Executor':
                        pass
                    elif type == 'Filter':
                        pass
                with open('outFile/ETLTool.json', 'a', encoding='utf-8')as f:
                    for i in result:
                        f.write(str(i))
                print('ETLTASK')
