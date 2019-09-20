# -*- encoding: utf-8 -*-
"""
@File    : projectExecutor.py
@Time    : 19/8/2019 11:19
@Author  : liyang

工程执行器
"""

from classInit.mongodbSetting import mongo
from . import spider
from classInit.ETLTool import Transformer


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
                # self.saveDataToDB(data)
                print('SmartCrawler end!!!')
            # 为数据清洗模块
            elif module_type == 'ETLTask':
                # 迭代模块的各个工具（ETLTool）
                # 生成器生成数据
                ges = {}
                # 生成器生成标志符
                for tool in module.AllETLTools:
                    type = str(tool).split('.')[2]
                    # result为返回值
                    if type == 'Generator':
                        # 接受生成器返回的数据
                        # 如果数从文本生成
                        # if isinstance(tool,Generator.TextGE):
                        #     url = tool.generate()
                        # else:
                        ges[tool.Column] = tool.generate()
                    elif type == 'Transformer':
                        # 如果类型为DeleteTF，删除该列
                        if isinstance(tool,Transformer.DeleteTF):
                            tool.transform(ges, self.project)
                        ges[tool.Column] = tool.transform(ges, self.project)
                        print(ges[tool.Column])
                    elif type == 'Executor':
                        pass
                    elif type == 'Filter':
                        pass

                print('ETLTASK end!!!')
