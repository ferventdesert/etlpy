# -*- encoding: utf-8 -*-
"""
@File    : mongodbSetting.py
@Time    : 20/8/2019 11:28
@Author  : liyang

工程的MongoDB设置及处理模块
"""
import pymongo


class mongo():
    def __init__(self):
        self.host = ''
        self.port = 27017
        self.client = ''
        self.database = ''
        self.collection = ''

    def connect(self, host, port, database, collection):
        '''建立MongoDB数据库连接

        :param host: 数据库地址
        :param port:数据库端口
        :param database:待插入数据的数据库
        :param collection:待插入数据的集合
        :return:
        '''
        self.host = host
        self.port = port
        self.database = database
        self.collection = collection
        try:
            self.client = pymongo.MongoClient(self.host, self.port)
        except:
            print('MongoDB connect error')

    def insert_one(self, data):
        '''向指定数据库的指定集合插入一条数据

        :param data: 待插入的数据
        :return:
        '''

        try:
            db = self.client[self.database]
            collection = db[self.collection]
            # with open('outFile/SmartCrawler.json', 'r', encoding='utf-8')as f:
            #     temp = json.loads(f.read())
            # print(temp)
            # collection = client['test'].contents
            result = collection.insert_one(data)
            print(result)
        except:
            print('Database insert_one data error')
