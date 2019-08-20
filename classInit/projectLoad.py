# -*- encoding: utf-8 -*-
"""
@File    : projectLoad.py
@Time    : 18/8/2019 18:55
@Author  : liyang

XML工程导入模块
"""
import copy
import re
import xml.etree.ElementTree as ET

from classInit import ETLTask
from classInit import Project
from classInit import SmartCrawler
from classInit.ETLTool import Executor
from classInit.ETLTool import Filter
from classInit.ETLTool import Generator
from classInit.ETLTool import Transformer

# value为int的参数
intattrs = re.compile('Max|Min|Count|Index|Interval|Position')
# value为bool的参数
boolre = re.compile('^(One|Can|Is)|Enable|Should|Have|Revert')
rescript = re.compile('Regex|Number')


def SetAttr(etl, key, value):
    '''通过传入参数设置标签参数

    :param etl: 标签对象
    :param key: 标签参数key
    :param value: 标签参数value
    :return:
    '''
    # 如果key为'Group'或'Type'，返回空
    if key in ['Group', 'Type']:
        return

    # 根据正则表达式搜索key
    if intattrs.search(key) is not None:
        # 将对应的value转换为int
        try:
            t = int(value)
            # 设置对象属性value为t
            setattr(etl, key, t)
        except ValueError:
            # 返回值错误
            print('it is a ValueError')
            setattr(etl, key, value)
    elif boolre.search(key) is not None:
        setattr(etl, key, True if value == 'True' else False)
    else:
        setattr(etl, key, value)


def get_type_name(obj):
    '''
    # <class 'classInit.ETLTool.Generator.RangeGE'>
    :param obj: 传入类，eg.<class 'etl.PythonTF'>
    :return:类的类型 eg. PythonTF
    '''
    s = str(obj.__class__)
    p = s.split('.')
    r = p[-1].split('\'')[0]
    return r


def etl_factory(item, proj):
    '''将item及ietm的参数添加到工程

    :param item:传入项目对象
    :param proj:传入工程对象
    :return:item对象
    '''

    if isinstance(item, str):
        # item的类型
        type = item[-2:]
        if type == 'GE':
            item = Generator.create(item)
        elif type == 'TF':
            item = Transformer.create(item)
        elif type == 'EX':
            item = Executor.create(item)
        elif type == 'FT':
            item = Filter.create(item)

    else:
        item = item
    # 获取item类型

    name = get_type_name(item)
    # 添加name到工程工作字典,值为item的参数
    if name not in proj.__defaultdict__:
        proj.__defaultdict__[name] = copy.deepcopy(item.__dict__)
    return item


def GetChildNode(roots, name):
    '''获取子节点标签名为name的标签，并将该标签返回

    :param roots: 父节点
    :param name: 子节点标签名
    :return: 子节点标签（若有），None（若无）
    '''
    for etool in roots:
        if etool.get('Name') == name or etool.tag == name:
            return etool
    return None


def InitFromHttpItem(config, item):
    '''爬虫的http连接设置初始化

    :param config: 爬虫连接设置
    :param item:带添加设置的item
    '''
    httprib = config.attrib
    # 将参数由string格式转换为字典
    paras = SmartCrawler.Para2Dict(httprib['Parameters'], '\n', ':')
    # 获取item设置的header
    if 'User-Agent' in paras.keys():
        # xml文件中user-agent多了一个空格，eg.' : Mozilla/5.0 (X11'
        headers = paras['User-Agent'].strip()
        item.Headers = {'User-Agent': headers}
        # print(item.Headers )
    # 获取item的cookie
    # pass

    # 获取item的url
    item.Url = httprib['URL']
    # 获取item的post数据（若有）
    post = 'Postdata'
    if post in httprib:
        item.postdata = httprib[post]
    else:
        item.postdata = None


def Project_LoadXml(path):
    '''导入xml工程文件

    :param path:工程文件路径
    :return:是否读取xml文件成功
    '''
    # 读取xml文件
    tree = ET.parse(path)
    # 初始化工程
    proj = Project.Project()

    def factory(obj):
        '''声明obj

        :param obj: 传入对象
        :return:etl_factory返回的obj对象
        '''
        return etl_factory(obj, proj)

    # 获取根节点
    root = tree.getroot()
    # 获取doc节点
    root = root.find('Doc')
    # 迭代遍历doc节点
    for etool in root:
        # 获取children标签
        if etool.tag == 'Children':
            # 获取该标签类型
            etype = etool.get('Type')
            # 获取该标签名
            name = etool.get('Name')
            # 标签为数据清理工具
            if etype == 'SmartETLTool':
                # 生成ETLTask任务，并创建ETLTask对象
                etltool = factory(ETLTask.ETLTask())
                # 迭代SmartETLTool标签
                for m in etool:
                    if m.tag == 'Children':
                        # 获取标签类型
                        type = m.attrib['Type']
                        # 生成type型类，并创建该type类型对象
                        etl = factory(type)
                        # 添加对象的工程为文件初始工程
                        etl.__proj__ = proj
                        # 迭代标签参数
                        for att in m.attrib:
                            # 传入标签对象，标签参数名，标签参数值
                            SetAttr(etl, att, m.attrib[att])
                        # 将标签参数添加到ETLTask对象中添加标签
                        etltool.AllETLTools.append(etl)
                # 将此SmartETLTool任务添加到工程
                proj.modules[name] = etltool
            # 标签为爬虫
            elif etype == 'SmartCrawler':
                # 生成crawler对象,并将该对象加入到工程对象proj
                crawler = factory(SmartCrawler.SmartCrawler())
                # 生成crawler的HttpItem对象并将该对象加入到工程对象proj
                crawler.HttpItem = factory(SmartCrawler.HTTPItem())
                crawler.Name = etool.attrib['Name']
                crawler.Url = etool.attrib['URL']
                # 爬虫的url请求数目（one和list）
                crawler.IsMultiData = etool.attrib['IsMultiData']
                # 爬虫xpath的主xpath(若有）
                if 'RootXPath' in etool.attrib:
                    crawler.RootXPath = etool.attrib['RootXPath']
                else:
                    crawler.RootXPath = ''
                # 获取子节点，如果子节点的标签为HttpSet，代表http连接的设置
                httpconfig = GetChildNode(etool, 'HttpSet')
                # 对爬虫的http设置进行初始化
                InitFromHttpItem(httpconfig, crawler.HttpItem)
                # 对爬虫的login标签进行设置（若有）
                login = GetChildNode(etool, 'Login')
                if login is not None:
                    crawler.Login = factory(SmartCrawler.HTTPItem())
                    InitFromHttpItem(login, crawler.Login)
                # 获取该节点的所有爬虫标签的参数设置，每个标签为一个spider.CrawItem()
                crawler.CrawItems = []
                # 遍历etool的children标签
                for child in etool:
                    if child.tag == 'Children':
                        # 生成一个爬虫item
                        crawitem = factory(SmartCrawler.CrawItem())
                        crawitem.Name = child.attrib['Name']
                        crawitem.XPath = child.attrib['XPath']
                        crawler.CrawItems.append(crawitem)
                # 将SmartCrawler任务添加到proj中
                proj.modules[name] = crawler
        # 如果标签类型为数据库连接
        elif etool.tag == 'DBConnections':
            pass
            # for tool in etool:
            #     if tool.tag == 'Children':
            #         connector = extends.EObject()
            #         # 为数据库连接添加属性
            #         for att in tool.attrib:
            #             SetAttr(connector, att, tool.attrib[att])
            #         proj.connectors[connector.Name] = connector
    print('load project success')
    return proj
