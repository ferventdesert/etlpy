# -*- encoding: utf-8 -*-
"""
@File    : spider.py
@Time    : 19/8/2019 14:19
@Author  : liyang

工程的爬虫模块
"""
import requests
from lxml import etree


def setCrawItems(CrawItems):
    '''
    设置爬虫规则(xpath)
    :param CrawItems: 爬虫规则
    :return:
    '''
    items = []
    for item in CrawItems:
        name = str(item.Name)
        xpath = str(item.XPath)
        i = [name, xpath]
        items.append(item)
    return items


def setHttpItem(HttpItem):
    '''
    设置http访问参数
    :param HttpItem:
    :return:
    '''
    return HttpItem.Headers


def getURLdata(url, headers):
    """根据url爬取html

    :param url: 爬取的url
    :return: html文本（正常），"get URL data error"（异常）
    """
    try:

        r = requests.get(url, headers=headers, timeout=30)
        # 如果状态码不是200 则应发HTTPError异常
        r.raise_for_status()
        # 设置编码
        # r.encoding = r.apparent_encoding
        return r.text
    except:
        return "get URL data error"


def saveData(title, data):
    """储存html至指定文件夹

    :param title: 文件标题
    :param data: 文件内容
    :return: 无
    """
    file = 'htmlFile/' + title + '.html'
    with open(file, 'w', encoding='utf-8')as f:
        f.write(data)


def processHtml(html, RootXPath, items):
    """对html文件进行XPath分析提取

    :param html: 传入的html文本
    :param RootXPath: xpath的主xpath(查询xpath=RootXPath+xpath)
    :param items: 所需提取的字段（list），根据item.Name，item.xpath获取标题和xpth
    :return: result (html对xpath的数据提取，list[dict{name:data}]）
    """
    html = etree.HTML(html)
    # result = etree.tostring(html)
    result = {}
    for i in items:

        # 对xpath出现‘#text[1]’进行特殊处理
        xpath = (RootXPath + i.XPath)
        if xpath.find('#text[1]'):
            xpath = xpath.replace('#text[1]', '/text()')
        data = []
        for d in html.xpath(xpath):
            # 如果返回为字符串，不改变
            if isinstance(d, str):
                data.append(d)
            # 否则提取元素的string
            else:
                data.append(d.xpath('string()'))
        result[i.Name] = data
    return result
