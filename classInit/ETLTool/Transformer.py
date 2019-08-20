# -*- encoding: utf-8 -*-
"""
@File    : Transformer.py
@Time    : 19/8/2019 08:58
@Author  : liyang

转换器
"""
import sys

sys.path.append('../')
from classInit.ETLTask import ETLTool
from classInit import spider
import os
import re
import json


class Transformer(ETLTool):
    '''ETLTool的转换类组（在xml文件中为 Group="Transformer"）

    '''

    def __init__(self):
        # 继承父类的初始化
        super(Transformer, self).__init__()
        # 初始化是否为多重返回数据为False
        self.IsMultiYield = False
        self.NewColumn = ''
        self.OneOutput = True
        self.OneInput = False

    def transform(self, data, project):
        pass

    def process(self, data, project):
        pass
        # if self.IsMultiYield:  # one to many
        #     for r in data:
        #         for p in self.transform(r):
        #             yield extends.MergeQuery(p, r, self.NewColumn)
        #     return
        # for d in data:  # one to one
        #     if self.OneOutput:
        #         if self.Column not in d or self.Column not in d:
        #             yield d
        #             continue
        #         item = d[self.Column] if self.OneInput else d
        #         res = self.transform(item)
        #         key = self.NewColumn if self.NewColumn != '' else self.Column
        #         d[key] = res
        #     else:
        #         self.transform(d)
        #     yield d


def create(item):
    '''
    类实列化
    :param item: 待实例化的类名
    :return: 实例化后的类（对象）
    '''

    return eval('%s()' % item)


class AddNewTF(Transformer):

    def transform(self, data, project):
        return self.NewValue


class AutoIndexTF(Transformer):
    def init(self):
        super(AutoIndexTF, self).__init__()
        self.currindex = 0

    def transform(self, data, project):
        self.currindex += 1
        return self.currindex


class RenameTF(Transformer):

    def __init__(self):
        super(RenameTF, self).__init__()
        self.OneOutput = False

    def transform(self, data, project):
        if not self.Column in data:
            return
        item = data[self.Column]
        del data[self.Column]
        if self.NewColumn != "":
            data[self.NewColumn] = item


class DeleteTF(Transformer):
    def __init__(self):
        super(DeleteTF, self).__init__()
        self.OneOutput = False

    def transform(self, data, project):
        if self.Column in data:
            del data[self.Column]


class HtmlTF(Transformer):
    pass
    # def __init__(self):
    #     super(HtmlTF, self).__init__()
    #     self.OneInput = True
    #
    # def transform(self, data, project):
    #     return html.escape(data) if self.ConvertType == 'Encode' else html.unescape(data)


class UrlTF(Transformer):
    pass
    # def __init__(self):
    #     super(UrlTF, self).__init__()
    #     self.OneInput = True
    #
    # def transform(self, data, project):
    #     if self.ConvertType == 'Encode':
    #         url = data.encode('utf-8')
    #         return urllib.parse.quote(url)
    #     else:
    #         return urllib.parse.unquote(data)


class RegexSplitTF(Transformer):
    def transform(self, data, project):
        items = re.split(self.Regex, data)
        if len(items) <= self.Index:
            return data
        if not self.FromBack:
            return items[self.Index]
        else:
            index = len(items) - self.Index - 1
            if index < 0:
                return data
            else:
                return items[index]
        return items[index]


class MergeTF(Transformer):
    def __init__(self):
        super(MergeTF, self).__init__()
        self.Format = '{0}'
        self.MergeWith = ''

    def transform(self, data, project):
        res = self.Format
        if self.MergeWith == '':
            columns = []
        else:
            columns = [str(data[r]) for r in self.MergeWith.split(' ')]
        for d in data:
            columns.append(res.format(str(d[self.Column])))
        return columns
        # res = res.format(str(data[self.Column]))
        # yield res


class RegexTF(Transformer):
    def __init__(self):
        super(RegexTF, self).__init__()
        self.Script = ''
        self.OneInput = True

    def init(self):
        self.Regex = re.compile(self.Script)

    def transform(self, data, project):
        item = re.findall(self.Regex, str(data))
        if self.Index < 0:
            return ''
        if len(item) <= self.Index:
            return ''
        else:
            r = item[self.Index]
            return r if isinstance(r, str) else r[0]


class ReReplaceTF(RegexTF):

    def transform(self, data, project):
        return re.sub(self.Regex, self.ReplaceText, data)


class NumberTF(RegexTF):
    def __init__(self):
        super(NumberTF, self).__init__()
        self.Script = ''  # TODO

    def transform(self, data, project):
        t = super(NumberTF, self).transform(data)
        if t is not None and t != '':
            return int(t)
        return t


class SplitTF(Transformer):
    def __init__(self):
        super(SplitTF, self).__init__()
        self.SplitChar = ''
        self.OneInput = True

    def transform(self, data, project):
        splits = self.SplitChar.split(' ')
        sp = splits[0]
        if sp == '':
            return data

        r = data.split(splits[0])
        if len(r) > self.Index:
            return r[self.Index]
        return ''


class TrimTF(Transformer):
    def __init__(self):
        super(TrimTF, self).__init__()
        self.OneInput = True

    def transform(self, data, project):
        return data.strip()


class StrExtractTF(Transformer):
    def __init__(self):
        super(StrExtractTF, self).__init__()
        self.HaveStartEnd = False
        self.Start = ''
        self.OneInput = True
        self.End = ''

    def transform(self, data, project):
        start = data.find(self.Former)
        if start == -1:
            return
        end = data.find(self.End, start)
        if end == -1:
            return
        if self.HaveStartEnd:
            end += len(self.End)
        if not self.HaveStartEnd:
            start += len(self.Former)
        return data[start:end]


class PythonTF(Transformer):
    def __init__(self):
        super(PythonTF, self).__init__()
        self.OneOutput = False
        self.Script = 'value'
        self.ScriptWorkMode = '不进行转换'

    def transform(self, data, project):
        result = eval(self.Script, {'value': data[self.Column]}, data)
        if result is not None and self.IsMultiYield == False:
            key = self.NewColumn if self.NewColumn != '' else self.Column
            data[key] = result
        return result


class CrawlerTF(Transformer):
    '''
    从爬虫转化
    '''

    def __init__(self):
        super(CrawlerTF, self).__init__()
        self.CrawlerSelector = ''
        self.MaxTryCount = 1
        self.IsRegex = False
        self.OneOutput = False

    def init(self, project):
        '''
        根据工程初始化爬虫
        :param project: 项目工程目录
        :return:
        '''
        self.IsMultiYield = True
        self.crawler = project.modules.get(self.CrawlerSelector, None)
        self.buff = {}

    def transform(self, data, project):
        self.init(project)
        crawler = self.crawler
        print(data)
        headers = spider.setHttpItem(crawler.HttpItem)
        # for d in data:
        #     # 爬虫返回的数据
        #     html_data = spider.getURLdata(d, headers)
        #     # 根据xpath规则处理html,
        #     data = spider.processHtml(html_data, crawler.RootXPath, crawler.CrawItems)
        #     print(len(data))
        for d in data:
            html_data = spider.getURLdata(d, headers)
            data = spider.processHtml(html_data, crawler.RootXPath, crawler.CrawItems)
            yield data


class XPathTF(Transformer):
    def __init__(self):
        super(XPathTF, self).__init__()
        self.XPath = ''
        self.IsMultiYield = True
        self.OneOutput = False

    def init(self):
        self.IsMultiYield = True
        self.OneOutput = False

    def transform(self, data, project):
        pass
        # if self.IsManyData:
        #     tree = spider.GetHtmlTree(data[self.Column])
        #     nodes = tree.xpath(self.XPath)
        #     for node in nodes:
        #         ext = {'Text': spider.getnodetext(node), 'HTML': etree.tostring(node).decode('utf-8')}
        #         ext['OHTML'] = ext['HTML']
        #         yield extends.MergeQuery(ext, data, self.NewColumn)
        # else:
        #     tree = spider.GetHtmlTree(data[self.Column])
        #     nodes = tree.xpath(self.XPath)
        #     node = nodes[0]
        #     if hasattr(node, 'text'):
        #         setValue(data, self, node.text)
        #     else:
        #         setValue(data, self, str(node))
        #     yield data


class ToListTF(Transformer):
    def transform(self, data, project):
        yield data


class JsonTF(Transformer):
    def __init__(self):
        super(JsonTF, self).__init__()
        self.OneOutput = False
        self.ScriptWorkMode = '文档列表'

    def init(self):
        self.IsMultiYield = self.ScriptWorkMode == '文档列表'

    def transform(self, data, project):
        js = json.loads(data[self.Column])
        if isinstance(js, list):
            for j in js:
                yield j
        else:
            yield js


class RangeTF(Transformer):
    def __init__(self):
        super(RangeTF, self).__init__()
        self.Skip = 0
        self.Take = 9999999

    def transform(self, data, project):
        pass
        # skip = int(extends.Query(data, self.Skip))
        # take = int(extends.Query(data, self.Take))
        # i = 0
        # for r in data:
        #     if i < skip:
        #         continue
        #     if i >= take:
        #         break
        #     i += 1
        #     yield r


class EtlTF(Transformer):
    pass
    # def transform(self, datas):
    #     subetl = self.__proj__.modules[self.ETLSelector]
    #     if self.IsMultiYield:
    #
    #         for data in datas:
    #             doc = data.copy()
    #             for r in subetl.__generate__(subetl.AllETLTools, [doc]):
    #                 yield extends.MergeQuery(r, data, self.NewColumn)
    #     else:
    #         yield None  # TODO


class BaiduLocation(Transformer):
    pass


class GetIPLocation(Transformer):
    pass


class GetRoute(Transformer):
    pass


class NearbySearch(Transformer):
    pass


class NlpTF(Transformer):
    pass


class TransTF(Transformer):
    pass


class JoinDBTF(Transformer):
    pass


class RepeatTF(Transformer):
    pass


class ResponseTF(Transformer):
    pass


class Time2StrTF(Transformer):
    pass


class DictTF(Transformer):
    pass


class FileExistFT(Transformer):
    def __init__(self):
        super(FileExistFT, self).__init__()
        self.Script = ''
        self.OneInput = True

    def transform(self, data, project):
        return str(os.path.exists(data))


class MergeRepeatTF(Transformer):
    pass


class DelayTF(Transformer):
    pass


class ReadFileTextTF(Transformer):
    pass


class WriteFileTextTF(Transformer):
    pass


class FileDataTF(Transformer):
    pass
