# -*- encoding: utf-8 -*-
"""
@File    : SmartCrawler.py
@Time    : 19/8/2019 08:56
@Author  : liyang

工程SmartCrawler初始化及设置模块
"""


class SmartCrawler():
    def __init__(self):
        self.IsMultiData = "List"
        self.HttpItem = None
        self.Name = None
        self.CrawItems = None
        self.Login = ""
        self.haslogin = False
        self.RootXPath = ''
        self.Url = ''

    # def autologin(self, loginItem):
    #     if loginItem.postdata is None:
    #         return
    #     import http.cookiejar
    #     cj = http.cookiejar.CookieJar()
    #     pro = urllib.request.HTTPCookieProcessor(cj)
    #     opener = urllib.request.build_opener(pro)
    #     t = [(r, loginItem.Headers[r]) for r in loginItem.Headers]
    #     opener.addheaders = t
    #     binary_data = loginItem.postdata.encode('utf-8')
    #     op = opener.open(loginItem.Url, binary_data)
    #     data = op.read().decode('utf-8')
    #     print(data)
    #     self.HttpItem.Url = op.url
    #     return opener


class CrawItem():
    '''爬虫item的创建和初始化

    '''

    def __init__(self, name=None, sample=None, ismust=False, isHTMLorText=True, xpath=None):
        self.XPath = xpath
        self.Sample = sample
        self.Name = name
        self.IsMust = ismust
        self.IsHTMLorText = isHTMLorText
        self.Children = []

    def __str__(self):
        return "%s %s %s" % (self.Name, self.XPath, self.Sample)


class HTTPItem():
    def __init__(self):
        self.Url = ''
        self.Cookie = ''
        self.Headers = None
        self.Timeout = 30
        self.opener = ""
        self.postdata = ''

    def PraseURL(self, url):
        pass
        # u = Para2Dict(urlparse(self.Url).query, '&', '=')
        # for r in extract.findall(url):
        #     url = url.replace('[' + r + ']', u[r])
        # return url

    # def GetHTML(self, destUrl=None):
    #     if destUrl is None:
    #         destUrl = self.Url
    #     destUrl = self.PraseURL(destUrl)
    #     socket.setdefaulttimeout(self.Timeout)
    #     cj = http.cookiejar.CookieJar()
    #     pro = urllib.request.HTTPCookieProcessor(cj)
    #     opener = urllib.request.build_opener(pro)
    #     t = [(r, self.Headers[r]) for r in self.Headers]
    #     opener.addheaders = t
    #     binary_data = self.postdata.encode('utf-8')
    #     try:
    #         destUrl.encode('ascii')
    #     except UnicodeEncodeError:
    #         destUrl = iriToUri(destUrl)
    #
    #     try:
    #         if self.postdata == '':
    #             page = opener.open(destUrl)
    #         else:
    #             page = opener.open(destUrl, binary_data)
    #         html = page.read()
    #     except Exception as e:
    #         print(e)
    #         return ""
    #
    #     if page.info().get('Content-Encoding') == 'gzip':
    #         html = gzip.decompress(html)
    #     encoding = charset.search(str(html))
    #     if encoding is not None:
    #         encoding = encoding.group(1)
    #     if encoding is None:
    #         encoding = 'utf-8'
    #     try:
    #         html = html.decode(encoding)
    #     except UnicodeDecodeError as e:
    #         print(e)
    #         import chardet
    #         encoding = chardet.detect(html)
    #         html = html.decode(encoding)
    #
    #     return html


def Para2Dict(para, split1, split2):
    '''对xml文件的Parameters进行转换,由string转换为dict

    :param para:参数str
    :param split1:第一个分割
    :param split2:第二个分割
    :return:str转换后的字典
    '''
    r = {}
    for s in para.split(split1):
        rs = s.split(split2)
        if len(rs) < 2:
            continue
        key = rs[0]
        value = s[len(key) + 1:]
        r[rs[0]] = value
    return r
