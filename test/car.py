# coding=utf-8
import os
import sys


parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

from etlpy.etlpy import *
import pandas as pd
from etlpy.multi_yielder import THREAD_MODE

df = pd.read_excel('/Users/zhaoyiming/Documents/datasets/汽车/汽车大系.xlsx')
df=df.drop_duplicates(['id'])

url = 'http://www.autohome.com.cn/'
url2 = 'http://reply.autohome.com.cn/api/comments/show.json?count=50&page={p}&id={pid}&appid=1&datatype=json'
url3 = 'https://club.m.autohome.com.cn/bbs/forum-c-{id}-{_}.html'

t = task().create(df).keep('id 中文名') \


news = \
task().cp('id:page').format(url + '{_}/0/0-0-1-0/').get().xpath('//*[@id="maindiv"]/div[2]/div/div[2]/div/a[last()-1]')[
    0].text() \
    .num()[0].let('p').range('1:[page]', mode='*').map(lambda x: x + 1).format(url + '{id}/0/0-0-{p}-0/').get() \
    .pyq('.cont-info > ul > li').list('中文名 id').html().tree() \
    .cp('p:标题').xpath('//h3/a')[0].text() \
    .cp('p:作者').xpath('//p[1]/span[1]/a')[0].text() \
    .cp('p:日期').xpath('//p[1]/span[2]')[0].text() \
    .cp('p:阅读').xpath('//p[1]/span[3]')[0].text() \
    .cp('p:评论').xpath('//p[1]/span[4]')[0].text() \
    .cp('p:标签').xpath('//p[3]/a')[0].text() \
    .cp('p:url').xpath('//h3/a/@href')[0].rm('p')

news_content = task().cp('url:pid').re('\d{6,}')[-1].url.map(lambda x: 'http:' + x).replace('(\d{1,2})-all)?.html',
    '.article-content').text()

news_comment = task().let('p').set(1).cp('pid:page').format(url2).get().load('json')['commentcountall'].map(
    lambda x: int(x / 50) + 1).let('p').range('1:[page]', mode='*') \
    .map(lambda x: x + 1).format(url2).get().load('json')['commentlist'].list().drill().rm('p')


forum = task().rm('中文名').let('page').set('1').format(url3).get().xpath('//*[@id="pagination"]/span[1]/input/@value')[0].text().num()[0]\
    .pl().let('p').range('1:[page]',mode='*').format(url3).get() \
    .pyq('.dataitem').list().html().tree() \
    .cp('p:标题').xpath('//a/div[1]/h4')[0].text() \
    .cp('p:信息').xpath('//@lang')[0].text() \
    .rm('p id').count(1000)

t+= forum
with open('car_detail.txt','w',encoding='utf-8') as f:
    for r in t.query(mode=[THREAD_MODE]):
        if r['标题'] is not None:
            r['标题']=r['标题'].replace('\t',' ')
        f.write('%s\t%s\n'%(r['信息'],r['标题']))






"""https://clubajax.autohome.com.cn/topic/rv?ids=68636168%2C68483133%2C68337683%2C68538636%2C68769979%2C68107494%2C68727736%2C68662797%2C68641093%2C68542968%2C68526238%2C68335740%2C68144650%2C67887433%2C63603597%2C61320362%2C64789017%2C61320052%2C68788727%2C68690688%2C68793958%2C68734539%2C68108882%2C66747057%2C68740022%2C61420614%2C68396378%2C68793765%2C68399126%2C66709631%2C67093707%2C67668632%2C66511580%2C68519018%2C68699937%2C68706016%2C68324712%2C68539772%2C68725096%2C68652907%2C68788998%2C68154712%2C68770010%2C68783882%2C68784381%2C68752386%2C68788776%2C68793267%2C68763771%2C67551583%2C68782222%2C68792798%2C68791810%2C68165203%2C68792061%2C68788867%2C68785752%2C68792239%2C68722930%2C68783776%2C68718609%2C68790735%2C68775439%2C68719829%2C64485489%2C68780459%2C68780533%2C68771320%2C68791102%2C68670320%2C68791082%2C62935564%2C68703688%2C67832705%2C68755372%2C68637196%2C68751759%2C68318281%2C68749733%2C68769108%2C68757290%2C66806202%2C68781958%2C68790045%2C66281281%2C68697656%2C68784487%2C68464857%2C68787867%2C68789735%2C68788752%2C64795404%2C68776675%2C68217534%2C68785868%2C68422331%2C68784001%2C65515744%2C68742603%2C68786494%2C66011183%2C68768396%2C68771370%2C68752519%2C68774346%2C68784505%2C68782260%2C68721234%2C68774469%2C68694268%2C68766355%2C68208651%2C68784014%2C68767245%2C68787972%2C68765607%2C68759041%2C68787761%2C
"""