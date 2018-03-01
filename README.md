
# etlpy: Python编写的流式爬虫系统

## 简介

etlpy是纯Python开发的函数库，实现流式DSL(领域特定语言)，能一行内完成爬虫，文件处理和数据清洗等。能和pandas等类库充分集成。

它和linux的bash pipeline,C#的Linq以及作者本人开发的Hawk有高度的相似性。

下面一行代码实现了获取博客园第1到10页的所有html:
```
from etlpy.etlpy import *
t= task().p.create(range(1,10)).cp('p:html').format('http://www.cnblogs.com/p{_}').get()
#t.to_df()  生成DataFrame
for data in t:
    print data

```
把上面的t改成下面的语句，自动监测算法就能自动分析网页结构，生成解析脚本：

`t=task().create().url.set('http://www.cnblogs.com').get().tree().detect()`


在p列生成从1到10的数，拷贝p列到html列，将html列合并为url,并发送web请求，最后的html正文保存在html列。

etlpy的特性有：

- 同时支持python2和python3
- 内置方便的代理，http get/post请求，写法与requests库非常相似
- 内置正则解析，html转义，json转换等数据清洗功能，直接输出
- 能方便地将任务按照协程，线程，进程，和多机分布式的方式进行任务并行