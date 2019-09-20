# etlpy
##designed by desert
a smart stream-like crawler &amp; etl python library

##1.简介
etlpy是基于配置文件的数据采集和清洗工具。  

写爬虫和数据清洗代码总是很烦人。因此，应该通过工具生成爬虫和数据清洗的代码！  etlpy就是为了解决这个问题而生的。  

通过可视化和图形化设计工具（Hawk），快速生成爬虫和数据清洗流程，并保存为xml文件，并由etlpy引擎解析它，即可获得最终的数据结果。

##2.使用
使用起来非常简单执行main.py文件）:
```
from classInit import projectLoad
from classInit.projectExecutor import projExecute

path = 'xmlFile'
project = projectLoad.Project_LoadXml(path + '/demo.xml')
print(project.modules)
proj = projExecute(project)
t = proj.projectFunction()

```

##3.基本原理
模块分为 生成，过滤，排序，转换，执行四种。  

利用Python的生成器，可以将不同模块组织起来，定义一个流水线，数据（python的字典）会在流水线上被加工和消费。  

##4. 用途
爬虫，计算，清洗，任何符合一定计算范式的数据，都可以使用它来完成。
