# -*- encoding: utf-8 -*-
"""
@File    : main.py
@Time    : 18/8/2019 18:55
@Author  : liyang

工程执行main文件
"""

from classInit import projectLoad
from classInit.projectExecutor import projExecute

path = 'xmlFile'
project = projectLoad.Project_LoadXml(path + '/t.xml')
print(project.modules)
proj = projExecute(project)
t = proj.projectFunction()
