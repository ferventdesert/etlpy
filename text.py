# -*- encoding: utf-8 -*-
"""
@File    : text.py
@Time    : 23/8/2019 14:14
@Author  : liyang
"""
import re
t = [{'1':'asgewt'},{'2':'asoiwerogroifndjkl'}]
for i in range(len(t)):
    if '1' in t[i]:
        flag = i
        print('5')
del t[flag]
print(t)