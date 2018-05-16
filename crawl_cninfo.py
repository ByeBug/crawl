#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 13 14:06:55 2018

@author: zhao
"""

import requests
import json
from bs4 import BeautifulSoup as bs

headers = {
        'Host': 'www.cninfo.com.cn',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
        }

url_brief = 'http://www.cninfo.com.cn/information/brief/shmb600221.html'
r = requests.get(url_brief, headers=headers)
if r.status_code == 200:
    r.encoding = 'gb2312'
    s = bs(r.text, 'html.parser')
else:
    print('brief request error', r.status_code)
    
    
url_issue = 'http://www.cninfo.com.cn/information/issue/shmb600221.html'
r = requests.get(url_issue, headers=headers)
if r.status_code == 200:
    r.encoding = 'gb2312'
    s2 = bs(r.text, 'html.parser')
else:
    print('issue request error', r.status_code)
