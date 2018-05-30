# coding: utf-8
"""
遍历json/qichacha/下的文件
取出股票代码
调用东方财富股票爬虫爬取数据
"""

import glob
import json
from spiders.crawl_eastmoney import crawl_from_eastmoney
from spiders.crawl_cninfo import crawl_from_cninfo


files = glob.glob('./json/qichacha/*.json')
codes = []

for j in files:
    with open(j, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if data['overview']['stock_code']:
        codes.append(data['overview']['stock_code'])

for code in codes:
    e = crawl_from_eastmoney(code)

    if e['b_stock_info']:
        crawl_from_cninfo(e['b_stock_info']['code'])
    
    crawl_from_cninfo(code)
