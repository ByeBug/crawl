"""
遍历json/qichacha中的文件
将相关的股票爬下来
"""

import glob, json
from spiders.crawl_stock import crawl_stock

files = glob.glob('json/qichacha/*.json')

for i in files:
    with open(i, 'r', encoding='utf-8') as f:
        data = json.load(f)
    code = data['overview']['stock_code']
    if code:
        print(code)
        crawl_stock(code)
