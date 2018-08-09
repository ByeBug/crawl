"""
遍历json/eastmoney中的文件
重新爬取
即添加了按年度划分的财务数据
"""

import glob, re
from spiders.crawl_eastmoney import crawl_from_eastmoney

files = glob.glob('json/eastmoney/*.json')

for f in files:
    code = re.search(r'\d{6}', f).group()
    crawl_from_eastmoney(code)
