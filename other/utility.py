"""
遍历 json/cninfo 中的文件
获取全部股票代码
重新爬取，增加发行筹资信息
再保存为JSON文件
"""

import glob, re
from spiders.crawl_cninfo import crawl_from_cninfo

files = glob.glob('g:/crawl/json/cninfo/*.json')

for i in files:
    code = re.search(r'\d{6}', i).group()

    crawl_from_cninfo(code)

