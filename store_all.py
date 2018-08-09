"""
遍历json/qichacha中的文件
全部存进数据库
"""

import glob
from store_to_mysql import store

files = glob.glob('json/qichacha/*.json')

for i in files:
    store(i)
    