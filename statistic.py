"""
统计爬虫数据库中的情况
"""

import os
import configparser

import pymysql


if not os.path.isfile('myconfig.cfg'):
    print("myconfig.cfg doesn't exist")
    exit()

config = configparser.RawConfigParser()
config.read('myconfig.cfg', encoding='utf-8')

try:
    crawl_db_host = config['crawl_db']['host']
    crawl_db_port = int(config['crawl_db']['port'])
    crawl_db_user = config['crawl_db']['user']
    crawl_db_passwd = config['crawl_db']['passwd']
    crawl_db_db = config['crawl_db']['db']

    conn = pymysql.connect(host=crawl_db_host, port=crawl_db_port,
                        user=crawl_db_user, passwd=crawl_db_passwd,
                        db=crawl_db_db, charset='utf8')
    cursor = conn.cursor()
except Exception as e:
    print('Connect to crawl_db failed')
    print(e)
    exit()

# 总计
sql = """SELECT COUNT(*) FROM crawl"""
cursor.execute(sql)
total = cursor.fetchone()[0]

# 已爬
sql = """SELECT COUNT(*) FROM crawl 
WHERE crawled_date > '2000-01-01'"""
cursor.execute(sql)
crawled = cursor.fetchone()[0]

# 未爬
sql = """SELECT COUNT(*) FROM crawl 
WHERE crawled_date = '2000-01-01'"""
cursor.execute(sql)
no_crawl = cursor.fetchone()[0]

# 按level统计
sql = """SELECT `level`, count(*) FROM crawl 
WHERE crawled_date > '2000-01-01'
GROUP BY `level`"""
cursor.execute(sql)
level_stat = cursor.fetchall()

# 按id统计
sql = """SELECT right(`unique`, 1) as `id`, count(*)
FROM crawl WHERE crawled_date > '2000-01-01'
GROUP BY `id`"""
cursor.execute(sql)
id_result = cursor.fetchall()
id_stat = []
for i in range(8):
    id_stat.append((i, id_result[2*i][1] + id_result[2*i+1][1]))

# 输出
print('=== 概览 ===')
print('{}: {}'.format('总计', total))
print('{}: {}'.format('已爬', crawled))
print('{}: {}'.format('未爬', no_crawl))
print('\n=== 按 level 统计 ===')
for i in level_stat:
    print('{:>2}: {}'.format(i[0], i[1]))
print('\n=== 按 id 统计 ===')
for i in id_stat:
    print('{}: {}'.format(i[0], i[1]))

cursor.close()
conn.close()
