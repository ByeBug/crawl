"""
将 big_customer_result.json 中的集团
写入 mysql crawl_db的groups表
"""


import pymysql
import json, re


# 将集团信息存入数据库
def groups_to_mysql():
    with open('big_customer_result.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    sql = """insert into groups(`unique`, `root_name`)
        values (%s, %s)"""

    for i in data:
        unique = re.search(r'firm_(\w+).html', i['url']).group(1)
        root_name = i['n_name']

        try:
            cursor.execute(sql, (unique, root_name))
        except pymysql.DatabaseError as e:
            cursor.close()
            conn.close()

            raise e

    conn.commit()


# 初始化 crawl表，将集团加入其中
def init_crawl():
    sql = """select `unique`, `root_name` from groups"""

    cursor.execute(sql)
    q = cursor.fetchall()

    sql = """insert into crawl(`unique`, `name`, `level`, `crawled_date`)
        values (%s, %s, %s, %s)"""
    for i in q:
        try:
            cursor.execute(sql, (i[0], i[1], 0, '2000-01-01'))
        except pymysql.DatabaseError as e:
            cursor.close()
            conn.close()

            raise e
    
    conn.commit()


if __name__ == '__main__':
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='crawl_db', charset='utf8')
    cursor = conn.cursor()

    init_crawl()
    
    cursor.close()
    conn.close()
