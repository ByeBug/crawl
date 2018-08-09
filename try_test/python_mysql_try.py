"""
测试各种 pymysql 操作
"""

import pymysql

import datetime

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='mysqltest', charset='utf8')
cursor = conn.cursor()

try:
    sql = 'select * from test where c_date < %s'
    d = datetime.date.today()
    
    cursor.execute(sql, (d,))
    q = cursor.fetchall()
except pymysql.DatabaseError as e:
    cursor.close()
    conn.close()

    raise e

cursor.close()
conn.close()
