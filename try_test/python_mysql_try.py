"""
测试各种 pymysql 操作
"""

import pymysql


conn = pymysql.connect(host='localhost', port=3306, 
                       user='root', passwd='root', 
                       db='mysqltest', charset='utf8')
cursor = conn.cursor()

try:
    sql = """select * from test where d_varchar in %s"""
    item = ('你好', 'aaa')
    
    cursor.execute(sql, (item,))
    conn.commit()
    
    q = cursor.fetchall()
except pymysql.DatabaseError as e:
    conn.rollback()
    
    cursor.close()
    conn.close()

    raise e

cursor.close()
conn.close()
