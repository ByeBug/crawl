"""
添加变更信息到c_changeinfo
TODO: 加入 store_to_mysql
"""

import glob, json, re, hashlib

import pymysql


conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='financing_test4', charset='utf8')
cursor = conn.cursor()

files = glob.glob('json/qichacha/*.json')

for i in files:
    
    with open(i, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    pure_name = re.sub(r'\W', '', data['companyName'])
    c_id = hashlib.md5(pure_name.encode('utf-8')).hexdigest()
    
    changeInfos = data['changeInfos']
    
    for c in changeInfos:
        sql = """insert into c_changeinfo
        values (%s, %s, %s, %s, %s)"""
        try:
            cursor.execute(sql, (c_id, c['time'], c['item'], c['before'], c['after']))
            conn.commit()
        except pymysql.DatabaseError as e:
            print(data['companyName'])
            cursor.close()
            conn.close()
            raise e

cursor.close()
conn.close()
