"""
爬取海航集团下的北金所的公告
"""
import json, os

import pymysql

from spiders.crawl_cfae import crawl_cfae


conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='financing_test4', charset='utf8')
cursor = conn.cursor()

group = '海航集团'

sql = 'select name from c_client where `group`=%s'
cursor.execute(sql, group)
q = cursor.fetchall()

cursor.close()
conn.close()

if not os.path.isfile('cfae_tmp.json'):
    data = {
        'crawled': [],
        'all_announcements': []
    }
    with open('cfae_tmp.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

with open('cfae_tmp.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

total = len(q)
for index, i in enumerate(q, 1):
    name = i[0].replace('（', '(').replace('）', ')')
    if name in data['crawled']:
        continue
    print('%d/%d' % (index, total), name)
    try:
        announcements = crawl_cfae(name)
        data['all_announcements'].extend(announcements)
        data['crawled'].append(name)
    except Exception as e:
        print(name, 'has error')
#        with open('cfae_tmp.json', 'w', encoding='utf-8') as f:
#            json.dump(data, f, ensure_ascii=False)
        print(e)

    
with open('cfae_tmp.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False)