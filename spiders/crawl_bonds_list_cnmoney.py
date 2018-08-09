"""
从中国外汇交易中心获取债券列表
"""

import json
import requests


url = 'http://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondMarketInfoList2'

headers = {
    'host': 'www.chinamoney.com.cn',
    'User-Agent':
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
}

data = {
    'pageNo': 1,
    'pageSize': 1000,
    'bondName': '',
    'bondCode': '',
    'issueEnty': '',
    'bondType': '',
    'couponType': '',
    'issueYear': '',
    'entyDefinedCode': '',
    'rtngShrt': ''
}

resultList = []
while 1:
    r = requests.post(url, data, timeout=4, headers=headers)
    r.raise_for_status()

    q = r.json()

    print('crawl page {} of {}'.format(q['data']['nextpg'], q['data']['pageTotal']))
    if q['data']['resultList']:
        resultList.extend(q['data']['resultList'])
        data['pageNo'] += 1
    else:
        break

path = 'g:/crawl/'
json_path = path + 'json/'

with open(json_path + 'cnmoney_bonds_list.json', 'w', encoding='utf-8') as f:
    json.dump(resultList, f, ensure_ascii=False)
