"""
从企风控爬取控股企业信息
账号：13820000001
密码：qfk123456!@#$%^
"""

import json, os
import requests

def crawl_holdings(unique):
    url = 'http://www.qifengkong.com/a/corp/common/businessInfo/vipHoldComp'

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Host': 'www.qifengkong.com',
        'Origin': 'http://www.qifengkong.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
    }

    cookie_str = '_uab_collina=152842848977186429643909; _umdata=0712F33290AB8A6DAA9492599E5221BD3237218696EEFAD2ABC21DB1ED3694ECD448655B4B2FC5B6CD43AD3E795C914CB74BCF38E0505659E004A3CCEDCA6DBD; rememberMe=BdSXw4sOf37KagakL19jwdGimgdWJnEMnkHJaiGEUUxMDRdwHc88CedqevuFyjr0pFpALWyRMguAdKU6PAnYC6RAe+VB1bsovCmoOMr53DeUaGCO78B4LH//j825H+p/pmrVbaOlmX0547ftqYx5VQxwzcyGUpJgm5G7rvPnSVxwovbkY/sfcLL549/+e8Rb2XPoLhtp6VpvzKx6bgasimQlDMkj88TlUE1iIXPU5pDZDMRr50In8WyFkjgrtJy7TXKjzPk6ZGLTbQJp/iplHDxmy0C13m99SJSN7i01gq8Fos8UZ0sQbYSyFbNJtTnTYbqodDH3UEmrFgX3QhVY+3Qt8C4fswdtWOcgksWXkpNqQk/r9Wm37QyaSmje9yPxybf+RjwpwJmijHN5W6I4+k9Ac8EaUQdNif8poSX3gVfo2aZ1SoCf7pmTDN0GaO781B9UL6jwOLQrLoTT8oR2d9mM/iRcOLXf7H1F3aVxUBR+I1uL7NEk5PjvI1IzAcLe5pWg3+RDtRHga9G8lqnjWY2Rkgr5XPHQ123MzwgQf9LPEy+XJRg1A7+io6bdRQ3QSZZq94T77VdDIOc7GA9TXogtnu079yaBH4MwUhm3QJEVmdtrBjrRKtrwWioHDYNDOGYiIiG2knIkLww1u8w9Q7BDu3W90lI1HitupmJyio2tct09fmRjTmpiulSQfMdzb1RLvn9LogGujtC25NHC5v1n8+SD/4aFc0Rdpd7/6qxM9YoYn5cle9pXsAVsRj7yC2e8swD39NHrOZq/4+RRho5kmhnLcu1vFERA7cOF/RcpDevvHvr5FF3T/ykISXftTUkgpwojcoUqLzGS11LCrQ==; UM_distinctid=163f1ec418b497-0e7232db07f648-5d4e211f-1fa400-163f1ec418c816; CNZZDATA1256820335=2139564685-1528769809-http%253A%252F%252Fwww.qifengkong.com%252F%7C1528769809; qcc.session.id=c5becea2-7c0c-4ad3-9d50-cefe9d935713'
    cookies = {}
    for cookie in cookie_str.split(';'):
        k, v = cookie.strip().split('=', 1)
        cookies[k] = v

    data = {
        'condition': {
            'keyNo': unique,
            'page': {
                'pageNo': 1,
                'pageSize': 50,
                'orderBy': ''
            }
        }
    }

    entity_list = []
    while 1:
        print('crawling page {}'.format(data['condition']['page']['pageNo']))
        r = requests.post(url, json=data, timeout=4, cookies=cookies, headers=headers)
        r.raise_for_status()
        q = r.json()
        if q['entityList']:
            entity_list.extend(q['entityList'])
            data['condition']['page']['pageNo'] += 1
        else:
            break

    return entity_list


if __name__ == '__main__':
    unique = '3ad1d93a9080cc86ffc0e9612717cc0d'

    entity_list = crawl_holdings(unique)

    path = 'g:/crawl/'
    json_path = path + 'json/qifengkong/'
    if not os.path.isdir(json_path):
        os.makedirs(json_path)
    with open(json_path + unique + '.json', 'w', encoding='utf-8') as f:
        json.dump(entity_list, f, ensure_ascii=False)
