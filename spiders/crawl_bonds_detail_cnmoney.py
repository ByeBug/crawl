"""
从中国外汇交易中心爬取债券信息

需要提供 bondDefinedCode
"""
import requests, os, json

def crawl_detail_cnmoney(bondDefinedCode):
    url = 'http://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo'

    headers = {
        'host': 'www.chinamoney.com.cn',
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
    }

    data = {
        'bondDefinedCode': bondDefinedCode
    }

    r = requests.post(url, data=data, timeout=4, headers=headers)

    bond_detail = r.json()['data']['bondBaseInfo']

    path = 'g:/crawl/'
    json_path = path + 'json/cnmoney/'

    if not os.path.isdir(json_path):
        os.makedirs(json_path)
    with open(json_path + bond_detail['bondCode'] + '.json', 'w', encoding='utf-8') as f:
        json.dump(bond_detail, f, ensure_ascii=False)

    return bond_detail


if __name__ == '__main__':
    bondDefinedCode = '1000056396'

    bond_detail = crawl_detail_cnmoney(bondDefinedCode)
