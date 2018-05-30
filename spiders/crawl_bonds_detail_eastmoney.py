# -*- coding: utf-8 -*-
"""
Created on Sun May 20 00:44:50 2018

@author: storm

从东方财富爬取债券详细信息
"""

import requests


def crawl_bond_detail_eastmoney(bond_code, market):

    # 债券基本资料 企/转共有

    url = 'http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js'
    headers = {
        'host': 'dcfm.eastmoney.com',
        'User-Agent':
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
    }
    payload = {
        'type': 'ZQ_JBXX',
        'token': '70f12f2f4f091e459a279469fe49eca5',
        'filter': "(BONDCODE='{}')(TEXCH='CNSE{}')".format(bond_code, market)
    }

    r = requests.get(url, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    try:
        bond_detail = r.json()[0]
    except Exception as e:
        print('request text:', r.text)
        raise e

    # 债券基本资料 转独有
    # bond_id = bond_code + 1 or 2, 1 for 上交所, 2 for 深交所

    if bond_detail['TEXCH'] == 'CNSESH':
        bond_id = bond_code + '1'
    elif bond_detail['TEXCH'] == 'CNSESZ':
        bond_id = bond_code + '2'

    url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx'
    payload = {
        'type': 'CT',
        'cmd': '{}'.format(bond_id),
        'sty': 'FC20CD',
        'js': '(x)',
        'token': '4f1862fc3b5e77c150a2b985b12db0fd'
    }

    r = requests.get(url, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    l = r.text.split(',')

    bond_detail['stock_name'] = l[16]
    bond_detail['stock_price'] = l[17]
    bond_detail['convert_date'] = l[8]
    bond_detail['convert_price'] = l[5]
    bond_detail['convert_value'] = l[9]
    bond_detail['premium_rate'] = l[10]
    bond_detail['HS_trigger_price'] = l[13]
    bond_detail['QS_trigger_price'] = l[12]
    bond_detail['expire_trigger_price'] = l[6]
    bond_detail['pure_bond_value'] = l[7]

    return bond_detail


if __name__ == '__main__':
    code = '113507'
    market = 'SH'
    bond_detail = crawl_bond_detail_eastmoney(code, market)
