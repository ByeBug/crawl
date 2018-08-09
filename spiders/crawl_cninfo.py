#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 13 14:06:55 2018

@author: zhao

从巨潮获取股票详细信息
包括 公司概况、发行筹资、股票质押公告
"""

"""
http://www.cninfo.com.cn/information/brief/szcn300135.html

shmb
上证 A股 600 601 603
上证 B股 900

szmb
深证 A股 000
深证 B股 200

szcn
深证 创业板 300

szsme
深证 中小板 002
"""

import re
import os
import json, time
import configparser

import requests
from bs4 import BeautifulSoup as bs


def init_cninfo():
    cninfo = {}

    return cninfo


def crawl_from_cninfo(code):
    if re.match('(^600)|(^601)|(^603)|(^900)', code):
        cninfo_code = 'shmb' + code
    elif re.match('(^000)|(^200)', code):
        cninfo_code = 'szmb' + code
    elif re.match('(^300)', code):
        cninfo_code = 'szcn' + code
    elif re.match('(^002)', code):
        cninfo_code = 'szsme' + code

    headers = {
        'Host': 'www.cninfo.com.cn',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
    }

    # 公司概况
    url = 'http://www.cninfo.com.cn/information/brief/%s.html' % cninfo_code

    r = requests.get(url, timeout=4, headers=headers)
    r.raise_for_status()

    r.encoding = 'gb2312'

    s = bs(r.text, 'html.parser')

    cninfo = init_cninfo()

    cninfo['stock_code'] = s.find('tr').find('td').contents[1].string.strip()
    cninfo['stock_abbreviation'] = s.find('tr').find('td').contents[3].string.strip()
    trs = s.find_all('table')[1].find_all('tr')
    cninfo['company_name'] = trs[0].find_all('td')[1].string.strip()
    cninfo['english_name'] = trs[1].find_all('td')[1].string.strip()
    cninfo['address'] = trs[2].find_all('td')[1].string.strip()
    cninfo['company_abbreviation'] = trs[3].find_all('td')[1].string.strip()
    cninfo['legal_person'] = trs[4].find_all('td')[1].string.strip()
    cninfo['secretary'] = trs[5].find_all('td')[1].string.strip()
    cninfo['registered_capital'] = trs[6].find_all('td')[1].string.strip()
    cninfo['industry'] = trs[7].find_all('td')[1].string.strip()
    cninfo['postcode'] = trs[8].find_all('td')[1].string.strip()
    cninfo['phone'] = trs[9].find_all('td')[1].string.strip()
    cninfo['website'] = trs[11].find_all('td')[1].string.strip()
    cninfo['list_time'] = trs[12].find_all('td')[1].string.strip()
    cninfo['issue_time'] = trs[13].find_all('td')[1].string.strip()
    cninfo['issue_num'] = trs[14].find_all('td')[1].string.strip()
    cninfo['issue_price'] = trs[15].find_all('td')[1].string.strip()
    cninfo['issue_pe_ratio'] = trs[16].find_all('td')[1].string.strip()
    cninfo['issue_mode'] = trs[17].find_all('td')[1].string.strip()
    cninfo['main_underwriter'] = trs[18].find_all('td')[1].string.strip()
    cninfo['list_sponsor'] = trs[19].find_all('td')[1].string.strip()
    cninfo['sponsor_institution'] = trs[20].find_all('td')[1].string.strip()

    # # 发行筹资
    # url = 'http://www.cninfo.com.cn/information/issue/%s.html' % cninfo_code

    # r = requests.get(url, timeout=4, headers=headers)
    # r.raise_for_status()

    # r.encoding = 'gb2312'

    # s = bs(r.text, 'html.parser')

    # trs = s.select('div.zx_left table > tr')
    # cninfo['issue_type'] = trs[0].find_all('td')[1].string.strip()
    # cninfo['issue_begin'] = trs[1].find_all('td')[1].string.strip()
    # cninfo['issue_nature'] = trs[2].find_all('td')[1].string.strip()
    # cninfo['issue_shares_type'] = trs[3].find_all('td')[1].string.strip()
    # cninfo['issue_way'] = trs[4].find_all('td')[1].string.strip()
    # cninfo['issue_count'] = trs[5].find_all('td')[1].string.strip()
    # cninfo['issue_rmb_price'] = trs[6].find_all('td')[1].string.strip()
    # cninfo['issue_fore_price'] = trs[7].find_all('td')[1].string.strip()
    # cninfo['fact_raise_funds'] = trs[8].find_all('td')[1].string.strip()
    # cninfo['fact_issue_cost'] = trs[9].find_all('td')[1].string.strip()
    # cninfo['issue_rate'] = trs[10].find_all('td')[1].string.strip()
    # cninfo['online_rate'] = trs[11].find_all('td')[1].string.strip()
    # cninfo['two_grade_rate'] = trs[12].find_all('td')[1].string.strip()

    # # 配股
    # cninfo['allotment'] = []
    # url = 'http://www.cninfo.com.cn/information/allotment/%s.html' % cninfo_code

    # r = requests.get(url, timeout=4, headers=headers)
    # r.raise_for_status()

    # r.encoding = 'gb2312'

    # s = bs(r.text, 'html.parser')

    # trs = s.select('div.zx_left table > tr')
    # trs = trs[1:]
    # if trs[0].find_all('td')[0].string.strip():
    #     for i in trs:
    #         tds = i.find_all('td')
    #         cninfo['allotment'].append({
    #             'year': tds[0].string.strip(),
    #             'plan': tds[1].string.strip(),
    #             'price': tds[2].string.strip(),
    #             'register_date': tds[3].string.strip(),
    #             'dividend_date': tds[4].string.strip(),
    #             'begin_end_date': tds[5].string.strip(),
    #             'listed_date': tds[6].string.strip(),
    #         })

    # 质押公告
    cninfo['pledge_announcement'] = []
    url = 'http://www.cninfo.com.cn/cninfo-new/announcement/query'

    payload = {
        'stock': code,
        'searchkey': '质押',
        'category': '',
        'pageNum': 1,
        'pageSize': 50,
        'column': 'szse_main',
        'tabName': 'fulltext',
        'sortName': '',
        'sortType': '',
        'limit': '',
        'seDate': ''
    }

    while 1:
        r = requests.post(url, data=payload, timeout=4, headers=headers)
        r.raise_for_status()

        s = r.json()

        for announcement in s['announcements']:
            item = {
                'adjunctSize': announcement['adjunctSize'],
                'adjunctType': announcement['adjunctType'],
                'adjunctUrl': 'http://www.cninfo.com.cn/' + announcement['adjunctUrl'],
                'announcementTime': time.strftime('%Y-%m-%d', time.localtime(announcement['announcementTime'] / 1000)),
                'announcementTitle': announcement['announcementTitle']
            }
            cninfo['pledge_announcement'].append(item)
        
        if not s['hasMore']:
            break

        payload['pageNum'] += 1

    # 保存为JSON
    config = configparser.RawConfigParser()
    config.read('config.cfg', encoding='utf-8')
    path = config['crawl']['save_path']

    json_path = os.path.join(path, 'json/cninfo')
    if not os.path.isdir(json_path):
        os.makedirs(json_path)
    with open(os.path.join(json_path, code + '.json'), 'w', encoding='utf-8') as f:
        json.dump(cninfo, f, ensure_ascii=False)

    return cninfo


if __name__ == '__main__':
    code = '000031'
    cninfo = crawl_from_cninfo(code)
