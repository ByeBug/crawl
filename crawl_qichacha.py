#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  7 21:56:58 2018

@author: zhao
"""

import re
import time, json
import requests
from bs4 import BeautifulSoup as bs

def crawl_from_qichacha(name, url):
    print('crawling', name)

    headers = {
        'Host': 'www.qichacha.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36'
    }
    cookies = {}
    with open('config.json', 'r') as f:
        cookie_str = json.load(f)['cookie_qichacha']
    for cookie in cookie_str.split(';'):
        k, v = cookie.strip().split('=')
        cookies[k] = v

    if not  url:
        payload = {'key': name}
        url_search = 'http://www.qichacha.com/search'
        r = requests.get(url_search, params=payload, cookies=cookies, headers=headers)
        if r.status_code == 200:
            s = bs(r.text, 'html.parser')
            a = s.find('section', id='searchlist').find('tbody').find('tr').find_all('td')[1].find('a')
            url = 'http://www.qichacha.com' + a['href']
        else:
            print('search request error', r.status_code)

    r = requests.get(url, cookies=cookies, headers=headers)
    if r.status_code == 200:
        filename = '_'.join([name, time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()), 'qichacha.html'])
        with open('html/' + filename, 'w', encoding='utf-8') as f:
            f.write(r.text)

        s = bs(r.text, 'html.parser')

        if str(s.select('header ul li')[6]).find('zhaosy') == -1:
            print('Not login')

        qichacha = {}

        # 概况
        try:
            div = s.find('div', id='company-top').find('div', class_='content')
            qichacha['companyName'] = div.select('h1')[0].string.strip()
            qichacha['phone'] = div.select('div:nth-of-type(2) span:nth-of-type(2) span')[0].string.strip()
            qichacha['email'] = div.select('div:nth-of-type(3) span:nth-of-type(2) a')[0].string.strip()
            qichacha['address'] = div.select('div:nth-of-type(4) span:nth-of-type(2) a')[0].string.strip()
        except:
            div = s.find('div', class_='content')
            qichacha['companyName'] = div.div.contents[0].string.strip()
            qichacha['phone'] = div.select('div:nth-of-type(2) span:nth-of-type(2)')[0].string.strip()
            qichacha['email'] = div.select('div:nth-of-type(3) span:nth-of-type(2)')[0].string.strip()
            qichacha['address'] = div.select('div:nth-of-type(4) span:nth-of-type(2)')[0].string.strip()
        qichacha['url'] = r.url
        try:
            qichacha['website'] = div.select('div:nth-of-type(3) span:nth-of-type(4) a')[0].string.strip()
        except:
            qichacha['website'] = ''

        # 工商信息
        baseInfo = {}
        div = s.find('section', id='Cominfo')
        if len(div.find_all('table')) == 1:
            baseInfo['legal_person'] = ''
            table = div.find_all('table')[0]
        else:
            baseInfo['legal_person'] = div.select('table tr:nth-of-type(2) a')[0].string.strip()
            table = div.find_all('table')[1]

        baseInfo['registered_capital'] = table.select('tr td:nth-of-type(2)')[0].string.strip()
        baseInfo['paid_in_capital'] = table.select('tr td:nth-of-type(4)')[0].string.strip()
        baseInfo['company_status'] = table.select('tr:nth-of-type(2) td:nth-of-type(2)')[0].string.strip()
        baseInfo['registered_time'] = table.select('tr:nth-of-type(2) td:nth-of-type(4)')[0].string.strip()
        baseInfo['credit_code'] = table.select('tr:nth-of-type(3) td:nth-of-type(2)')[0].string.strip()
        baseInfo['identification_num'] = table.select('tr:nth-of-type(3) td:nth-of-type(4)')[0].string.strip()
        baseInfo['registration_num'] = table.select('tr:nth-of-type(4) td:nth-of-type(2)')[0].string.strip()
        baseInfo['organization_num'] = table.select('tr:nth-of-type(4) td:nth-of-type(4)')[0].string.strip()
        baseInfo['company_type'] = table.select('tr:nth-of-type(5) td:nth-of-type(2)')[0].string.strip()
        baseInfo['industry'] = table.select('tr:nth-of-type(5) td:nth-of-type(4)')[0].string.strip()
        baseInfo['check_time'] = table.select('tr:nth-of-type(6) td:nth-of-type(2)')[0].string.strip()
        baseInfo['registration_authority'] = table.select('tr:nth-of-type(6) td:nth-of-type(4)')[0].string.strip()
        baseInfo['english_name'] = table.select('tr:nth-of-type(7) td:nth-of-type(4)')[0].string.strip()
        baseInfo['used_name'] = table.select('tr:nth-of-type(8) td:nth-of-type(2)')[0].string.strip()
        baseInfo['staff_size'] = table.select('tr:nth-of-type(9) td:nth-of-type(2)')[0].string.strip()
        baseInfo['business_limitation'] = table.select('tr:nth-of-type(9) td:nth-of-type(4)')[0].string.strip()
        baseInfo['registered_address'] = table.select('tr:nth-of-type(10) td:nth-of-type(2)')[0].contents[0].strip()
        baseInfo['business_scope'] = table.select('tr:nth-of-type(11) td:nth-of-type(2)')[0].string.strip()

        qichacha['baseInfo'] = baseInfo

        # 股东
        # 是否分页 是否有字段为空
        # 不分页
        # example https://www.qichacha.com/firm_cd6b024cf176db2943619725b60b499d.html
        holders = []
        trs = s.find('section', id='Sockinfo').select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            holder = {}
            holder['name'] = tds[1].find('a').string.strip()
            holder['url'] = 'http://www.qichacha.com' + tds[1].find('a')['href']
            holder['proportion'] = tds[2].string.strip()
            holder['amount'] = tds[3].contents[0].strip()
            holder['time'] = tds[4].contents[0].strip()
            holder['type'] = tds[5].string.strip()

            holders.append(holder)

        qichacha['holders'] = holders

        # 对外投资
        investments = []
        trs = s.find('section', id='touzilist').select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            investment = {}
            investment['company_name'] = tds[0].a.string.strip()
            investment['url'] = 'http://www.qichacha.com' + tds[0].a['href']
            investment['legal_person'] = tds[1].a.string.strip()
            investment['registered_capital'] = tds[2].string.strip()
            investment['invest_proportion'] = tds[3].string.strip()
            investment['registered_time'] = tds[4].string.strip()
            investment['company_status'] = tds[5].span.string.strip()

            investments.append(investment)

        pager = s.find('section', id='touzilist').find('ul', class_='pagination')

        if pager:
            pages = int(re.search('(\d+)$', pager.find_all('li')[-1].a.string).group())
            page = 2
            while page <= pages:
                unique = re.search('_(\w+).html', qichacha['url']).group(1)
                payload = {'box': 'touzi', 'companyname': name, 'p': page, 'tab': 'base', 'unique': unique}
                url = 'https://www.qichacha.com/company_getinfos'
                temp_r = requests.get(url, params=payload, headers=headers, cookies=cookies)
                if temp_r.status_code == 200:
                    temp_s = bs(temp_r.text, 'html.parser')
                    trs = temp_s.select('table tr')[1:]
                    for tr in trs:
                        tds = tr.find_all('td')
                        investment = {}
                        investment['company_name'] = tds[0].a.string.strip()
                        investment['url'] = 'http://www.qichacha.com' + tds[0].a['href']
                        investment['legal_person'] = tds[1].a.string.strip()
                        investment['registered_capital'] = tds[2].string.strip()
                        investment['invest_proportion'] = tds[3].string.strip()
                        investment['registered_time'] = tds[4].string.strip()
                        investment['company_status'] = tds[5].span.string.strip()

                        investments.append(investment)
                else:
                    print('invest pagination error', temp_r.status_code)
                page += 1

        qichacha['investments'] = investments

        # 高管
        managers = []
        trs = s.find('section', id='Mainmember').select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            manager = {}
            manager['name'] = tds[1].a.string.strip()
            manager['position'] = tds[2].string.strip()

            managers.append(manager)

        qichacha['managers'] = managers

        # 变更信息
        changeInfos = []
        try:
            trs = s.find('section', id='Changelist').select('table tr')[1:]
            for tr in trs:
                tds = tr.find_all('td')
                changeInfo = {}
                changeInfo['time'] = tds[1].string.strip()
                changeInfo['item'] = tds[2].contents[0].strip()
                before = ''
                for content in tds[3].div.contents:
                    if content.string:
                        before += content.string.strip()
                changeInfo['before'] = before
                after = ''
                for content in tds[4].div.contents:
                    if content.string:
                        after += content.string.strip()
                changeInfo['after'] = after

                changeInfos.append(changeInfo)
        except:
            changeInfos = []

        qichacha['changeInfos'] = changeInfos

        filename = name + '_qichacha.json'
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(qichacha, f, ensure_ascii=False)

        print('crawl finished', name)

        return qichacha

    else:
        print('detail request error', r.status_code)
        return -1


#qichacha = crawl_from_qichacha('海航集团有限公司', '')
