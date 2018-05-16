#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 16 14:13:33 2018

@author: zhao

crawl company base info from qichacha
"""

import re
import time, json
import requests
from bs4 import BeautifulSoup as bs

#def crawl_info_qcc(name, unique):

name = '海南省慈航公益基金会'
unique = 'sdd3f7007d57247dcfed853623ad53e7'


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
    
if not unique:
    payload = {'key': name}
    url_search = 'http://www.qichacha.com/search'
    r = requests.get(url_search, params=payload, cookies=cookies, headers=headers)
    if r.status_code == 200:
        s = bs(r.text, 'html.parser')
        a = s.find('section', id='searchlist').find('tbody').find('tr').find_all('td')[1].find('a')
        url = 'http://www.qichacha.com' + a['href']
        unique = re.search('firm_(\w+).html', url).group(1)
    else:
        print('search request error', r.status_code)


url = 'http://www.qichacha.com/company_getinfos'
payload = {
        'unique': unique,
        'tab': 'base',
        'companyname': name
        }
r = requests.get(url, cookies=cookies, headers=headers, params=payload)
if r.status_code == 200:
    filename = '_'.join([name, time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()), 'qichacha.html'])
    with open('html/' + filename, 'w', encoding='utf-8') as f:
        f.write(r.text)

    s = bs(r.text, 'html.parser')
    
    qichacha = {}
    
    # 工商信息
    baseInfo = {}
    div = s.find('section', id='Cominfo')
    if div:
        if len(div.find_all('table')) == 1:
            table = div.find('table')
            baseInfo['credit_code'] = table.select('tr:nth-of-type(1) td:nth-of-type(2)')[0].string.strip()
            baseInfo['legal_person'] = table.select('tr:nth-of-type(2) td:nth-of-type(2)')[0].string.strip()
            baseInfo['registered_capital'] = table.select('tr:nth-of-type(2) td:nth-of-type(4)')[0].string.strip()
            baseInfo['registered_time'] = table.select('tr:nth-of-type(3) td:nth-of-type(2)')[0].string.strip()
            baseInfo['company_status'] = table.select('tr:nth-of-type(3) td:nth-of-type(4)')[0].string.strip()
            baseInfo['company_type'] = table.select('tr:nth-of-type(4) td:nth-of-type(2)')[0].string.strip()
            baseInfo['registration_authority'] = table.select('tr:nth-of-type(4) td:nth-of-type(4)')[0].string.strip()
            
            qichacha['baseInfo'] = baseInfo
        else:
            table = div.find_all('table')[1]
            baseInfo['legal_person'] = div.select('table tr:nth-of-type(2) a')[0].string.strip()
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
    holders = []
    div = s.find('section', id='Sockinfo')
    if div:
        trs = div.select('table tr')[1:]
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
    div = s.find('section', id='touzilist')
    if div:
        trs = div.select('table tr')[1:]
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
    div = s.find('section', id='Mainmember')
    if div:
        trs = div.select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            manager = {}
            manager['name'] = tds[1].a.string.strip()
            manager['position'] = tds[2].string.strip()
    
            managers.append(manager)
    
        qichacha['managers'] = managers
        
        
    
    # 变更信息
    changeInfos = []
    div = s.find('section', id='Changelist')
    if div:
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

        qichacha['changeInfos'] = changeInfos
        

        
else:
    print('detail request failed', r.status_code)
    
    
    
