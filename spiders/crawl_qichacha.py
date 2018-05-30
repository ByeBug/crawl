#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  7 21:56:58 2018

@author: zhao

从企查查爬取指定公司的信息，保存为JSON文件
"""

import re
import os
import time
import json
import requests
from bs4 import BeautifulSoup as bs


class CrawlError(Exception):
    """ Base crawl exception"""
    pass


class UrlError(CrawlError):
    """Url exception

    Raised when url is wrong
    """
    pass


class NotLoginError(CrawlError):
    """Not login exception

    Raised when not login
    """
    pass


class NeedValidationError(CrawlError):
    """Need validation exception

    Raised when need validation
    """
    pass


def init_qichacha():
    qichacha = {}

    overview = {}
    overview['stock_code'] = ''
    overview['phone'] = ''
    overview['email'] = ''
    overview['website'] = ''
    overview['address'] = ''

    baseInfo = {}
    baseInfo['legal_person'] = ''
    baseInfo['registered_capital'] = ''
    baseInfo['paid_in_capital'] = ''
    baseInfo['company_status'] = ''
    baseInfo['registered_time'] = ''
    baseInfo['credit_code'] = ''
    baseInfo['identification_num'] = ''
    baseInfo['registration_num'] = ''
    baseInfo['organization_num'] = ''
    baseInfo['company_type'] = ''
    baseInfo['industry'] = ''
    baseInfo['check_time'] = ''
    baseInfo['registration_authority'] = ''
    baseInfo['area'] = ''
    baseInfo['english_name'] = ''
    baseInfo['used_name'] = ''
    baseInfo['staff_size'] = ''
    baseInfo['business_limitation'] = ''
    baseInfo['registered_address'] = ''
    baseInfo['business_scope'] = ''

    holders = []
    investments = []
    managers = []
    changeInfos = []

    qichacha['companyName'] = ''
    qichacha['url'] = ''
    qichacha['overview'] = overview
    qichacha['baseInfo'] = baseInfo
    qichacha['holders'] = holders
    qichacha['investments'] = investments
    qichacha['managers'] = managers
    qichacha['changeInfos'] = changeInfos

    return qichacha


def search_company(name, cookies, headers, proxy_dict):
    payload = {'key': name}
    url = 'http://www.qichacha.com/search'

    r = requests.get(url, params=payload, cookies=cookies, headers=headers, timeout=5, proxies=proxy_dict)
    r.raise_for_status()

    if str(r.text).find('http://www.qichacha.com/index_verify') != -1:
        raise NeedValidationError('Need Browser validate: ' + r.url)

    s = bs(r.text, 'html.parser')

    if s.script.string:
        if s.script.string.find('var arg1=') != -1:
            raise NeedValidationError('Need Browser Open url: ' + r.url)

    if str(s.select('header ul li')[6]).find('zhaosy') == -1:
        raise NotLoginError()

    a = s.find('section', id='searchlist').find('tbody').find('tr').find_all('td')[1].find('a')
    url = 'http://www.qichacha.com' + a['href']

    return url


def get_detail(url, r, s, cookies, headers, proxy_dict):
    if url:
        r = requests.get(url, cookies=cookies, headers=headers, timeout=5, proxies=proxy_dict)
        r.raise_for_status()

        if str(r.text).find('http://www.qichacha.com/index_verify') != -1:
            raise NeedValidationError('Need Browser validate: ' + r.url)

        s = bs(r.text, 'html.parser')

        if s.script.string:
            if s.script.string.find('var arg1=') != -1:
                raise NeedValidationError('Need Browser Open url: ' + r.url)

        if str(s.select('header ul li')[6]).find('zhaosy') == -1:
            raise NotLoginError()

    qichacha = init_qichacha()

    companyName = s.find('div', id='company-top').find('div', class_='content').find('h1').string.strip()
    qichacha['companyName'] = companyName
    qichacha['url'] = r.url

    html_path = 'html/' + time.strftime('%Y-%m-%d', time.localtime())
    if not os.path.isdir(html_path):
        os.makedirs(html_path)
    with open(os.path.join(html_path, companyName + '.html'), 'w', encoding='utf-8') as f:
        f.write(r.text)

    # 概况
    overview = qichacha['overview']
    div = s.find('div', id='company-top')
    if div:
        div = div.find('div', class_='content')
        div_nth = 1
        # 上市信息
        if div.find_all('div')[1].find('span').string.find('上市详情') != -1:
            div_nth = 2
            stock_code = div.find_all('div')[1].find('a').string
            overview['stock_code'] = re.search(r'\((\d+)\)', stock_code).group(1)

        phone = div.find_all('div')[div_nth].find_all('span')[1].find('span')
        if phone:
            overview['phone'] = phone.string.strip()
        email = div.find_all('div')[div_nth + 1].find_all('span')[1].find('a')
        if email:
            overview['email'] = email.string.strip()
        website = div.find_all('div')[div_nth + 1].find_all('span')[3].find('a')
        if website:
            overview['website'] = website.string.strip()
        address = div.find_all('div')[div_nth + 2].find_all('span')[1].find('a')
        if address:
            overview['address'] = address.string.strip()

    # 工商信息
    baseInfo = qichacha['baseInfo']
    div = s.find('section', id='Cominfo')
    if div:
        table = div.find_all('table')[1]
        try:
            baseInfo['legal_person'] = div.select('table tr:nth-of-type(2) a')[0].string.strip()
        except:
            baseInfo['legal_person'] = ''
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
        baseInfo['area'] = table.select('tr:nth-of-type(7) td:nth-of-type(2)')[0].string.strip()
        baseInfo['english_name'] = table.select('tr:nth-of-type(7) td:nth-of-type(4)')[0].string.strip()
        used_name = table.select('tr:nth-of-type(8) td:nth-of-type(2)')[0].find('span')
        if used_name:
            baseInfo['used_name'] = used_name.string.strip()
        baseInfo['staff_size'] = table.select('tr:nth-of-type(9) td:nth-of-type(2)')[0].string.strip()
        baseInfo['business_limitation'] = table.select('tr:nth-of-type(9) td:nth-of-type(4)')[0].string.strip()
        baseInfo['registered_address'] = table.select('tr:nth-of-type(10) td:nth-of-type(2)')[0].contents[0].strip()
        baseInfo['business_scope'] = table.select('tr:nth-of-type(11) td:nth-of-type(2)')[0].string.strip()

    # 股东
    # 不分页
    # example https://www.qichacha.com/firm_cd6b024cf176db2943619725b60b499d.html
    holders = qichacha['holders']
    div = s.find('section', id='Sockinfo')
    if div:
        trs = div.select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            holder = {}
            holder['name'] = tds[1].find('a').string.strip()
            # 有些股东不是公司 如自然人  社会公众股等
            holder['url'] = 'http://www.qichacha.com' + tds[1].find('a')['href']
            holder['proportion'] = tds[2].string.strip()
            holder['amount'] = tds[3].contents[0].strip()
            holder['time'] = tds[4].contents[0].strip()
            holder['type'] = tds[5].string.strip()

            holders.append(holder)

    # 对外投资
    investments = qichacha['investments']
    div = s.find('section', id='touzilist')
    if div:
        trs = div.select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            investment = {}
            investment['company_name'] = tds[0].a.string.strip()
            investment['url'] = 'http://www.qichacha.com' + tds[0].a['href']
            try:
                investment['legal_person'] = tds[1].a.string.strip()
            except:
                investment['legal_person'] = ''
            investment['registered_capital'] = tds[2].string.strip()
            investment['invest_proportion'] = tds[3].string.strip()
            investment['registered_time'] = tds[4].string.strip()
            investment['company_status'] = tds[5].span.string.strip()

            investments.append(investment)

        pager = div.find('ul', class_='pagination')

        if pager:
            if pager.find_all('li')[-1].a.string == '>':
                pages = int(re.search(r'(\d+)$', pager.find_all('li')[-2].a.string).group())
            else:
                pages = int(re.search(r'(\d+)$', pager.find_all('li')[-1].a.string).group())
            page = 2
            while page <= pages:
                unique = re.search(r'_(\w+).html', qichacha['url']).group(1)
                payload = {'box': 'touzi', 'companyname': companyName,
                           'p': page, 'tab': 'base', 'unique': unique}
                url = 'https://www.qichacha.com/company_getinfos'

                temp_r = requests.get(url, params=payload, cookies=cookies, headers=headers, timeout=5, proxies=proxy_dict)
                temp_r.raise_for_status()

                temp_s = bs(temp_r.text, 'html.parser')
                trs = temp_s.select('table tr')[1:]
                for tr in trs:
                    tds = tr.find_all('td')
                    investment = {}
                    investment['company_name'] = tds[0].a.string.strip()
                    investment['url'] = 'http://www.qichacha.com' + tds[0].a['href']
                    try:
                        investment['legal_person'] = tds[1].a.string.strip()
                    except:
                        investment['legal_person'] = ''
                    investment['registered_capital'] = tds[2].string.strip()
                    investment['invest_proportion'] = tds[3].string.strip()
                    investment['registered_time'] = tds[4].string.strip()
                    investment['company_status'] = tds[5].span.string.strip()

                    investments.append(investment)

                page += 1

    # 高管
    managers = qichacha['managers']
    div = s.find('section', id='Mainmember')
    if div:
        trs = div.select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            manager = {}
            manager['name'] = tds[1].a.string.strip()
            manager['position'] = tds[2].string.strip()

            managers.append(manager)

    # 变更信息
    changeInfos = qichacha['changeInfos']
    div = s.find('section', id='Changelist')
    if div:
        trs = div.select('table tr')[1:]
        for tr in trs:
            tds = tr.find_all('td')
            changeInfo = {}
            changeInfo['time'] = tds[1].string.strip()
            changeInfo['item'] = tds[2].contents[0].strip()
            before = []
            for content in tds[3].div.contents:
                if content.string:
                    before.append(content.string.strip())
            changeInfo['before'] = ''.join(before)
            after = []
            for content in tds[4].div.contents:
                if content.string:
                    after.append(content.string.strip())
            changeInfo['after'] = ''.join(after)

            changeInfos.append(changeInfo)

    # 储存为JSON文件
    json_path = 'json/qichacha'
    if not os.path.isdir(json_path):
        os.makedirs(json_path)
    with open(os.path.join(json_path, companyName + '.json'), 'w', encoding='utf-8') as f:
        json.dump(qichacha, f, ensure_ascii=False)

    return qichacha


def crawl_from_qichacha(name, url, proxy_dict):
    # print('crawling', name)

    with open('config.json', 'r') as f:
        config = json.load(f)
        cookie_str = config['cookie_qichacha']
        user_agent = config['user_agent']

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, sdch, br',
        'accept-language': 'zh-CN,zh;q=0.8',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'upgrade-insecure-requests': '1',
        'user-agent': user_agent
    }
    cookies = {}
    for cookie in cookie_str.split(';'):
        k, v = cookie.strip().split('=')
        cookies[k] = v

    # 若未提供url 则搜索
    if not url:
        url = search_company(name, cookies, headers, proxy_dict)

    # 若提供的url打不开网站
    # 如中车集团的天津实业有限公司
    # 则重新搜索

    # 取消更正url的功能
    # 若url错误，则抛出异常，由调用者处理

    r = requests.get(url, cookies=cookies, headers=headers, timeout=5, proxies=proxy_dict)
    r.raise_for_status()

    if str(r.text).find('http://www.qichacha.com/index_verify') != -1:
        raise NeedValidationError('Need Browser validate: ' + r.url)

    s = bs(r.text, 'html.parser')

    if s.script.string:
        if s.script.string.find('var arg1=') != -1:
            raise NeedValidationError('Need Browser Open url: ' + r.url)

    if str(s.select('header ul li')[6]).find('zhaosy') == -1:
        raise NotLoginError()

    companyName = s.find('div', id='company-top').find('div', class_='content').find('h1').string
    if companyName:
        companyName = companyName.strip()

    if companyName:
        qichacha = get_detail('', r, s, cookies, headers, proxy_dict)    # 已经正确，不需要再请求
    else:
        # url = search_company(name, cookies, headers, proxy_dict)
        # print('right url:', url)
        # qichacha = get_detail(url, r, s, cookies, headers, proxy_dict)   # 传入正确的url，重新请求
        raise UrlError((name, r.url))

    # print('crawl finished', name)

    return qichacha


if __name__ == '__main__':
    name = '海航投资集团股份有限公司'
    url = 'https://www.qichacha.com/firm_24bb12e95b13cfa0c2394214e2b60f50.html'
    proxy_dict = {'http': '', 'https': '118.190.159.105:808'}

    qichacha = crawl_from_qichacha(name, url, proxy_dict)
