# -*- coding: utf-8 -*-

import re, time, json
import requests
from bs4 import BeautifulSoup as bs


def crawl_from_tianyancha(name, url):
    headers = {
        'Host': 'www.tianyancha.com',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36'
    }
    cookies = {}
    with open('config.json', 'r') as f:
        cookie_str = json.load(f)['cookie_tianyancha']
    for cookie in cookie_str.split(';'):
        k, v = cookie.strip().split('=')
        cookies[k] = v

    if not url:
        payload = {'key': name}
        url_search = 'https://www.tianyancha.com/search'
        r = requests.get(url_search, params=payload, cookies=cookies, headers=headers)
        if r.status_code == 200:
            s = bs(r.text, 'html.parser')
            div = s.find('div', class_='search_result_single')
            url = div.find('a', class_='query_name')['href']
            companyID = re.search('(\d+)$', url).group()
        else:
            print('search request error', r.status_code)

    r = requests.get(url, cookies=cookies, headers=headers)
    if r.status_code == 200:
        filename = '_'.join([name, time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()), 'tianyancha.html'])
        with open('html/' + filename, 'w', encoding='utf-8') as f:
            f.write(r.text)

        # if login

        s = bs(r.text, 'html.parser')

        tianyancha = {}

        # 概况
        tianyancha['companyName'] = s.find('div', class_='company_header_width').find('h1').string
        tianyancha['companyID'] = re.search('(\d+)$', r.url).group()
        tianyancha['url'] = r.url
        # 电话 有更多
        tianyancha['phone'] = s.select('div.company_header_interior > div:nth-of-type(2) > div:nth-of-type(1) > span:nth-of-type(2)')[0].string
        tianyancha['email'] = s.select('div.company_header_interior > div:nth-of-type(2) > div:nth-of-type(2) > span:nth-of-type(2)')[0].string
        tianyancha['website'] = s.select('div.company_header_interior > div:nth-of-type(3) > div:nth-of-type(1) a')[0]['href']
        tianyancha['address'] = s.select('div.company_header_interior > div:nth-of-type(3) > div:nth-of-type(2) span:nth-of-type(2)')[0].string
        # 简介 有详情
        tianyancha['brief'] = s.select('div.company_header_interior > div:nth-of-type(4) script')[0].string.strip()
        tianyancha['update_time'] = s.find('span', class_='updatetimeComBox').string

        # 工商信息
        baseInfo = {}
        div = s.find('div', id='_container_baseInfo')
        div1 = div.select('div > div:nth-of-type(2)')[0]
        baseInfo['legal_person'] = div1.find('div', class_='human-top').find('a').string
        # 注册资本不正确
        baseInfo['registered_capital'] = div1.select('tbody td:nth-of-type(2) > div:nth-of-type(1) text')[0].string
        # 注册时间不正确
        baseInfo['registered_time'] = div1.select('tbody td:nth-of-type(2) > div:nth-of-type(2) text')[0].string
        baseInfo['company_status'] = div1.select('tbody td:nth-of-type(2) > div:nth-of-type(3) > div:nth-of-type(2) div')[0].string

        trs = div.select('div > div:nth-of-type(3) tbody')[0].find_all('tr')
        baseInfo['registration_num'] = trs[0].find_all('td')[1].string
        baseInfo['organization_num'] = trs[0].find_all('td')[3].string
        baseInfo['credit_code'] = trs[1].find_all('td')[1].string
        baseInfo['company_type'] = trs[1].find_all('td')[3].string
        baseInfo['identification_num'] = trs[2].find_all('td')[1].string
        baseInfo['industry'] = trs[2].find_all('td')[3].string
        baseInfo['business_limitation'] = trs[3].find_all('td')[1].string
        # 核准时间不正确
        baseInfo['check_time'] = trs[3].find_all('td')[3].string
        baseInfo['registration_authority'] = trs[4].find_all('td')[1].string
        baseInfo['english_name'] = trs[4].find_all('td')[3].string
        baseInfo['registered_address'] = trs[5].find_all('td')[1].contents[0]
        baseInfo['business_scope'] = trs[6].select('td:nth-of-type(2) span.js-full-container')[0].string

        tianyancha['baseInfo'] = baseInfo

        # 高管
        managers = []
        divs = s.find('div', id='_container_staff').find_all('div', class_='staffinfo-module-container')
        for div in divs:
            manager = {}
            manager['position'] = div.find('span').string
            manager['name'] = div.find('a').string

            managers.append(manager)

        tianyancha['managers'] = managers

        # 股东
        holders = []
        trs = s.find('div', id='_container_holder').find('tbody').find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            holder = {}
            holder['name'] = tds[0].a.contents[0]
            holder['url'] = tds[0].a['href']
            holder['proportion'] = tds[1].find('span').string
            holder['amount'] = tds[2].find_all('span')[0].string
            # 出资时间可能没有
            try:
                invest_time = tds[2].find_all('span')[1].string
            except:
                invest_time = ''
            holder['time'] = invest_time
            holders.append(holder)

        tianyancha['holders'] = holders

        # 对外投资
        investments = []
        trs = s.find('div', class_='out-investment-container').find('tbody').find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            investment = {}
            investment['company_name'] = tds[0].find('span').string
            investment['url'] = tds[0].find('a')['href']
            investment['legal_person'] = tds[1].find('a').string
            investment['registered_capital'] = tds[2].find('span').string
            investment['invest_amount'] = tds[3].find('span').string
            investment['invest_proportion'] = tds[4].find('span').string
            investment['registered_time'] = tds[5].find('span').string
            investment['company_status'] = tds[6].find('span').string

            investments.append(investment)

        pager = s.find('div', id='_container_invest').find('div', class_='company_pager')
        if pager:
            pages = int(pager.find('div', class_='total').contents[1])
            page = 2
            while page <= pages:
                payload = {'pn': page, 'id': tianyancha['companyID']}
                url = 'https://www.tianyancha.com/pagination/invest.xhtml'
                temp_r = requests.get(url, params=payload, cookies=cookies, headers=headers)
                if temp_r.status_code == 200:
                    temp_s = bs(temp_r.text, 'html.parser')
                    trs = temp_s.find('div', class_='out-investment-container').find('tbody').find_all('tr')
                    for tr in trs:
                        tds = tr.find_all('td')
                        investment = {}
                        investment['company_name'] = tds[0].find('span').string
                        investment['legal_person'] = tds[1].find('a').string
                        investment['registered_capital'] = tds[2].find('span').string
                        investment['invest_amount'] = tds[3].find('span').string
                        investment['invest_proportion'] = tds[4].find('span').string
                        investment['registered_time'] = tds[5].find('span').string
                        investment['company_status'] = tds[6].find('span').string

                        investments.append(investment)

                else:
                    print('invest pagination error', temp_r.status_code)
                page += 1

        tianyancha['investments'] = investments

        # 变更信息
        changeInfos = []
        trs = s.find('div', id='_container_changeinfo').find('tbody').find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            changeInfo = {}
            changeInfo['time'] = tds[0].find('div').string
            changeInfo['item'] = tds[1].find('div').string
            before = ''
            for content in tds[2].find('div').contents:
                if content.string:
                    before += content.string
            changeInfo['before'] = before
            after = ''
            for content in tds[3].find('div').contents:
                if content.string:
                    after += content.string
            changeInfo['after'] = after

            changeInfos.append(changeInfo)

        pager = s.find('div', id='_container_changeinfo').find('div', class_='company_pager')
        if pager:
            pages = int(pager.find('div', class_='total').contents[1])
            page = 2
            while page <= pages:
                payload = {'pn': page, 'id': tianyancha['companyID']}
                url = 'https://www.tianyancha.com/pagination/changeinfo.xhtml'
                temp_r = requests.get(url, params=payload, cookies=cookies, headers=headers)
                if temp_r.status_code == 200:
                    temp_s = bs(temp_r.text, 'html.parser')
                    trs = temp_s.find('tbody').find_all('tr')
                    for tr in trs:
                        tds = tr.find_all('td')
                        changeInfo = {}
                        changeInfo['time'] = tds[0].find('div').string
                        changeInfo['item'] = tds[1].find('div').string
                        before = ''
                        for content in tds[2].find('div').contents:
                            if content.string:
                                before += content.string
                        changeInfo['before'] = before
                        after = ''
                        for content in tds[3].find('div').contents:
                            if content.string:
                                after += content.string
                        changeInfo['after'] = after

                        changeInfos.append(changeInfo)

                else:
                    print('changeinfo pagination error', temp_r.status_code)
                page += 1

        tianyancha['changeInfos'] = changeInfos

        # 债券信息
        bonds = []
        trs = s.find('div', id='_container_bond').find('tbody').find_all('tr')
        for tr in trs:
            json_str = tr.find('script').string
            bond = json.loads(json_str)

            bonds.append(bond)

        pager = s.find('div', id='_container_bond').find('div', class_='company_pager')
        if pager:
            pages = int(pager.find('div', class_='total').contents[1])
            page = 2
            while page <= pages:
                payload = {'pn': page, 'name': name}
                url = 'https://www.tianyancha.com/pagination/bond.xhtml'
                temp_r = requests.get(url, params=payload, cookies=cookies, headers=headers)
                if temp_r.status_code == 200:
                    temp_s = bs(temp_r.text, 'html.parser')
                    trs = temp_s.find('tbody').find_all('tr')
                    for tr in trs:
                        json_str = tr.find('script').string
                        bond = json.loads(json_str)

                        bonds.append(bond)
                else:
                    print('bond pagination error', temp_r.status_code)
                page += 1

        tianyancha['bonds'] = bonds

        return tianyancha

    else:
        print('detail request error', r.status_code)
        return -1



def crawl_from_qichacha(name, url):
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

    if not url:
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
        div = s.find('div', id='company-top').find('div', class_='content')
        qichacha['companyName'] = div.select('h1')[0].string.strip()
        qichacha['url'] = r.url
        qichacha['phone'] = div.select('div:nth-of-type(2) span:nth-of-type(2) span')[0].string.strip()
        qichacha['email'] = div.select('div:nth-of-type(3) span:nth-of-type(2) a')[0].string.strip()
        qichacha['website'] = div.select('div:nth-of-type(3) span:nth-of-type(4) a')[0].string.strip()
        qichacha['address'] = div.select('div:nth-of-type(4) span:nth-of-type(2) a')[0].string.strip()

        # 工商信息
        baseInfo = {}
        div = s.find('section', id='Cominfo')
        baseInfo['legal_person'] = div.select('table tr:nth-of-type(2) a')[0].string.strip()
        baseInfo['registered_capital'] = div.select('table:nth-of-type(2) tr td:nth-of-type(2)')[0].string.strip()
        baseInfo['paid_in_capital'] = div.select('table:nth-of-type(2) tr td:nth-of-type(4)')[0].string.strip()
        baseInfo['company_status'] = div.select('table:nth-of-type(2) tr:nth-of-type(2) td:nth-of-type(2)')[
            0].string.strip()
        baseInfo['registered_time'] = div.select('table:nth-of-type(2) tr:nth-of-type(2) td:nth-of-type(4)')[
            0].string.strip()
        baseInfo['credit_code'] = div.select('table:nth-of-type(2) tr:nth-of-type(3) td:nth-of-type(2)')[
            0].string.strip()
        baseInfo['identification_num'] = div.select('table:nth-of-type(2) tr:nth-of-type(3) td:nth-of-type(4)')[
            0].string.strip()
        baseInfo['registration_num'] = div.select('table:nth-of-type(2) tr:nth-of-type(4) td:nth-of-type(2)')[
            0].string.strip()
        baseInfo['organization_num'] = div.select('table:nth-of-type(2) tr:nth-of-type(4) td:nth-of-type(4)')[
            0].string.strip()
        baseInfo['company_type'] = div.select('table:nth-of-type(2) tr:nth-of-type(5) td:nth-of-type(2)')[
            0].string.strip()
        baseInfo['industry'] = div.select('table:nth-of-type(2) tr:nth-of-type(5) td:nth-of-type(4)')[
            0].string.strip()
        baseInfo['check_time'] = div.select('table:nth-of-type(2) tr:nth-of-type(6) td:nth-of-type(2)')[
            0].string.strip()
        baseInfo['registration_authority'] = div.select('table:nth-of-type(2) tr:nth-of-type(6) td:nth-of-type(4)')[
            0].string.strip()
        baseInfo['english_name'] = div.select('table:nth-of-type(2) tr:nth-of-type(7) td:nth-of-type(4)')[
            0].string.strip()
        baseInfo['used_name'] = div.select('table:nth-of-type(2) tr:nth-of-type(8) td:nth-of-type(2)')[
            0].string.strip()
        baseInfo['staff_size'] = div.select('table:nth-of-type(2) tr:nth-of-type(9) td:nth-of-type(2)')[
            0].string.strip()
        baseInfo['business_limitation'] = div.select('table:nth-of-type(2) tr:nth-of-type(9) td:nth-of-type(4)')[
            0].string.strip()
        baseInfo['registered_address'] = \
        div.select('table:nth-of-type(2) tr:nth-of-type(10) td:nth-of-type(2)')[0].contents[0].strip()
        baseInfo['business_scope'] = div.select('table:nth-of-type(2) tr:nth-of-type(11) td:nth-of-type(2)')[
            0].string.strip()

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

        return qichacha

    else:
        print('detail request error', r.status_code)
        return -1




name = '海航集团有限公司'

tianyancha = crawl_from_tianyancha(name, '')
qichacha = crawl_from_qichacha(name, '')

company = {}
company['tianyancha'] = tianyancha
company['qichacha'] = qichacha

filename = name + '.json'
with open(filename, 'w', encoding='utf-8') as f:
    json.dump(company, f, ensure_ascii=False)

# crawl holders
for holder in company['tianyancha']['holders']:
    tianyancha = crawl_from_tianyancha(holder['name'], holder['url'])

for holder in company['qichacha']['holders']:
    qichacha = crawl_from_qichacha(holder['name'], holder['url'])

# crawl investments
for investment in company['tianyancha']['investments']:
    tianyancha = crawl_from_tianyancha(investment['name'], investment['url'])

for investment in company['qichacha']['investments']:
    qichacha = crawl_from_qichacha(investment['name'], investment['url'])
