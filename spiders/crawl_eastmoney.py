#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 12 15:07:31 2018

@author: zhao

从东方财富获取股票详细信息

A股为主体 B股为附属
"""

import re
import os
import json
import requests


def init_eastmoney():
    eastmoney = {}

    return eastmoney


def crawl_from_eastmoney(code):
    p = re.compile('(^600)|(^601)|(^603)|(^900)')
    if p.match(code):
        eastmoney_code = 'sh' + code
    else:
        eastmoney_code = 'sz' + code

    payload = {'code': eastmoney_code}
    headers = {
        'Host': 'emweb.securities.eastmoney.com',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
    }

    eastmoney = init_eastmoney()

    # 公司概况
    url_survey = 'http://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax'
    r = requests.get(url_survey, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    data = r.json()['Result']
    survey = {}
    survey['base_info'] = data['jbzl']
    survey['about_issue'] = data['fxxg']

    eastmoney['code'] = survey['base_info']['agdm']
    eastmoney['name'] = survey['base_info']['agjc']

    eastmoney['survey'] = survey

    # B股信息
    b_stock_info = {}
    if re.search(r'\d{6}', survey['base_info']['bgdm']):
        if p.match(survey['base_info']['bgdm']):
            b_code = 'sh' + survey['base_info']['bgdm']
        else:
            b_code = 'sz' + survey['base_info']['bgdm']

        payload = {'code': b_code}
        r = requests.get(url_survey, timeout=4, headers=headers, params=payload)
        r.raise_for_status()

        data = r.json()['Result']
        b_stock_info['code'] = data['jbzl']['bgdm']
        b_stock_info['name'] = data['jbzl']['bgjc']
        b_stock_info['stock_type'] = data['jbzl']['zqlb']
        b_stock_info['list_date'] = data['fxxg']['ssrq']
        b_stock_info['exchange'] = data['jbzl']['ssjys']
        b_stock_info['pe'] = data['fxxg']['fxsyl']
        b_stock_info['issue_date'] = data['fxxg']['wsfxrq']
        b_stock_info['issue_type'] = data['fxxg']['fxfs']
        b_stock_info['face_value'] = data['fxxg']['mgmz']
        b_stock_info['issue_count'] = data['fxxg']['fxl']
        b_stock_info['issue_price'] = data['fxxg']['mgfxj']
        b_stock_info['issue_cost'] = data['fxxg']['fxfy']
        b_stock_info['issue_tot_market'] = data['fxxg']['fxzsz']
        b_stock_info['raise_funds'] = data['fxxg']['mjzjje']
        b_stock_info['open_price'] = data['fxxg']['srkpj']
        b_stock_info['end_price'] = data['fxxg']['srspj']
        b_stock_info['turnover_rate'] = data['fxxg']['srhsl']
        b_stock_info['max_price'] = data['fxxg']['srzgj']
        b_stock_info['under_rate'] = data['fxxg']['wxpszql']
        b_stock_info['price_rate'] = data['fxxg']['djzql']

    eastmoney['b_stock_info'] = b_stock_info

    # 股东研究
    url_holders = 'http://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/ShareholderResearchAjax'
    r = requests.get(url_holders, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    data = r.json()
    holders = {}
    holders['top10_circulation_holders'] = data['sdltgd']
    holders['top10_holders'] = data['sdgd']

    eastmoney['holders'] = holders

    # 经营分析
    url_business = 'http://emweb.securities.eastmoney.com/PC_HSF10/BusinessAnalysis/BusinessAnalysisAjax'
    r = requests.get(url_business, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    data = r.json()['zygcfx']
    business = []
    for d in data:
        item = {}
        item['date'] = d['rq']
        item['industry'] = d['hy']
        item['area'] = d['qy']
        item['production'] = d['cp']

        business.append(item)

    eastmoney['business'] = business

    # 公司高管
    url_managers = 'http://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/CompanyManagementAjax'
    r = requests.get(url_managers, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    data = r.json()['Result']['RptManagerList']
    managers = []
    for d in data:
        item = {}
        item['name'] = d['xm']
        item['sex'] = d['xb']
        item['age'] = d['nl']
        item['education'] = d['xl']
        item['position'] = d['zw']
        item['start_date'] = d['rzsj']
        item['brief'] = d['jj']

        managers.append(item)

    eastmoney['managers'] = managers

    payload = {
        'code': eastmoney_code,
        'companyType': 4,
        'reportDateType': 0,
        'reportType': 1
    }

    # 资产负债表
    url_balance = 'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/zcfzbAjax'
    r = requests.get(url_balance, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    data = json.loads(r.json())

    eastmoney['balance_sheet'] = data

    # 利润表
    url_income = 'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/lrbAjax'
    r = requests.get(url_income, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    data = json.loads(r.json())

    eastmoney['income_statement'] = data

    # 现金流量表
    url_cash_flow = 'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/xjllbAjax'
    r = requests.get(url_cash_flow, timeout=4, headers=headers, params=payload)
    r.raise_for_status()

    data = json.loads(r.json())

    eastmoney['cash_flow_statement'] = data

    # 储存为文件
    json_path = 'json/eastmoney'
    if not os.path.isdir(json_path):
        os.makedirs(json_path)
    with open(os.path.join(json_path, code + '.json'), 'w', encoding='utf-8') as f:
        json.dump(eastmoney, f, ensure_ascii=False)

    return eastmoney


if __name__ == '__main__':
    code = '600221'
    eastmoney = crawl_from_eastmoney(code)
