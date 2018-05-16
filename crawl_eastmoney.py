#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 12 15:07:31 2018

@author: zhao
"""
import requests
import json

code = 'sh600221'
payload = {'code': code}
headers = {
        'Host': 'emweb.securities.eastmoney.com',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
        }

eastmoney = {}

url_survey = 'http://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax'
r = requests.get(url_survey, headers=headers, params=payload)
if r.status_code == 200:
    eastmoney['survey'] = r.json()
else:
    print('requests company survey failed', r.status_code)
    
url_holders = 'http://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/ShareholderResearchAjax'
r = requests.get(url_holders, headers=headers, params=payload)
if r.status_code == 200:
    eastmoney['holders'] = r.json()
else:
    print('requests company survey failed', r.status_code)
    
url_business = 'http://emweb.securities.eastmoney.com/PC_HSF10/BusinessAnalysis/BusinessAnalysisAjax'
r = requests.get(url_business, headers=headers, params=payload)
if r.status_code == 200:
    eastmoney['business'] = r.json()
else:
    print('requests company survey failed', r.status_code)
    
url_managers = 'http://emweb.securities.eastmoney.com/PC_HSF10/CompanyManagement/CompanyManagementAjax'
r = requests.get(url_managers, headers=headers, params=payload)
if r.status_code == 200:
    eastmoney['managers'] = r.json()
else:
    print('requests company survey failed', r.status_code)

payload = {
        'code': code,
        'companyType': 4,
        'reportDateType': 0,
        'reportType': 1
        }
url_zichanfuzhai = 'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/zcfzbAjax'
r = requests.get(url_zichanfuzhai, headers=headers, params=payload)
if r.status_code == 200:
    eastmoney['fuzhai'] = json.loads(r.json())
else:
    print('requests company survey failed', r.status_code)
    
url_lirun = 'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/lrbAjax'
r = requests.get(url_lirun, headers=headers, params=payload)
if r.status_code == 200:
    eastmoney['lirun'] = json.loads(r.json())
else:
    print('requests company survey failed', r.status_code)
    
url_xianjinliuliang = 'http://emweb.securities.eastmoney.com/NewFinanceAnalysis/xjllbAjax'
r = requests.get(url_xianjinliuliang, headers=headers, params=payload)
if r.status_code == 200:
    eastmoney['xianjinliuliang'] = json.loads(r.json())
else:
    print('requests company survey failed', r.status_code)
    
filename = code + '_eastmoney.json'
with open(filename, 'w', encoding='utf-8') as f:
    json.dump(eastmoney, f, ensure_ascii=False)
    
