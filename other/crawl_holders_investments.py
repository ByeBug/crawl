"""
爬取二级股东和投资
爬取过的加入crawled列表
爬取出错的加入has_error列表
需要验证后程序退出，退出前保存 crawled 和 has_error 到文件
TODO(zhaosy): 考虑多个公司，更大规模的情况，是否使用数据库
"""


import logging
import json
import re, os
import sqlite3
from spiders.crawl_qichacha import crawl_from_qichacha, NeedValidationError
from spiders.crawl_stock import crawl_stock


logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
handler1 = logging.FileHandler('crawl_log.log', mode='w', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S')
handler1.setFormatter(formatter)
stream_handler = logging.StreamHandler()
logger.addHandler(handler1)
logger.addHandler(stream_handler)

# conn = sqlite3.connect('d:/firm_unique_qichacha.db', check_same_thread=False)
# cursor = conn.cursor()


def crawl_holders(data):
    logger.info('----crawling {} holders----'.format(data['companyName']))
    for i in data['holders']:
        if re.search(r'firm_(\w+).html', i['url']) and i['name'] not in crawled:
            try:
                q = crawl_from_qichacha(i['name'], i['url'], {})
            except NeedValidationError as e:
                raise e
            except Exception as e:
                logger.info('error: {}, {}'.format(i['name'], i['url']))
                has_error.append((i['name'], i['url']))
                crawled.append(i['name'])
            else:
                if q['overview']['stock_code']:
                    crawl_stock(q['overview']['stock_code'])
                logger.info('crawl: {}'.format(i['name']))
                crawled.append(i['name'])
    logger.info('----crawling {} holders end----'.format(data['companyName']))


def crawl_investments(data):
    logger.info('++++crawling {} investments++++'.format(data['companyName']))
    for i in data['investments']:
        if re.search(r'firm_(\w+).html', i['url']) and i['name'] not in crawled:
            try:
                q = crawl_from_qichacha(i['name'], i['url'], {})
            except NeedValidationError as e:
                raise e
            except Exception as e:
                logger.info('error: {}, {}'.format(i['name'], i['url']))
                has_error.append((i['name'], i['url']))
                crawled.append(i['name'])
            else:
                if q['overview']['stock_code']:
                    crawl_stock(q['overview']['stock_code'])
                logger.info('crawl: {}'.format(i['name']))
                crawled.append(i['name'])
    logger.info('++++crawling {} investments end++++'.format(data['companyName']))


company = '海航集团有限公司'
group_list = [
    {
        'name': '瓮福(集团)有限责任公司',
        'url': 'https://www.qichacha.com/firm_fe8d1260622ab5e7868628fdc7943dbe.html'
    },
    {
        'name': '贵州乾朗大宗商品交易中心有限公司',
        'url': 'https://www.qichacha.com/firm_e121c99856b15da0831d889a5089fbe4.html'
    },
    {
        'name': '贵州汇生林业开发有限公司',
        'url': 'https://www.qichacha.com/firm_bc79a2ed616358340df33a6155d399c1.html'
    },
]


with open('crawled.json', 'r', encoding='utf-8') as f:
    crawled = json.load(f)

with open('error.json', 'r', encoding='utf-8') as f:
    has_error = json.load(f)

for g in group_list:
    if not os.path.isfile('json/qichacha/' + g['name'] + '.json'):
        # 公司本身
        q = crawl_from_qichacha(g['name'], '', {})
        if q['overview']['stock_code']:
            crawl_stock(q['overview']['stock_code'])
        crawled.append(g['name'])
    
    unique = re.search(r'_(\w+).html', g['url']).group(1)
    with open('json/qichacha/' + unique + '.json', 'r', encoding='utf-8') as f:
        root_data = json.load(f)

    try:
        # 一级母公司
        crawl_holders(root_data)

        # 一级子公司
        crawl_investments(root_data)

        # 二级母公司
        for i in root_data['holders']:
            if re.search(r'firm_(\w+).html', i['url']) and i['name'] not in has_error:
                unique = re.search(r'_(\w+).html', i['url']).group(1)
                with open('json/qichacha/' + unique + '.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                crawl_holders(data)

        # 二级子公司
        j = 0
        invest_num = len(root_data['investments'])
        for i in root_data['investments']:
            if re.search(r'firm_(\w+).html', i['url']) and i['name'] not in has_error:
                unique = re.search(r'_(\w+).html', i['url']).group(1)
                with open('json/qichacha/' + unique + '.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)
                j += 1
                logger.info('%d/%d' % (j, invest_num))
                crawl_investments(data)

    except NeedValidationError as e:
        with open('crawled.json', 'w', encoding='utf-8') as f:
            json.dump(crawled, f, ensure_ascii=False)
        with open('error.json', 'w', encoding='utf-8') as f:
            json.dump(has_error, f, ensure_ascii=False)
        logger.info('need validation')
    except Exception as e:
        with open('crawled.json', 'w', encoding='utf-8') as f:
            json.dump(crawled, f, ensure_ascii=False)
        with open('error.json', 'w', encoding='utf-8') as f:
            json.dump(has_error, f, ensure_ascii=False)
        raise e

with open('crawled.json', 'w', encoding='utf-8') as f:
    json.dump(crawled, f, ensure_ascii=False)
with open('error.json', 'w', encoding='utf-8') as f:
    json.dump(has_error, f, ensure_ascii=False)
logger.info('====Crawl {} Finished'.format(root_data['companyName']))
